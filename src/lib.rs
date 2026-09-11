#![warn(clippy::pedantic)]
#![warn(missing_docs)]

//! Tokio-Based Wayland Unix Socket Listener
//!
//! This is an opinionated `tokio`+`tokio_utils` wayland socket listener meant
//! to run on its own thread that delivers messages over channels and cancels
//! on a `CancellationToken`

use sendfd::{RecvWithFd, SendWithFd};
use std::{
    collections::VecDeque,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};
use tokio::{
    net::{UnixSocket, UnixStream},
    select,
    sync::mpsc::{Sender, channel},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

/// The atomic client ID counter, assigned as clients connect
static NEXT_CLIENT_ID: AtomicU32 = AtomicU32::new(1);

/// Maximum number of pending clients on the unix socket
const SOCKET_CLIENT_BACKLOG: u32 = 1024;

/// The highest `N` tried when naming the socket `wayland-N` ourselves
const MAX_AUTO_DISPLAY: u32 = 32;

/// Size of the ancillary buffer used when receiving file descriptors
const MAX_FDS_IN: usize = 28;

/// Maximum number of messages that may be queued for a single client before we give up on it.
const CLIENT_SEND_QUEUE_LIMIT: usize = 4096;

/// A request from a client: one framed wayland message, still untyped.
#[derive(Debug)]
pub struct WaylandRequest {
    /// The object being acted upon
    pub object_id: u32,
    /// The op code (method) to call
    pub op_code: u16,
    /// Arguments to the operation
    pub args: Vec<u8>,
}

/// A request with an associated client-id
pub struct WaylandRequestWithClientInfo {
    /// The client id the request came from
    pub client_id: u32,
    /// The request itself
    pub message: WaylandRequest,
}

/// An event on its way to a client: one wayland message, and the file
/// descriptors that travel with it.
#[derive(Debug)]
pub struct WaylandEvent {
    /// The object the event concerns
    pub object_id: u32,
    /// The op code (event) being sent
    pub op_code: u16,
    /// Arguments to the event
    pub args: Vec<u8>,
    /// Descriptors to attach as ancillary data when sending this event.
    pub fds: Vec<OwnedFd>,
}

/// A batch of wayland messages and file descriptors from a socket read
pub struct WaylandReadBatch {
    /// The client this read came from
    pub client_id: u32,
    /// The descriptors the read delivered, in arrival order
    pub fds: Vec<OwnedFd>,
    /// The whole requests from this read
    pub messages: Vec<WaylandRequest>,
}

/// A message from this thread notifying the compositor of a new client connection
pub struct WaylandNewClientMessage {
    /// The id for the new client
    pub client_id: u32,
    /// A sender for sending events back to this client
    pub socket_sender: Sender<WaylandEvent>,
    /// Cancels just this client's socket tasks, leaving other clients running
    /// The compositor triggers it to drop a client that has stopped reading.
    pub client_cancel_token: CancellationToken,
}

/// The top-level type for channel messages sent from this thread to the compositor thread
pub enum WaylandSocketMessage {
    /// A message denoting a new client connection
    NewClient(WaylandNewClientMessage),
    /// One read of a client's socket, with everything it produced
    Read(WaylandReadBatch),
    /// A message denoting a client hanging up the socket
    ClientDisconnected {
        /// The id of the disconnected client
        client_id: u32,
    },
}

/// What came of trying to take a message off the front of a read buffer
#[derive(Debug)]
pub(crate) enum Framed {
    /// A whole message, now removed from the buffer
    Message(WaylandRequest),
    /// Not all of one has arrived yet. The buffer is left alone
    Incomplete,
    /// The header says the message is shorter than a header, so there is no
    /// way to find where the next one begins. Carries the length claimed
    Malformed(u16),
}

/// Run a listening socket/loop until told to shutdown
///
/// # Errors
///
/// Errors naming or setting up socket.  Everything after a client just
/// is logged and typically disconnects the client.
pub async fn run_wayland_socket(
    socket_path: Option<String>,
    ready: tokio::sync::oneshot::Sender<String>,
    compositor_message_sender: tokio::sync::mpsc::Sender<WaylandSocketMessage>,
    cancel_token: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    // Hold the lock
    let (socket_path, _lock) = match socket_path {
        Some(socket_path) => {
            let lock = claim_socket_name(&socket_path)?;
            (socket_path, lock)
        }
        None => claim_auto_socket_name()?,
    };

    if std::path::Path::new(&socket_path).exists() {
        std::fs::remove_file(&socket_path)?;
    }
    let socket = UnixSocket::new_stream()?;
    socket.bind(&socket_path)?;

    let listener = socket.listen(SOCKET_CLIENT_BACKLOG)?;

    debug!("Wayland socket listening on {}", socket_path);

    let _ = ready.send(socket_path.clone());

    let mut client_handles: Vec<JoinHandle<()>> = Vec::new();

    loop {
        let res = select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _)) => {
                        let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
                        let stream = Arc::new(stream);
                        let compositor_message_channel = compositor_message_sender.clone();
                        let handle = handle_client(client_id, stream, compositor_message_channel, cancel_token.clone());
                        client_handles.push(handle);
                        client_handles.retain(|handle| !handle.is_finished());
                        Ok(())
                    }
                    Err(e) => {
                        debug!("Error accepting client: {}", e);
                        Err(anyhow::anyhow!("Error accepting client: {e}"))
                    }
                }
            }
            () = cancel_token.cancelled() => {
                Err(anyhow::anyhow!("Wayland socket received shutdown signal"))
            }
        };
        if res.is_err() {
            break;
        }
    }

    debug!("Waiting for client socket threads to terminate");
    for handle in client_handles {
        let _ = handle.await;
    }

    info!("Wayland socket shutting down...");
    if let Err(e) = std::fs::remove_file(&socket_path) {
        debug!("Failed to remove socket file: {}", e);
    }
    Ok(())
}

/// Handle and individual client
#[allow(clippy::too_many_lines)]
fn handle_client(
    client_id: u32,
    stream: Arc<UnixStream>,
    compositor_message_channel: Sender<WaylandSocketMessage>,
    cancel_token: CancellationToken,
) -> JoinHandle<()> {
    let sender_stream = stream.clone();
    tokio::spawn(async move {
        debug!("New client connected");
        let mut data = VecDeque::<u8>::new();
        let (socket_send_tx, socket_send_rx) = channel::<WaylandEvent>(CLIENT_SEND_QUEUE_LIMIT);

        let cancel_token = cancel_token.child_token();

        if compositor_message_channel
            .send(WaylandSocketMessage::NewClient(WaylandNewClientMessage {
                client_id,
                socket_sender: socket_send_tx.clone(),
                client_cancel_token: cancel_token.clone(),
            }))
            .await
            .is_err()
        {
            debug!("Compositor is gone; dropping the new connection");
            return;
        }

        let sender_cancel_token = cancel_token.clone();
        tokio::spawn(async move {
            let mut socket_send_rx = socket_send_rx;

            debug!("Wayland socket send task started");

            loop {
                select! {
                    biased;

                    message = socket_send_rx.recv() => {
                        if let Some(message) = message {
                            let mut buffer = Vec::new();
                            buffer.extend_from_slice(&message.object_id.to_le_bytes());
                            let message_length_and_opcode =
                                ((u32::try_from(message.args.len()).expect("args should not be a length exceeds u32::MAX") + 8) << 16) | u32::from(message.op_code);
                            buffer.extend_from_slice(&message_length_and_opcode.to_le_bytes());
                            buffer.extend_from_slice(&message.args);

                            let raw_fds: Vec<RawFd> =
                                message.fds.iter().map(AsRawFd::as_raw_fd).collect();
                            let mut bytes_sent = 0;
                            let mut fds_sent = false;
                            while bytes_sent < buffer.len() {
                                let writable = select! {
                                    biased;

                                    res = sender_stream.writable() => res,
                                    () = sender_cancel_token.cancelled() => {
                                        debug!("Wayland socket send task cancelled mid-write");
                                        return;
                                    }
                                };
                                if let Err(e) = writable {
                                    debug!("Error waiting for writable: {}", e);
                                    return;
                                }
                                let fds_to_send = if fds_sent { &[] } else { &raw_fds[..] };
                                match sender_stream.try_io(tokio::io::Interest::WRITABLE, || {
                                    sender_stream.send_with_fd(&buffer[bytes_sent..], fds_to_send)
                                }) {
                                    Ok(n) => {
                                        bytes_sent += n;
                                        fds_sent = true;
                                    }
                                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {},
                                    Err(e) => {
                                        debug!("Error sending message to client: {}", e);
                                        return;
                                    }
                                }
                            }
                        } else {
                            debug!("Wayland socket send channel closed");
                            break;
                        }
                    }
                    () = sender_cancel_token.cancelled() => {
                        debug!("Wayland socket send task received shutdown signal");
                        break;
                    }
                }
            }
        });

        loop {
            select! {
                () = cancel_token.cancelled() => {
                    debug!("Wayland socket receive task received shutdown signal");
                    break;
                }
                res = stream.readable() => {
                    match res {
                        Ok(()) => {}
                        Err(e) => {
                            debug!("Client disconnected: {}", e);
                            break;
                        }
                    }
                }
            }

            let mut buffer = [0u8; 4096];
            let mut fds = [0; MAX_FDS_IN];
            let result = stream.try_io(tokio::io::Interest::READABLE, || {
                stream.recv_with_fd(&mut buffer, &mut fds)
            });
            match result {
                Ok((0, _)) => {
                    debug!("Client disconnected");
                    break;
                }
                Ok((data_read, fds_read)) => {
                    data.extend(&buffer[..data_read]);

                    let mut batch_fds = Vec::with_capacity(fds_read);
                    for &fd in &fds[..fds_read] {
                        // Mark the descriptor close-on-exec
                        unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
                        batch_fds.push(unsafe { OwnedFd::from_raw_fd(fd) });
                    }

                    let mut messages = Vec::new();
                    let mut malformed = false;
                    loop {
                        match take_message(&mut data) {
                            Framed::Message(msg) => messages.push(msg),
                            Framed::Incomplete => break,
                            Framed::Malformed(length) => {
                                debug!(
                                    "Invalid message length {length} from client, disconnecting"
                                );
                                malformed = true;
                                break;
                            }
                        }
                    }

                    if (!batch_fds.is_empty() || !messages.is_empty())
                        && let Err(e) = compositor_message_channel
                            .send(WaylandSocketMessage::Read(WaylandReadBatch {
                                client_id,
                                fds: batch_fds,
                                messages,
                            }))
                            .await
                    {
                        debug!("Failed to send messages to compositor: {}", e);
                        break;
                    }

                    if malformed {
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => {
                    debug!("Client disconnected: {}", e);
                    break;
                }
            }
        }

        let _ = compositor_message_channel
            .send(WaylandSocketMessage::ClientDisconnected { client_id })
            .await;
    })
}

/// Take exclusive ownership of a socket name
pub(crate) fn claim_socket_name(socket_path: &str) -> anyhow::Result<std::fs::File> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;

    let lock_path = format!("{socket_path}.lock");
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o660)
        .open(&lock_path)
        .map_err(|e| anyhow::anyhow!("cannot open the lock file {lock_path}: {e}"))?;

    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let e = std::io::Error::last_os_error();
        anyhow::bail!(
            "another compositor is already using {socket_path} ({e}); \
             set a different socket_path to run a second one"
        );
    }
    Ok(lock)
}

/// Pick and claim a socket name nobody else is using.
pub(crate) fn claim_auto_socket_name() -> anyhow::Result<(String, std::fs::File)> {
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR")
        .map_err(|_| anyhow::anyhow!("XDG_RUNTIME_DIR is not set; cannot auto-name the socket"))?;

    for n in 0..=MAX_AUTO_DISPLAY {
        let socket_path = format!("{runtime_dir}/wayland-{n}");
        if let Ok(lock) = claim_socket_name(&socket_path) {
            debug!("Claimed socket name {}", socket_path);
            return Ok((socket_path, lock));
        }
    }
    anyhow::bail!(
        "every socket name from wayland-0 to wayland-{MAX_AUTO_DISPLAY} in {runtime_dir} \
         is taken; set an explicit socket_path to use another"
    )
}

/// Take the next complete message off the front of a client's read buffer.
pub(crate) fn take_message(data: &mut VecDeque<u8>) -> Framed {
    if data.len() < 8 {
        return Framed::Incomplete;
    }
    let object_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let length_and_opcode = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    let message_length = (length_and_opcode >> 16) as u16;
    let op_code = (length_and_opcode & 0xFFFF) as u16;

    if message_length < 8 {
        return Framed::Malformed(message_length);
    }
    if data.len() < message_length as usize {
        return Framed::Incomplete;
    }

    let mut message = data.drain(..message_length as usize);
    message.by_ref().take(8).for_each(drop);

    Framed::Message(WaylandRequest {
        object_id,
        op_code,
        args: message.collect(),
    })
}
