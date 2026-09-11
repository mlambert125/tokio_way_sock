# tokio_way_sock

A Tokio-Based Wayland Unix Socket Listener

This is an opinionated `tokio`+`tokio_utils` wayland socket listener meant
to run on its own thread that delivers messages over channels and cancels
on a `CancellationToken`.

If you're using tokio and tasks/threads to manage the components of your
compositor (e.g. socket, compositor, backend), this library will probably be of
use to you. 

If you don't want the tokio dependency and/or would prefer a synchronous epoll
implementation for use in a single call-loop to avoid threading, this is
probably not what you want.

## Usage

This is meant to be run on its own thread using
`tokio::spawn(way_sock::run_wayland_socket(...))`:

```rust
let (wayland_message_tx, wayland_message_rx) = channel::<WaylandSocketMessage>(1000);
let cancel_token = tokio_util::sync::CancellationToken::new();
let socket_handle = tokio::spawn(tokio_way_socket::run_wayland_socket(
    None,
    wayland_message_tx,
    cancel_token.clone(),
    socket_ready_tx,
));

// TODO: poll `wayland_message_rx` here or on another thread to get incoming
//       clients and wayland messages and process them.  A new client gives back
//       a sender channel for the compositor to send events back on the socket

```
### One Shot Ready Message

This is a one-shot sender that sends a single message containing the negotiated
socket name when the listener has started and is ready for clients to start
running and connecting.

### Compositor Channel Messages 

- NewClient: a client_id, a channel for sending events
  back to the client and a cancellation token for just this channel
- Read: A batch of messages and file descriptors that arrived together
- ClientDisconnected: A client_id that disconnected

#### File Descriptors

File descriptors are packed in a message batch without being associated with 
messages because which ones belong to which message can only be handled by a 
client that is maintaining the state of provisioned wayland objects so that it
can properly decode the appropriate object/method to see if fds are expected.

Note that file descriptors may arrive in an earlier batch than the message that
the apply to, but will always arrive in order.  It is up to clients to keep a
running queue of incoming file descriptors and to remove/consume them as it
processes messages that require them.  To keep ordering/processing correct, you
*must* consume them if a message requires them.

### CancellationToken

The passed in cancellation token is the top-level cancellation token that can
be signalled to gracefully shut down the entire socket gracefully.

## Building

If you are using nix, a flake is included for use with nix develop that sets up
an appropriate rust toolchain and lsps for rust and nix.  If you are using another
distribution or OS, this is a simple project with no special dependencies other
than the typical rust toolchain - you should just be able to `cargo build`.

## License

MIT OR Apache-2.0
