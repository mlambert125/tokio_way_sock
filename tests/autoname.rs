use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn autonames_and_reports() {
    let dir = std::env::temp_dir().join(format!("way-sock-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    unsafe { std::env::set_var("XDG_RUNTIME_DIR", &dir) };

    // First server: should claim wayland-0 and report it via ready.
    let (tx1, _rx1) = mpsc::channel(16);
    let (ready1_tx, ready1_rx) = oneshot::channel();
    let cancel1 = CancellationToken::new();
    let h1 = tokio::spawn(tokio_way_sock::run_wayland_socket(
        None,
        ready1_tx,
        tx1,
        cancel1.clone(),
    ));
    let name1 = ready1_rx.await.expect("first server should come up");
    assert_eq!(name1, dir.join("wayland-0").to_str().unwrap());

    // Second server while the first holds its lock: should skip to wayland-1.
    let (tx2, _rx2) = mpsc::channel(16);
    let (ready2_tx, ready2_rx) = oneshot::channel();
    let cancel2 = CancellationToken::new();
    let h2 = tokio::spawn(tokio_way_sock::run_wayland_socket(
        None,
        ready2_tx,
        tx2,
        cancel2.clone(),
    ));
    let name2 = ready2_rx.await.expect("second server should come up");
    assert_eq!(name2, dir.join("wayland-1").to_str().unwrap());

    // Explicit path: ready carries the path back unchanged.
    let explicit = dir.join("my-sock").to_str().unwrap().to_string();
    let (tx3, _rx3) = mpsc::channel(16);
    let (ready3_tx, ready3_rx) = oneshot::channel();
    let cancel3 = CancellationToken::new();
    let h3 = tokio::spawn(tokio_way_sock::run_wayland_socket(
        Some(explicit.clone()),
        ready3_tx,
        tx3,
        cancel3.clone(),
    ));
    assert_eq!(ready3_rx.await.unwrap(), explicit);

    cancel1.cancel();
    cancel2.cancel();
    cancel3.cancel();
    h1.await.unwrap().unwrap();
    h2.await.unwrap().unwrap();
    h3.await.unwrap().unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}
