fn main() -> Result<(), Box<dyn std::error::Error>> {
    connectrpc_build::Config::new()
        .files(&["../../proto/noti.proto"])
        .includes(&["../../proto"])
        .include_file("_noti_include.rs")
        .compile()?;

    Ok(())
}
