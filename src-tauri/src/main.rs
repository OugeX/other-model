fn main() {
    if std::env::args().any(|arg| arg == "--gateway-only") {
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        if let Err(err) = runtime.block_on(other_model_lib::run_gateway_only()) {
            eprintln!("gateway-only failed: {err}");
            std::process::exit(1);
        }
        return;
    }
    other_model_lib::run();
}
