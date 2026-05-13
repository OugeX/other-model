fn main() {
    if let Some(model) = configure_codex_model_arg() {
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        if let Err(err) =
            runtime.block_on(other_model_lib::configure_codex_from_local_config(model))
        {
            eprintln!("configure-codex failed: {err}");
            std::process::exit(1);
        }
        return;
    }
    if std::env::args().any(|arg| arg == "--configure-codex-no-proxy") {
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        if let Err(err) = runtime.block_on(other_model_lib::configure_codex_no_proxy_only()) {
            eprintln!("configure-codex-no-proxy failed: {err}");
            std::process::exit(1);
        }
        return;
    }
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

fn configure_codex_model_arg() -> Option<String> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--configure-codex-model" {
            return args.next();
        }
        if let Some(model) = arg.strip_prefix("--configure-codex-model=") {
            return Some(model.to_string());
        }
    }
    None
}
