fn main() {
    let mut args = std::env::args().skip(1);
    let command = args.next().unwrap_or_else(|| "serve".to_string());
    if command != "serve" {
        eprintln!(
            "Usage: other-model-web serve [--host 127.0.0.1] [--port 14556]\n\nEnvironment: OTHER_MODEL_DB, OTHER_MODEL_ADMIN_PASSWORD, OTHER_MODEL_WEB_DIST"
        );
        std::process::exit(2);
    }

    let mut host = other_model_lib::web::default_web_host();
    let mut port = other_model_lib::web::default_web_port();
    while let Some(arg) = args.next() {
        if arg == "--host" {
            if let Some(value) = args.next() {
                host = value;
            }
            continue;
        }
        if let Some(value) = arg.strip_prefix("--host=") {
            host = value.to_string();
            continue;
        }
        if arg == "--port" {
            if let Some(value) = args.next().and_then(|item| item.parse::<u16>().ok()) {
                port = value;
            }
            continue;
        }
        if let Some(value) = arg.strip_prefix("--port=") {
            if let Ok(value) = value.parse::<u16>() {
                port = value;
            }
        }
    }

    let worker_threads = std::thread::available_parallelism()
        .map(|value| value.get().max(2))
        .unwrap_or(2);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .expect("tokio runtime");
    if let Err(err) = runtime.block_on(other_model_lib::web::run_web_server(host, port)) {
        eprintln!("other-model-web failed: {err}");
        std::process::exit(1);
    }
}
