use luxbase_lib::mcp::{McpOptions, run};

#[tokio::main]
async fn main() {
    let mut options = McpOptions::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--allow-writes" => options.allow_writes = true,
            "--data-dir" => {
                let Some(dir) = args.next() else {
                    eprintln!("luxbase-mcp: --data-dir requires a path");
                    std::process::exit(2);
                };
                options.data_dir = Some(dir.into());
            }
            "--help" | "-h" => {
                println!("luxbase-mcp: MCP (stdio) bridge for LuxBase saved connections.\n\n  --allow-writes   enable the execute tool for single write statements (audited)\n  --data-dir PATH  use an alternate LuxBase data directory (default: the app data dir)\n\nExample agent config:\n  {{ \"command\": \"luxbase-mcp\", \"args\": [] }}");
                return;
            }
            other => {
                eprintln!("luxbase-mcp: unknown argument '{other}' (try --help)");
                std::process::exit(2);
            }
        }
    }
    if let Err(error) = run(options).await {
        eprintln!("luxbase-mcp: {}", error.message);
        std::process::exit(1);
    }
}
