use std::io;

fn main() {
    if let Err(error) = agent_knowledge_client::cli::run(std::env::args_os().skip(1), io::stdout())
    {
        if let Err(report_error) = error.write_diagnostic(io::stderr()) {
            eprintln!("could not write command failure: {report_error}");
        }
        std::process::exit(2);
    }
}
