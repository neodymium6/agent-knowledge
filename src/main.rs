mod admin;
mod cli;
mod client;
mod gateway;
mod queue_ingress;
mod worker;

fn main() {
    if let Err(error) = cli::run(std::env::args_os(), std::io::stdout()) {
        if let Err(report_error) = error.write_diagnostic(std::io::stderr()) {
            eprintln!("could not write command failure: {report_error}");
        }
        std::process::exit(2);
    }
}
