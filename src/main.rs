mod cli;

fn main() {
    if let Err(error) = cli::run(std::env::args_os(), std::io::stdout()) {
        eprintln!("{error}");
        std::process::exit(2);
    }
}
