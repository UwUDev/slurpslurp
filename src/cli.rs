use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[clap(name = "slurpslurp", author, version, about, disable_help_flag = true)]
pub struct Cli {
    #[clap(subcommand)]
    pub mode: Option<Mode>,

    #[arg(long, short)]
    pub help: bool,
}

#[derive(Subcommand, Debug)]
pub enum Mode {
    Sniff,
    Scrape {
        #[clap(value_enum)]
        target_type: TargetType,
        #[clap(value_parser)]
        id: u64,
        #[clap(value_parser)]
        tokens: Vec<String>,
    },
}

#[derive(ValueEnum, Clone, Debug)]
pub enum TargetType {
    Channel,
    Guild,
}