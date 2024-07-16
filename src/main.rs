use clap::{Args, Parser, Subcommand};
use env_logger::Env;
use log::{info, trace};
use solagg::{
    api::api_server,
    stream::{start_streamer, SolAggStreamer},
};

#[derive(Parser)]
#[clap(name = "SolAgg")]
#[clap(author = "mjzk")]
#[clap(version = "1.0.0")]
#[clap(about = "SolAgg is a Solana blockchain data aggregator.", long_about = None)]
#[clap(propagate_version = true)]
pub struct SolAggCmd {
    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// start SolAgg.
    Start(Start),
}

#[derive(Args, Debug)]
pub struct Start {}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // Set up the logger
    let env = Env::default().filter_or("RUST_LOG", "solagg=info");
    env_logger::Builder::from_env(env)
        .target(env_logger::Target::Stdout)
        .init();

    let cmd = SolAggCmd::parse();
    match cmd.cmd {
        Cmd::Start(_) => {
            info!("Starting SolAgg server...");
            let streamer = SolAggStreamer::new_arc(true)?;
            info!("SolAgg streamer initialized.");
            let s = streamer.clone();
            info!("To start SolAgg streamer...");
            let fut_streamer = tokio::spawn(async move { start_streamer(s).await });
            let fut_api_srv = tokio::spawn(async move { api_server(streamer).await });
            let res = tokio::try_join!(fut_streamer, fut_api_srv);
            match res {
                Ok((res0, res1)) => {
                    log::trace!("solagg normal exit; res0 = {:?}, res1 = {:?}", res0, res1);
                }
                Err(err) => {
                    log::error!("solagg failed; error = {}", err);
                }
            }
            trace!("??? SolAgg exited.");
        }
    }
    Ok(())
}
