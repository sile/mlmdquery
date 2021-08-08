use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Opt {
    Count(CountOpt),
    Get(GetOpt),
}

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum CountOpt {
    Artifacts(mlmdquery::artifacts::CountArtifactsOpt),
    ArtifactTypes(mlmdquery::artifact_types::ArtifactTypesOpt),
    Executions(mlmdquery::executions::CountExecutionsOpt),
    ExecutionTypes(mlmdquery::execution_types::ExecutionTypesOpt),
    Contexts(mlmdquery::contexts::CountContextsOpt),
    ContextTypes(mlmdquery::context_types::ContextTypesOpt),
    Events(mlmdquery::events::CountEventsOpt),
}

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum GetOpt {
    Artifacts(mlmdquery::artifacts::GetArtifactsOpt),
    ArtifactTypes(mlmdquery::artifact_types::ArtifactTypesOpt),
    Executions(mlmdquery::executions::GetExecutionsOpt),
    ExecutionTypes(mlmdquery::execution_types::ExecutionTypesOpt),
    Contexts(mlmdquery::contexts::GetContextsOpt),
    ContextTypes(mlmdquery::context_types::ContextTypesOpt),
    Events(mlmdquery::events::GetEventsOpt),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    match opt {
        Opt::Count(CountOpt::Artifacts(opt)) => print_json(opt.count().await?)?,
        Opt::Get(GetOpt::Artifacts(opt)) => print_json(opt.get().await?)?,
        Opt::Count(CountOpt::ArtifactTypes(opt)) => print_json(opt.count().await?)?,
        Opt::Get(GetOpt::ArtifactTypes(opt)) => print_json(opt.get().await?)?,
        Opt::Count(CountOpt::Executions(opt)) => print_json(opt.count().await?)?,
        Opt::Get(GetOpt::Executions(opt)) => print_json(opt.get().await?)?,
        Opt::Count(CountOpt::ExecutionTypes(opt)) => print_json(opt.count().await?)?,
        Opt::Get(GetOpt::ExecutionTypes(opt)) => print_json(opt.get().await?)?,
        Opt::Count(CountOpt::Contexts(opt)) => print_json(opt.count().await?)?,
        Opt::Get(GetOpt::Contexts(opt)) => print_json(opt.get().await?)?,
        Opt::Count(CountOpt::ContextTypes(opt)) => print_json(opt.count().await?)?,
        Opt::Get(GetOpt::ContextTypes(opt)) => print_json(opt.get().await?)?,
        Opt::Count(CountOpt::Events(opt)) => print_json(opt.count().await?)?,
        Opt::Get(GetOpt::Events(opt)) => print_json(opt.get().await?)?,
    }
    Ok(())
}

fn print_json(item: impl serde::Serialize) -> anyhow::Result<()> {
    serde_json::to_writer_pretty(std::io::stdout().lock(), &item)?;
    println!();
    Ok(())
}
