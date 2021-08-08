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
    Artifacts,
    ArtifactTypes(mlmdquery::artifact_types::ArtifactTypesOpt),
    Executions,
    ExecutionTypes(mlmdquery::execution_types::ExecutionTypesOpt),
    Contexts,
    ContextTypes(mlmdquery::context_types::ContextTypesOpt),
    Events,
}

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum GetOpt {
    Artifacts,
    ArtifactTypes(mlmdquery::artifact_types::ArtifactTypesOpt),
    Executions,
    ExecutionTypes(mlmdquery::execution_types::ExecutionTypesOpt),
    Contexts,
    ContextTypes(mlmdquery::context_types::ContextTypesOpt),
    Events,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    match opt {
        Opt::Count(CountOpt::ArtifactTypes(opt)) => print_json(opt.count().await?)?,
        Opt::Get(GetOpt::ArtifactTypes(opt)) => print_json(opt.get().await?)?,
        Opt::Count(CountOpt::ExecutionTypes(opt)) => print_json(opt.count().await?)?,
        Opt::Get(GetOpt::ExecutionTypes(opt)) => print_json(opt.get().await?)?,
        Opt::Count(CountOpt::ContextTypes(opt)) => print_json(opt.count().await?)?,
        Opt::Get(GetOpt::ContextTypes(opt)) => print_json(opt.get().await?)?,
        _ => todo!(),
    }
    Ok(())
}

fn print_json(item: impl serde::Serialize) -> anyhow::Result<()> {
    serde_json::to_writer_pretty(std::io::stdout().lock(), &item)?;
    println!();
    Ok(())
}
