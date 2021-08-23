use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Opt {
    #[allow(missing_docs)]
    #[structopt(flatten)]
    Batchable(BatchableOpt),

    /// Generates graphs in DOT language.
    Graph(GraphOpt),

    /// Executes a batch of commands.
    Batch(BatchOpt),
}

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
struct BatchOpt {
    /// Database URL.
    #[structopt(long, env = "MLMD_DB", hide_env_values = true)]
    db: String,

    /// Number of worker threads.
    #[structopt(long, default_value = "10")]
    workers: std::num::NonZeroUsize,

    /// Commands to be executed.
    commands: Vec<BatchableOpt>,
}

impl BatchOpt {
    async fn execute(&self) -> anyhow::Result<()> {
        let handlers = (0..self.workers.get())
            .map(|i| {
                let db = self.db.clone();
                let commands = self
                    .commands
                    .iter()
                    .enumerate()
                    .filter_map(|(j, c)| {
                        if j % self.workers.get() == i {
                            Some((j, c.clone()))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                tokio::spawn(async move {
                    let mut store = mlmd::MetadataStore::connect(&db).await?;
                    let mut results = Vec::new();
                    for (i, command) in commands {
                        let result = command.execute_with_store(&mut store).await?;
                        results.push((i, result));
                    }
                    Ok(results)
                })
            })
            .collect::<Vec<tokio::task::JoinHandle<anyhow::Result<_>>>>();

        let mut results = Vec::new();
        for handler in handlers {
            results.extend(handler.await??);
        }
        results.sort_by_key(|x| x.0);
        serde_json::to_writer_pretty(
            std::io::stdout().lock(),
            &results.into_iter().map(|x| x.1).collect::<Vec<_>>(),
        )?;
        println!();
        Ok(())
    }
}

#[derive(Debug, Clone, StructOpt, serde::Serialize, serde::Deserialize)]
#[structopt(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
enum BatchableOpt {
    /// Counts artifacts/executions/contexts/events.
    Count(CountOpt),

    /// Gets artifacts/executions/contexts/events.
    Get(GetOpt),
}

impl std::str::FromStr for BatchableOpt {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        let v = serde_json::from_str(s)?;
        Ok(v)
    }
}

impl BatchableOpt {
    fn db_uri(&self) -> &str {
        match self {
            Self::Count(CountOpt::Artifacts(opt)) => &opt.common.db,
            Self::Get(GetOpt::Artifacts(opt)) => &opt.common.db,
            Self::Count(CountOpt::ArtifactTypes(opt)) => &opt.db,
            Self::Get(GetOpt::ArtifactTypes(opt)) => &opt.db,
            Self::Count(CountOpt::Executions(opt)) => &opt.common.db,
            Self::Get(GetOpt::Executions(opt)) => &opt.common.db,
            Self::Count(CountOpt::ExecutionTypes(opt)) => &opt.db,
            Self::Get(GetOpt::ExecutionTypes(opt)) => &opt.db,
            Self::Count(CountOpt::Contexts(opt)) => &opt.common.db,
            Self::Get(GetOpt::Contexts(opt)) => &opt.common.db,
            Self::Count(CountOpt::ContextTypes(opt)) => &opt.db,
            Self::Get(GetOpt::ContextTypes(opt)) => &opt.db,
            Self::Count(CountOpt::Events(opt)) => &opt.common.db,
            Self::Get(GetOpt::Events(opt)) => &opt.common.db,
        }
    }

    async fn execute(&self) -> anyhow::Result<serde_json::Value> {
        let mut store = mlmd::MetadataStore::connect(self.db_uri()).await?;
        Ok(self.execute_with_store(&mut store).await?)
    }

    async fn execute_with_store(
        &self,
        store: &mut mlmd::MetadataStore,
    ) -> anyhow::Result<serde_json::Value> {
        match self {
            Self::Count(CountOpt::Artifacts(opt)) => to_json(opt.count(store).await?),
            Self::Get(GetOpt::Artifacts(opt)) => to_json(opt.get(store).await?),
            Self::Count(CountOpt::ArtifactTypes(opt)) => to_json(opt.count(store).await?),
            Self::Get(GetOpt::ArtifactTypes(opt)) => to_json(opt.get(store).await?),
            Self::Count(CountOpt::Executions(opt)) => to_json(opt.count(store).await?),
            Self::Get(GetOpt::Executions(opt)) => to_json(opt.get(store).await?),
            Self::Count(CountOpt::ExecutionTypes(opt)) => to_json(opt.count(store).await?),
            Self::Get(GetOpt::ExecutionTypes(opt)) => to_json(opt.get(store).await?),
            Self::Count(CountOpt::Contexts(opt)) => to_json(opt.count(store).await?),
            Self::Get(GetOpt::Contexts(opt)) => to_json(opt.get(store).await?),
            Self::Count(CountOpt::ContextTypes(opt)) => to_json(opt.count(store).await?),
            Self::Get(GetOpt::ContextTypes(opt)) => to_json(opt.get(store).await?),
            Self::Count(CountOpt::Events(opt)) => to_json(opt.count(store).await?),
            Self::Get(GetOpt::Events(opt)) => to_json(opt.get(store).await?),
        }
    }
}

#[derive(Debug, Clone, StructOpt, serde::Serialize, serde::Deserialize)]
#[structopt(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
enum CountOpt {
    /// Counts artifacts.
    Artifacts(mlmdquery::artifacts::CountArtifactsOpt),

    /// Counts artifact types.
    ArtifactTypes(mlmdquery::artifact_types::ArtifactTypesOpt),

    /// Counts executions.
    Executions(mlmdquery::executions::CountExecutionsOpt),

    /// Counts execution types.
    ExecutionTypes(mlmdquery::execution_types::ExecutionTypesOpt),

    /// Counts contexts.
    Contexts(mlmdquery::contexts::CountContextsOpt),

    /// Counts context types.
    ContextTypes(mlmdquery::context_types::ContextTypesOpt),

    /// Counts events.
    Events(mlmdquery::events::CountEventsOpt),
}

#[derive(Debug, Clone, StructOpt, serde::Serialize, serde::Deserialize)]
#[structopt(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
enum GetOpt {
    /// Gets artifacts.
    Artifacts(mlmdquery::artifacts::GetArtifactsOpt),

    /// Gets artifact types.
    ArtifactTypes(mlmdquery::artifact_types::ArtifactTypesOpt),

    /// Gets executions.
    Executions(mlmdquery::executions::GetExecutionsOpt),

    /// Gets execution types.
    ExecutionTypes(mlmdquery::execution_types::ExecutionTypesOpt),

    /// Gets contexts.
    Contexts(mlmdquery::contexts::GetContextsOpt),

    /// Gets context types.
    ContextTypes(mlmdquery::context_types::ContextTypesOpt),

    /// Gets events.
    Events(mlmdquery::events::GetEventsOpt),
}

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum GraphOpt {
    /// Generates a graph showing the lineage of an artifact.
    Lineage(mlmdquery::lineage::GraphLineageOpt),

    /// Generates a graph showing the input and output of an execution.
    Io(mlmdquery::io::GraphIoOpt),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    match opt {
        Opt::Batchable(opt) => {
            serde_json::to_writer_pretty(std::io::stdout().lock(), &opt.execute().await?)?;
            println!();
        }
        Opt::Batch(opt) => opt.execute().await?,
        Opt::Graph(GraphOpt::Lineage(opt)) => opt.graph(&mut std::io::stdout().lock()).await?,
        Opt::Graph(GraphOpt::Io(opt)) => opt.graph(&mut std::io::stdout().lock()).await?,
    }
    Ok(())
}

fn to_json(item: impl serde::Serialize) -> anyhow::Result<serde_json::Value> {
    let v = serde_json::to_value(&item)?;
    Ok(v)
}
