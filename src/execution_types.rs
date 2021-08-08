use crate::serialize::Type;

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct ExecutionTypesOpt {
    #[structopt(long, env = "MLMD_DB")]
    pub db: String,
}

impl ExecutionTypesOpt {
    pub async fn count(&self) -> anyhow::Result<usize> {
        Ok(self.get().await?.len())
    }

    pub async fn get(&self) -> anyhow::Result<Vec<Type>> {
        let mut store = mlmd::MetadataStore::connect(&self.db).await?;
        let types = store.get_execution_types().execute().await?;
        Ok(types.into_iter().map(Type::from).collect())
    }
}
