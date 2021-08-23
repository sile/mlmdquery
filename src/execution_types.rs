//! `$ mlmdquery {get,count} execution-types` implementation.
use crate::serialize::Type;

/// `$ mlmdquery {get,count} execution-types` options.
#[derive(Debug, structopt::StructOpt, serde::Serialize, serde::Deserialize)]
#[structopt(rename_all = "kebab-case")]
pub struct ExecutionTypesOpt {
    /// Database URL.
    #[structopt(long, env = "MLMD_DB", hide_env_values = true)]
    #[serde(skip)]
    pub db: String,
}

impl ExecutionTypesOpt {
    /// `$ mlmdquery count execution-types` implementation.
    pub async fn count(&self, store: &mut mlmd::MetadataStore) -> anyhow::Result<usize> {
        Ok(self.get(store).await?.len())
    }

    /// `$ mlmdquery get execution-types` implementation.
    pub async fn get(&self, store: &mut mlmd::MetadataStore) -> anyhow::Result<Vec<Type>> {
        let types = store.get_execution_types().execute().await?;
        Ok(types.into_iter().map(Type::from).collect())
    }
}
