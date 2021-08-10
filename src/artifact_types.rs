//! `$ mlmdquery {get,count} artifact-types` implementation.
use crate::serialize::Type;

/// `$ mlmdquery {get,count} artifact-types` options.
#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct ArtifactTypesOpt {
    /// Database URL.
    #[structopt(long, env = "MLMD_DB", hide_env_values = true)]
    pub db: String,
}

impl ArtifactTypesOpt {
    /// `$ mlmdquery count artifact-types` implementation.
    pub async fn count(&self) -> anyhow::Result<usize> {
        Ok(self.get().await?.len())
    }

    /// `$ mlmdquery get artifact-types` implementation.
    pub async fn get(&self) -> anyhow::Result<Vec<Type>> {
        let mut store = mlmd::MetadataStore::connect(&self.db).await?;
        let types = store.get_artifact_types().execute().await?;
        Ok(types.into_iter().map(Type::from).collect())
    }
}
