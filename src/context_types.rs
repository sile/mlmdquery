//! `$ mlmdquery {get,count} context-types` implementation.
use crate::serialize::Type;

/// `$ mlmdquery {get,count} context-types` options.
#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct ContextTypesOpt {
    /// Database URL.
    #[structopt(long, env = "MLMD_DB", hide_env_values = true)]
    pub db: String,
}

impl ContextTypesOpt {
    /// `$ mlmdquery count context-types` implementation.
    pub async fn count(&self, store: &mut mlmd::MetadataStore) -> anyhow::Result<usize> {
        Ok(self.get(store).await?.len())
    }

    /// `$ mlmdquery get context-types` implementation.
    pub async fn get(&self, store: &mut mlmd::MetadataStore) -> anyhow::Result<Vec<Type>> {
        let types = store.get_context_types().execute().await?;
        Ok(types.into_iter().map(Type::from).collect())
    }
}
