//! `$ mlmdquery {get,count} contexts` implementation.
use crate::serialize::Context;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// `$ mlmdquery {get,count} contexts` common options.
#[derive(Debug, Clone, structopt::StructOpt, serde::Serialize, serde::Deserialize)]
#[structopt(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub struct CommonContextsOpt {
    /// Database URL.
    #[structopt(long, env = "MLMD_DB", hide_env_values = true)]
    #[serde(skip)]
    pub db: String,

    /// Target context IDs.
    #[structopt(long = "id")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ids: Vec<i32>,

    /// Target context name.
    #[structopt(long, requires("type-name"))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Target context name pattern (SQL LIKE statement value).
    #[structopt(long, requires("type-name"), conflicts_with("name"))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_pattern: Option<String>,

    /// Target context type.
    #[structopt(long = "type")]
    #[serde(rename = "type")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,

    /// Artifact ID attributed to target contexts.
    #[structopt(long = "artifact")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<i32>,

    /// Execution ID associated to target contexts.
    #[structopt(long = "execution")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub executions: Vec<i32>,

    /// Start of creation time (UNIX timestamp seconds).
    #[structopt(long)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ctime_start: Option<f64>,

    /// End of creation time (UNIX timestamp seconds).
    #[structopt(long)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ctime_end: Option<f64>,

    /// Start of update time (UNIX timestamp seconds).
    #[structopt(long)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_start: Option<f64>,

    /// End of update time (UNIX timestamp seconds).
    #[structopt(long)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_end: Option<f64>,
}

impl CommonContextsOpt {
    fn request<'a>(
        &self,
        store: &'a mut mlmd::MetadataStore,
    ) -> mlmd::requests::GetContextsRequest<'a> {
        let mut request = store.get_contexts();

        if !self.ids.is_empty() {
            request = request.ids(self.ids.iter().copied().map(mlmd::metadata::ContextId::new));
        }
        match (&self.name, &self.name_pattern, &self.type_name) {
            (Some(name), None, Some(type_name)) => {
                request = request.type_and_name(type_name, name);
            }
            (None, Some(name_pattern), Some(type_name)) => {
                request = request.type_and_name_pattern(type_name, name_pattern);
            }
            (None, None, Some(type_name)) => {
                request = request.ty(type_name);
            }
            _ => {}
        }
        request = request.artifacts(
            self.artifacts
                .iter()
                .copied()
                .map(mlmd::metadata::ArtifactId::new),
        );
        request = request.executions(
            self.executions
                .iter()
                .copied()
                .map(mlmd::metadata::ExecutionId::new),
        );
        request = match (self.ctime_start, self.ctime_end) {
            (None, None) => request,
            (Some(s), None) => request.create_time(Duration::from_secs_f64(s)..),
            (None, Some(e)) => request.create_time(..Duration::from_secs_f64(e)),
            (Some(s), Some(e)) => {
                request.create_time(Duration::from_secs_f64(s)..Duration::from_secs_f64(e))
            }
        };
        request = match (self.mtime_start, self.mtime_end) {
            (None, None) => request,
            (Some(s), None) => request.update_time(Duration::from_secs_f64(s)..),
            (None, Some(e)) => request.update_time(..Duration::from_secs_f64(e)),
            (Some(s), Some(e)) => {
                request.update_time(Duration::from_secs_f64(s)..Duration::from_secs_f64(e))
            }
        };

        request
    }
}

/// Fields that can be used to sort a search result.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(missing_docs)]
pub enum ContextOrderByField {
    Id,
    Name,
    #[serde(rename = "ctime")]
    CreateTime,
    #[serde(rename = "mtime")]
    UpdateTime,
}

impl ContextOrderByField {
    const POSSIBLE_VALUES: &'static [&'static str] = &["id", "name", "ctime", "mtime"];
}

impl Default for ContextOrderByField {
    fn default() -> Self {
        Self::Id
    }
}

impl std::str::FromStr for ContextOrderByField {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "id" => Ok(Self::Id),
            "name" => Ok(Self::Name),
            "ctime" => Ok(Self::CreateTime),
            "mtime" => Ok(Self::UpdateTime),
            _ => anyhow::bail!("invalid value: {:?}", s),
        }
    }
}

impl From<ContextOrderByField> for mlmd::requests::ContextOrderByField {
    fn from(x: ContextOrderByField) -> Self {
        match x {
            ContextOrderByField::Id => Self::Id,
            ContextOrderByField::Name => Self::Name,
            ContextOrderByField::CreateTime => Self::CreateTime,
            ContextOrderByField::UpdateTime => Self::UpdateTime,
        }
    }
}

/// `$ mlmdquery count contexts` options.
#[derive(Debug, Clone, structopt::StructOpt, serde::Serialize, serde::Deserialize)]
pub struct CountContextsOpt {
    /// Common options.
    #[structopt(flatten)]
    #[serde(flatten)]
    pub common: CommonContextsOpt,
}

impl CountContextsOpt {
    /// `$ mlmdquery count contexts` implementation.
    pub async fn count(&self, store: &mut mlmd::MetadataStore) -> anyhow::Result<usize> {
        let n = self.common.request(store).count().await?;
        Ok(n)
    }
}

/// `$ mlmdquery get contexts` options.
#[derive(Debug, Clone, structopt::StructOpt, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetContextsOpt {
    /// Common options.
    #[structopt(flatten)]
    #[serde(flatten)]
    pub common: CommonContextsOpt,

    /// Field to be used to sort a search result.
    #[structopt(long, default_value="id", possible_values = ContextOrderByField::POSSIBLE_VALUES)]
    #[serde(default)]
    pub order_by: ContextOrderByField,

    /// If specified, the search results will be sorted in ascending order.
    #[structopt(long)]
    #[serde(default)]
    pub asc: bool,

    /// Maximum number of artifacts in a search result.
    #[structopt(long, default_value = "100")]
    #[serde(default = "GetContextsOpt::limit_default")]
    pub limit: usize,

    /// Number of artifacts to be skipped from a search result.
    #[structopt(long, default_value = "0")]
    #[serde(default)]
    pub offset: usize,
}

impl GetContextsOpt {
    fn limit_default() -> usize {
        100
    }

    /// `$ mlmdquery get context` implementation.
    pub async fn get(&self, store: &mut mlmd::MetadataStore) -> anyhow::Result<Vec<Context>> {
        let contexts = self
            .common
            .request(store)
            .limit(self.limit)
            .offset(self.offset)
            .order_by(self.order_by.into(), self.asc)
            .execute()
            .await?;

        let context_types = self.get_context_types(store, &contexts).await?;
        Ok(contexts
            .into_iter()
            .map(|x| Context {
                id: x.id.get(),
                name: x.name,
                type_name: context_types[&x.type_id].clone(),
                ctime: x.create_time_since_epoch.as_secs_f64(),
                mtime: x.last_update_time_since_epoch.as_secs_f64(),
                properties: x
                    .properties
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
                custom_properties: x
                    .custom_properties
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
            })
            .collect())
    }

    async fn get_context_types(
        &self,
        store: &mut mlmd::MetadataStore,
        contexts: &[mlmd::metadata::Context],
    ) -> anyhow::Result<BTreeMap<mlmd::metadata::TypeId, String>> {
        Ok(store
            .get_context_types()
            .ids(
                contexts
                    .iter()
                    .map(|x| x.type_id)
                    .collect::<BTreeSet<_>>()
                    .into_iter(),
            )
            .execute()
            .await?
            .into_iter()
            .map(|x| (x.id, x.name))
            .collect::<BTreeMap<_, _>>())
    }
}
