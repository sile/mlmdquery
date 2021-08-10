//! `$ mlmdquery {get,count} executions` implementation.
use crate::serialize::Execution;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// `$ mlmdquery {get,count} executions` common options.
#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct CommonExecutionsOpt {
    /// Database URL.
    #[structopt(long, env = "MLMD_DB", hide_env_values = true)]
    pub db: String,

    /// Target execution IDs.
    #[structopt(long = "id")]
    pub ids: Vec<i32>,

    /// Target execution name.
    #[structopt(long, requires("type-name"))]
    pub name: Option<String>,

    /// Target execution type.
    #[structopt(long = "type")]
    pub type_name: Option<String>,

    /// Context ID to which target executions belong.
    #[structopt(long)]
    pub context: Option<i32>,

    /// Start of creation time (UNIX timestamp seconds).
    #[structopt(long)]
    pub ctime_start: Option<f64>,

    /// End of creation time (UNIX timestamp seconds).
    #[structopt(long)]
    pub ctime_end: Option<f64>,

    /// Start of update time (UNIX timestamp seconds).
    #[structopt(long)]
    pub mtime_start: Option<f64>,

    /// End of update time (UNIX timestamp seconds).
    #[structopt(long)]
    pub mtime_end: Option<f64>,
}

impl CommonExecutionsOpt {
    fn request<'a>(
        &self,
        store: &'a mut mlmd::MetadataStore,
    ) -> mlmd::requests::GetExecutionsRequest<'a> {
        let mut request = store.get_executions();

        if !self.ids.is_empty() {
            request = request.ids(
                self.ids
                    .iter()
                    .copied()
                    .map(mlmd::metadata::ExecutionId::new),
            );
        }
        match (&self.name, &self.type_name) {
            (Some(name), Some(type_name)) => {
                request = request.type_and_name(type_name, name);
            }
            (None, Some(type_name)) => {
                request = request.ty(type_name);
            }
            _ => {}
        }
        if let Some(x) = self.context {
            request = request.context(mlmd::metadata::ContextId::new(x));
        }
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
#[derive(Debug, Clone, Copy)]
#[allow(missing_docs)]
pub enum ExecutionOrderByField {
    Id,
    Name,
    CreateTime,
    UpdateTime,
}

impl ExecutionOrderByField {
    const POSSIBLE_VALUES: &'static [&'static str] = &["id", "name", "ctime", "mtime"];
}

impl std::str::FromStr for ExecutionOrderByField {
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

impl From<ExecutionOrderByField> for mlmd::requests::ExecutionOrderByField {
    fn from(x: ExecutionOrderByField) -> Self {
        match x {
            ExecutionOrderByField::Id => Self::Id,
            ExecutionOrderByField::Name => Self::Name,
            ExecutionOrderByField::CreateTime => Self::CreateTime,
            ExecutionOrderByField::UpdateTime => Self::UpdateTime,
        }
    }
}

/// `$ mlmdquery count executions` options.
#[derive(Debug, structopt::StructOpt)]
pub struct CountExecutionsOpt {
    /// Common options.
    #[structopt(flatten)]
    pub common: CommonExecutionsOpt,
}

impl CountExecutionsOpt {
    /// `$ mlmdquery count executions` implementation.
    pub async fn count(&self) -> anyhow::Result<usize> {
        let mut store = mlmd::MetadataStore::connect(&self.common.db).await?;
        let n = self.common.request(&mut store).count().await?;
        Ok(n)
    }
}

/// `$ mlmdquery get executions` options.
#[derive(Debug, structopt::StructOpt)]
pub struct GetExecutionsOpt {
    /// Common options.
    #[structopt(flatten)]
    pub common: CommonExecutionsOpt,

    /// Field to be used to sort a search result.
    #[structopt(long, default_value="id", possible_values = ExecutionOrderByField::POSSIBLE_VALUES)]
    pub order_by: ExecutionOrderByField,

    /// If specified, the search results will be sorted in ascending order.
    #[structopt(long)]
    pub asc: bool,

    /// Maximum number of artifacts in a search result.
    #[structopt(long, default_value = "100")]
    pub limit: usize,

    /// Number of artifacts to be skipped from a search result.
    #[structopt(long, default_value = "0")]
    pub offset: usize,
}

impl GetExecutionsOpt {
    /// `$ mlmdquery get executions` implementation.
    pub async fn get(&self) -> anyhow::Result<Vec<Execution>> {
        let mut store = mlmd::MetadataStore::connect(&self.common.db).await?;
        let executions = self
            .common
            .request(&mut store)
            .limit(self.limit)
            .offset(self.offset)
            .order_by(self.order_by.into(), self.asc)
            .execute()
            .await?;

        let execution_types = self.get_execution_types(&mut store, &executions).await?;
        Ok(executions
            .into_iter()
            .map(|x| Execution {
                id: x.id.get(),
                name: x.name,
                type_name: execution_types[&x.type_id].clone(),
                state: x.last_known_state.into(),
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

    async fn get_execution_types(
        &self,
        store: &mut mlmd::MetadataStore,
        executions: &[mlmd::metadata::Execution],
    ) -> anyhow::Result<BTreeMap<mlmd::metadata::TypeId, String>> {
        Ok(store
            .get_execution_types()
            .ids(
                executions
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