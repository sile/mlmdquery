use crate::serialize::Context;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct CommonContextsOpt {
    #[structopt(long, env = "MLMD_DB")]
    pub db: String,

    #[structopt(long = "id")]
    pub ids: Vec<i32>,

    #[structopt(long, requires("type-name"))]
    pub name: Option<String>,

    #[structopt(long = "type")]
    pub type_name: Option<String>,

    #[structopt(long)]
    pub artifact: Option<i32>,

    #[structopt(long)]
    pub execution: Option<i32>,

    #[structopt(long)]
    pub ctime_start: Option<f64>,

    #[structopt(long)]
    pub ctime_end: Option<f64>,

    #[structopt(long)]
    pub mtime_start: Option<f64>,

    #[structopt(long)]
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
        match (&self.name, &self.type_name) {
            (Some(name), Some(type_name)) => {
                request = request.type_and_name(type_name, name);
            }
            (None, Some(type_name)) => {
                request = request.ty(type_name);
            }
            _ => {}
        }
        if let Some(x) = self.artifact {
            request = request.artifact(mlmd::metadata::ArtifactId::new(x));
        }
        if let Some(x) = self.execution {
            request = request.execution(mlmd::metadata::ExecutionId::new(x));
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

#[derive(Debug, Clone, Copy)]
pub enum ContextOrderByField {
    Id,
    Name,
    CreateTime,
    UpdateTime,
}

impl ContextOrderByField {
    pub const POSSIBLE_VALUES: &'static [&'static str] = &["id", "name", "ctime", "mtime"];
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

#[derive(Debug, structopt::StructOpt)]
pub struct CountContextsOpt {
    #[structopt(flatten)]
    pub common: CommonContextsOpt,
}

impl CountContextsOpt {
    pub async fn count(&self) -> anyhow::Result<usize> {
        let mut store = mlmd::MetadataStore::connect(&self.common.db).await?;
        let n = self.common.request(&mut store).count().await?;
        Ok(n)
    }
}

#[derive(Debug, structopt::StructOpt)]
pub struct GetContextsOpt {
    #[structopt(flatten)]
    pub common: CommonContextsOpt,

    #[structopt(long, default_value="id", possible_values = ContextOrderByField::POSSIBLE_VALUES)]
    pub order_by: ContextOrderByField,

    #[structopt(long)]
    pub asc: bool,

    #[structopt(long, default_value = "100")]
    pub limit: usize,

    #[structopt(long, default_value = "0")]
    pub offset: usize,
}

impl GetContextsOpt {
    pub async fn get(&self) -> anyhow::Result<Vec<Context>> {
        let mut store = mlmd::MetadataStore::connect(&self.common.db).await?;
        let contexts = self
            .common
            .request(&mut store)
            .limit(self.limit)
            .offset(self.offset)
            .order_by(self.order_by.into(), self.asc)
            .execute()
            .await?;

        let context_types = self.get_context_types(&mut store, &contexts).await?;
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
