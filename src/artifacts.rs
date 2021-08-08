use crate::serialize::Artifact;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct CommonArtifactsOpt {
    #[structopt(long, env = "MLMD_DB")]
    pub db: String,

    #[structopt(long = "id")]
    pub ids: Vec<i32>,

    #[structopt(long, requires("type"))]
    pub name: Option<String>,

    #[structopt(long = "type")]
    pub type_name: Option<String>,

    #[structopt(long)]
    pub uri: Option<String>,

    #[structopt(long)]
    pub context: Option<i32>,

    #[structopt(long)]
    pub ctime_start: Option<f64>,

    #[structopt(long)]
    pub ctime_end: Option<f64>,

    #[structopt(long)]
    pub mtime_start: Option<f64>,

    #[structopt(long)]
    pub mtime_end: Option<f64>,
}

impl CommonArtifactsOpt {
    fn request<'a>(
        &self,
        store: &'a mut mlmd::MetadataStore,
    ) -> mlmd::requests::GetArtifactsRequest<'a> {
        let mut request = store.get_artifacts();

        if !self.ids.is_empty() {
            request = request.ids(
                self.ids
                    .iter()
                    .copied()
                    .map(mlmd::metadata::ArtifactId::new),
            );
        }
        if let Some(name) = &self.name {
            let type_name = self.type_name.as_ref().expect("unreachable");
            request = request.type_and_name(type_name, name);
        }
        if let Some(x) = &self.uri {
            request = request.uri(x);
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

#[derive(Debug, Clone, Copy)]
pub enum ArtifactOrderByField {
    Id,
    Name,
    CreateTime,
    UpdateTime,
}

impl ArtifactOrderByField {
    pub const POSSIBLE_VALUES: &'static [&'static str] = &["id", "name", "ctime", "mtime"];
}

impl std::str::FromStr for ArtifactOrderByField {
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

impl From<ArtifactOrderByField> for mlmd::requests::ArtifactOrderByField {
    fn from(x: ArtifactOrderByField) -> Self {
        match x {
            ArtifactOrderByField::Id => Self::Id,
            ArtifactOrderByField::Name => Self::Name,
            ArtifactOrderByField::CreateTime => Self::CreateTime,
            ArtifactOrderByField::UpdateTime => Self::UpdateTime,
        }
    }
}

#[derive(Debug, structopt::StructOpt)]
pub struct CountArtifactsOpt {
    #[structopt(flatten)]
    pub common: CommonArtifactsOpt,
}

impl CountArtifactsOpt {
    pub async fn count(&self) -> anyhow::Result<usize> {
        let mut store = mlmd::MetadataStore::connect(&self.common.db).await?;
        let n = self.common.request(&mut store).count().await?;
        Ok(n)
    }
}

#[derive(Debug, structopt::StructOpt)]
pub struct GetArtifactsOpt {
    #[structopt(flatten)]
    pub common: CommonArtifactsOpt,

    #[structopt(long, default_value="id", possible_values = ArtifactOrderByField::POSSIBLE_VALUES)]
    pub order_by: ArtifactOrderByField,

    #[structopt(long)]
    pub asc: bool,

    #[structopt(long, default_value = "100")]
    pub limit: usize,

    #[structopt(long, default_value = "0")]
    pub offset: usize,
}

impl GetArtifactsOpt {
    pub async fn get(&self) -> anyhow::Result<Vec<Artifact>> {
        let mut store = mlmd::MetadataStore::connect(&self.common.db).await?;
        let artifacts = self
            .common
            .request(&mut store)
            .limit(self.limit)
            .offset(self.offset)
            .order_by(self.order_by.into(), self.asc)
            .execute()
            .await?;

        let artifact_types = self.get_artifact_types(&mut store, &artifacts).await?;
        Ok(artifacts
            .into_iter()
            .map(|x| Artifact {
                id: x.id.get(),
                name: x.name,
                type_name: artifact_types[&x.type_id].clone(),
                uri: x.uri,
                state: x.state.into(),
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

    async fn get_artifact_types(
        &self,
        store: &mut mlmd::MetadataStore,
        artifacts: &[mlmd::metadata::Artifact],
    ) -> anyhow::Result<BTreeMap<mlmd::metadata::TypeId, String>> {
        Ok(store
            .get_artifact_types()
            .ids(
                artifacts
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
