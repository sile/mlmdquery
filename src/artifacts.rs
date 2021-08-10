//! `$ mlmdquery {get,count} artifacts` implementation.
use crate::serialize::Artifact;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// `$ mlmdquery {get,count} artifacts` common options.
#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct CommonArtifactsOpt {
    /// Database URL.
    #[structopt(long, env = "MLMD_DB", hide_env_values = true)]
    pub db: String,

    /// Target artifact IDs.
    #[structopt(long = "id")]
    pub ids: Vec<i32>,

    /// Target artifact name.
    #[structopt(long, requires("type-name"))]
    pub name: Option<String>,

    /// Target artifact type.
    #[structopt(long = "type")]
    pub type_name: Option<String>,

    /// Target artifact URI.
    #[structopt(long)]
    pub uri: Option<String>,

    /// Context ID to which target artifacts belong.
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
        match (&self.name, &self.type_name) {
            (Some(name), Some(type_name)) => {
                request = request.type_and_name(type_name, name);
            }
            (None, Some(type_name)) => {
                request = request.ty(type_name);
            }
            _ => {}
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

/// Fields that can be used to sort a search result.
#[derive(Debug, Clone, Copy)]
#[allow(missing_docs)]
pub enum ArtifactOrderByField {
    Id,
    Name,
    CreateTime,
    UpdateTime,
}

impl ArtifactOrderByField {
    const POSSIBLE_VALUES: &'static [&'static str] = &["id", "name", "ctime", "mtime"];
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

/// `$ mlmdquery count artifacts` options.
#[derive(Debug, structopt::StructOpt)]
pub struct CountArtifactsOpt {
    /// Common options.
    #[structopt(flatten)]
    pub common: CommonArtifactsOpt,
}

impl CountArtifactsOpt {
    /// `$ mlmdquery count artifacts` implementation.
    pub async fn count(&self) -> anyhow::Result<usize> {
        let mut store = mlmd::MetadataStore::connect(&self.common.db).await?;
        let n = self.common.request(&mut store).count().await?;
        Ok(n)
    }
}

/// `$ mlmdquery get artifacts` options.
#[derive(Debug, structopt::StructOpt)]
pub struct GetArtifactsOpt {
    /// Common options.
    #[structopt(flatten)]
    pub common: CommonArtifactsOpt,

    /// Field to be used to sort a search result.
    #[structopt(long, default_value="id", possible_values = ArtifactOrderByField::POSSIBLE_VALUES)]
    pub order_by: ArtifactOrderByField,

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

impl GetArtifactsOpt {
    /// `$ mlmdquery get artifacts` implementation.
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
