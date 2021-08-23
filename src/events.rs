//! `$ mlmdquery {get,count} events` implementation.
use crate::serialize::Event;
use std::collections::BTreeMap;

/// `$ mlmdquery {get,count} events` options.
#[derive(Debug, Clone, structopt::StructOpt, serde::Serialize, serde::Deserialize)]
#[structopt(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub struct CommonEventsOpt {
    /// Database URL.
    #[structopt(long, env = "MLMD_DB", hide_env_values = true)]
    #[serde(skip)]
    pub db: String,

    /// Artifact ID relating to target events.
    #[structopt(long)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<i32>,

    /// Execution ID relating to target events.
    #[structopt(long)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<i32>,
}

impl CommonEventsOpt {
    fn request<'a>(
        &self,
        store: &'a mut mlmd::MetadataStore,
    ) -> mlmd::requests::GetEventsRequest<'a> {
        let mut request = store.get_events();
        if let Some(x) = self.artifact {
            request = request.artifact(mlmd::metadata::ArtifactId::new(x));
        }
        if let Some(x) = self.execution {
            request = request.execution(mlmd::metadata::ExecutionId::new(x));
        }
        request
    }
}

/// `$ mlmdquery count events` options.
#[derive(Debug, Clone, structopt::StructOpt, serde::Serialize, serde::Deserialize)]
pub struct CountEventsOpt {
    /// Common options.
    #[structopt(flatten)]
    #[serde(flatten)]
    pub common: CommonEventsOpt,
}

impl CountEventsOpt {
    /// `$ mlmdquery count events` implementation.
    pub async fn count(&self, store: &mut mlmd::MetadataStore) -> anyhow::Result<usize> {
        let n = self.common.request(store).count().await?;
        Ok(n)
    }
}

/// `$ mlmdquery get events` options.
#[derive(Debug, Clone, structopt::StructOpt, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetEventsOpt {
    /// Common options.
    #[structopt(flatten)]
    #[serde(flatten)]
    pub common: CommonEventsOpt,

    /// Maximum number of artifacts in a search result.
    #[structopt(long, default_value = "100")]
    #[serde(default = "GetEventsOpt::limit_default")]
    pub limit: usize,

    /// Number of artifacts to be skipped from a search result.
    #[structopt(long, default_value = "0")]
    #[serde(default)]
    pub offset: usize,

    /// If specified, the search results will be sorted in ascending order.
    #[structopt(long)]
    #[serde(default)]
    pub asc: bool,
}

impl GetEventsOpt {
    fn limit_default() -> usize {
        100
    }

    /// `$ mlmdquery get events` implementation.
    pub async fn get(&self, store: &mut mlmd::MetadataStore) -> anyhow::Result<Vec<Event>> {
        let events = self
            .common
            .request(store)
            .limit(self.limit)
            .offset(self.offset)
            .order_by(mlmd::requests::EventOrderByField::CreateTime, self.asc)
            .execute()
            .await?;

        let artifact_types = self
            .get_artifact_types(store, events.iter().map(|x| x.artifact_id))
            .await?;
        let execution_types = self
            .get_execution_types(store, events.iter().map(|x| x.execution_id))
            .await?;
        Ok(events
            .into_iter()
            .map(|x| Event {
                artifact: x.artifact_id.get(),
                artifact_type: artifact_types[&x.artifact_id].clone(),
                execution: x.execution_id.get(),
                execution_type: execution_types[&x.execution_id].clone(),
                event_type: x.ty.into(),
                path: x.path.into_iter().map(From::from).collect(),
                time: x.create_time_since_epoch.as_secs_f64(),
            })
            .collect())
    }

    async fn get_artifact_types(
        &self,
        store: &mut mlmd::MetadataStore,
        artifact_ids: impl Iterator<Item = mlmd::metadata::ArtifactId>,
    ) -> anyhow::Result<BTreeMap<mlmd::metadata::ArtifactId, String>> {
        let artifacts = store
            .get_artifacts()
            .ids(artifact_ids)
            .execute()
            .await?
            .into_iter()
            .map(|x| (x.id, x.type_id))
            .collect::<BTreeMap<_, _>>();
        let artifact_types = store
            .get_artifact_types()
            .ids(artifacts.values().copied())
            .execute()
            .await?
            .into_iter()
            .map(|x| (x.id, x.name))
            .collect::<BTreeMap<_, _>>();
        Ok(artifacts
            .into_iter()
            .map(|(id, type_id)| (id, artifact_types[&type_id].clone()))
            .collect())
    }

    async fn get_execution_types(
        &self,
        store: &mut mlmd::MetadataStore,
        execution_ids: impl Iterator<Item = mlmd::metadata::ExecutionId>,
    ) -> anyhow::Result<BTreeMap<mlmd::metadata::ExecutionId, String>> {
        let executions = store
            .get_executions()
            .ids(execution_ids)
            .execute()
            .await?
            .into_iter()
            .map(|x| (x.id, x.type_id))
            .collect::<BTreeMap<_, _>>();
        let execution_types = store
            .get_execution_types()
            .ids(executions.values().copied())
            .execute()
            .await?
            .into_iter()
            .map(|x| (x.id, x.name))
            .collect::<BTreeMap<_, _>>();
        Ok(executions
            .into_iter()
            .map(|(id, type_id)| (id, execution_types[&type_id].clone()))
            .collect())
    }
}
