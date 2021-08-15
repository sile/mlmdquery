//! `$ mlmdquery graph io` implementation.
use crate::graph::{Edge, Graph, Node, NodeId};
use mlmd::metadata::ExecutionId;
use mlmd::MetadataStore;
use std::collections::{HashMap, HashSet};
use std::io::Write;

/// `$ mlmdquery graph io` options.
#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct GraphIoOpt {
    /// Database URL.
    #[structopt(long, env = "MLMD_DB", hide_env_values = true)]
    pub db: String,

    /// Target execution ID.
    pub execution: i32,

    /// Template to generate node URLs.
    ///
    /// You can use the following variables in the template:
    /// - `{node_type}`: "artifact" or "execution":
    /// - `{id}`: Artifact or Execution ID (depending on `node_type`)
    ///
    /// Please refer to the [tinytemplate](https://docs.rs/tinytemplate/) doc for the features of the template engine.
    #[structopt(long)]
    pub url_template: Option<String>,
}

impl GraphIoOpt {
    /// `$ mlmdquery graph io` implementation.
    pub async fn graph<W: Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        let mut store = MetadataStore::connect(&self.db).await?;

        let origin = NodeId::Execution(ExecutionId::new(self.execution));
        let mut stack = vec![origin];
        let mut nodes = HashMap::new();
        let mut edges = HashSet::new();
        while let Some(id) = stack.pop() {
            if nodes.contains_key(&id) {
                continue;
            }

            let node = get_node(&mut store, id).await?;
            nodes.insert(id, node);

            for edge in get_edges(&mut store, id).await? {
                stack.push(edge.from_node());
                stack.push(edge.to_node());
                edges.insert(edge);
            }
        }

        let graph = Graph::new(&mut store, origin, nodes, edges, self.url_template.clone()).await?;
        graph.generate(writer)?;
        Ok(())
    }
}

async fn get_node(store: &mut MetadataStore, id: NodeId) -> anyhow::Result<Node> {
    match id {
        NodeId::Artifact(id) => {
            let mut artifacts = store.get_artifacts().id(id).execute().await?;
            anyhow::ensure!(artifacts.len() == 1, "No such artifact: {}", id.get());
            Ok(Node::Artifact(artifacts.remove(0)))
        }
        NodeId::Execution(id) => {
            let mut executions = store.get_executions().id(id).execute().await?;
            anyhow::ensure!(executions.len() == 1, "No such execution: {}", id.get());
            Ok(Node::Execution(executions.remove(0)))
        }
    }
}

async fn get_edges(store: &mut MetadataStore, id: NodeId) -> anyhow::Result<Vec<Edge>> {
    match id {
        NodeId::Artifact(_) => Ok(Vec::new()),
        NodeId::Execution(id) => {
            let events = store.get_events().execution(id).execute().await?;
            Ok(events.into_iter().map(Edge::new).collect())
        }
    }
}
