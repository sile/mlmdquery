use mlmd::metadata::{
    Artifact, ArtifactId, ArtifactType, Event, EventType, Execution, ExecutionId, ExecutionType,
    TypeId,
};
use mlmd::MetadataStore;
use palette::{Gradient, Srgb};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use tinytemplate::TinyTemplate;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeId {
    Artifact(ArtifactId),
    Execution(ExecutionId),
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Artifact(x) => write!(f, "{}@artifact", x.get()),
            Self::Execution(x) => write!(f, "{}@execution", x.get()),
        }
    }
}

#[derive(Debug)]
pub enum Node {
    Artifact(Artifact),
    Execution(Execution),
}

impl Node {
    pub fn id(&self) -> NodeId {
        match self {
            Self::Artifact(x) => NodeId::Artifact(x.id),
            Self::Execution(x) => NodeId::Execution(x.id),
        }
    }

    pub fn label(&self) -> String {
        match self {
            Self::Artifact(x) => x.id.get().to_string(),
            Self::Execution(x) => x.id.get().to_string(),
        }
    }

    pub fn color(&self, colors: &HashMap<TypeId, Srgb<u8>>) -> String {
        let type_id = match self {
            Self::Artifact(x) => x.type_id,
            Self::Execution(x) => x.type_id,
        };
        let color = colors[&type_id];
        format!("#{:02x}{:02x}{:02x}", color.red, color.green, color.blue)
    }

    pub fn url(&self, template: Option<&TinyTemplate>) -> anyhow::Result<String> {
        if let Some(tt) = template {
            let context = match self {
                Self::Artifact(x) => UrlTemplateContext {
                    node_type: "artifact",
                    id: x.id.get(),
                },
                Self::Execution(x) => UrlTemplateContext {
                    node_type: "execution",
                    id: x.id.get(),
                },
            };
            Ok(tt.render("url", &context)?)
        } else {
            Ok("".to_owned())
        }
    }

    pub fn shape(&self) -> &str {
        match self {
            Self::Artifact(_) => "ellipse",
            Self::Execution(_) => "box",
        }
    }

    pub fn style(&self, origin: NodeId) -> &str {
        if self.id() == origin {
            "bold,dashed,filled"
        } else {
            "solid,filled"
        }
    }

    pub fn tooltip(&self, types: &BTreeMap<TypeId, Type>) -> anyhow::Result<String> {
        match self {
            Self::Artifact(x) => {
                let artifact = crate::serialize::ArtifactNode::new(
                    types[&x.type_id].name().to_owned(),
                    x.clone(),
                );
                Ok(serde_json::to_string_pretty(&artifact)?)
            }
            Self::Execution(x) => {
                let execution = crate::serialize::ExecutionNode::new(
                    types[&x.type_id].name().to_owned(),
                    x.clone(),
                );
                Ok(serde_json::to_string_pretty(&execution)?)
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Edge {
    event: Event,
}

impl Edge {
    pub fn new(event: Event) -> Self {
        Self { event }
    }

    pub fn label(&self) -> anyhow::Result<String> {
        let path = self
            .event
            .path
            .iter()
            .cloned()
            .map(crate::serialize::EventStep::from)
            .collect::<Vec<_>>();
        if path.is_empty() {
            return Ok("".to_owned());
        }
        Ok(serde_json::to_string(&path)?)
    }

    pub fn from_node(&self) -> NodeId {
        let is_input = matches!(
            self.event.ty,
            EventType::Input | EventType::DeclaredInput | EventType::InternalInput
        );
        if is_input {
            NodeId::Artifact(self.event.artifact_id)
        } else {
            NodeId::Execution(self.event.execution_id)
        }
    }

    pub fn to_node(&self) -> NodeId {
        let is_input = matches!(
            self.event.ty,
            EventType::Input | EventType::DeclaredInput | EventType::InternalInput
        );
        if is_input {
            NodeId::Execution(self.event.execution_id)
        } else {
            NodeId::Artifact(self.event.artifact_id)
        }
    }
}

#[derive(Debug, serde::Serialize)]
struct UrlTemplateContext {
    node_type: &'static str,
    id: i32,
}

#[derive(Debug)]
pub enum Type {
    Artifact(ArtifactType),
    Execution(ExecutionType),
}

impl Type {
    pub fn id(&self) -> TypeId {
        match self {
            Self::Artifact(x) => x.id,
            Self::Execution(x) => x.id,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Artifact(x) => &x.name,
            Self::Execution(x) => &x.name,
        }
    }

    pub fn shape(&self) -> &str {
        match self {
            Self::Artifact(_) => "ellipse",
            Self::Execution(_) => "box",
        }
    }
}

#[derive(Debug)]
pub struct Graph {
    origin: NodeId,
    nodes: HashMap<NodeId, Node>,
    edges: HashSet<Edge>,
    types: BTreeMap<TypeId, Type>,
    colors: HashMap<TypeId, Srgb<u8>>,
    url_template: Option<String>,
}

impl Graph {
    pub async fn new(
        store: &mut MetadataStore,
        origin: NodeId,
        nodes: HashMap<NodeId, Node>,
        edges: HashSet<Edge>,
        url_template: Option<String>,
    ) -> anyhow::Result<Self> {
        let mut types = BTreeMap::new();
        types.extend(
            store
                .get_artifact_types()
                .ids(
                    nodes
                        .values()
                        .filter_map(|x| {
                            if let Node::Artifact(x) = x {
                                Some(x)
                            } else {
                                None
                            }
                        })
                        .map(|x| x.type_id),
                )
                .execute()
                .await?
                .into_iter()
                .map(|x| (x.id, Type::Artifact(x))),
        );
        let artifact_type_count = types.len();

        types.extend(
            store
                .get_execution_types()
                .ids(
                    nodes
                        .values()
                        .filter_map(|x| {
                            if let Node::Execution(x) = x {
                                Some(x)
                            } else {
                                None
                            }
                        })
                        .map(|x| x.type_id),
                )
                .execute()
                .await?
                .into_iter()
                .map(|x| (x.id, Type::Execution(x))),
        );
        let execution_type_count = types.len() - artifact_type_count;

        let gradient = Gradient::new(vec![
            Srgb::new(1.0, 1.0, 1.0).into_linear(),
            Srgb::new(0.5, 0.5, 0.5).into_linear(),
        ]);
        let colors = types
            .iter()
            .filter_map(|(id, ty)| {
                if matches!(ty, Type::Artifact(_)) {
                    Some(*id)
                } else {
                    None
                }
            })
            .zip(gradient.take(artifact_type_count))
            .map(|(id, color)| (id, Srgb::<u8>::from(color)))
            .chain(
                types
                    .iter()
                    .filter_map(|(id, ty)| {
                        if matches!(ty, Type::Execution(_)) {
                            Some(*id)
                        } else {
                            None
                        }
                    })
                    .zip(gradient.take(execution_type_count))
                    .map(|(id, color)| (id, Srgb::<u8>::from(color))),
            )
            .collect();

        Ok(Self {
            origin,
            nodes,
            edges,
            types,
            colors,
            url_template,
        })
    }

    pub fn generate<W: Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        let url_template = if let Some(x) = &self.url_template {
            let mut tt = TinyTemplate::new();
            tt.add_template("url", x)?;
            Some(tt)
        } else {
            None
        };

        writeln!(writer, "digraph artifact_lineage_graph {{")?;
        writeln!(writer, "  concentrate=true;")?;
        for node in self.nodes.values() {
            writeln!(
                writer,
                "  {:?} [label={:?},shape={:?},style={:?},tooltip={:?},fillcolor={:?},URL={:?}];",
                node.id().to_string(),
                node.label(),
                node.shape(),
                node.style(self.origin),
                node.tooltip(&self.types)?,
                node.color(&self.colors),
                node.url(url_template.as_ref())?
            )?;
        }

        for edge in &self.edges {
            writeln!(
                writer,
                "  {:?} -> {:?} [label={:?}];",
                self.nodes[&edge.from_node()].id().to_string(),
                self.nodes[&edge.to_node()].id().to_string(),
                edge.label()?
            )?;
        }

        writeln!(writer, "  subgraph cluster_artifact_legend {{")?;
        writeln!(writer, "    label = \"Artifact Legend\";")?;
        let mut prev = None;
        for ty in self.types.values() {
            if matches!(ty, Type::Artifact(_)) {
                writeln!(
                    writer,
                    "    {:?}[shape={:?},style=filled,fillcolor=\"#{:02x}{:02x}{:02x}\"];",
                    ty.name(),
                    ty.shape(),
                    self.colors[&ty.id()].red,
                    self.colors[&ty.id()].green,
                    self.colors[&ty.id()].blue
                )?;
                if let Some(prev) = prev {
                    writeln!(
                        writer,
                        "{:?} -> {:?}[penwidth=0,arrowhead=none];",
                        prev,
                        ty.name()
                    )?;
                }
                prev = Some(ty.name());
            }
        }
        writeln!(writer, "  }}")?;

        writeln!(writer, "  subgraph cluster_execution_legend {{")?;
        writeln!(writer, "    label = \"Execution Legend\";")?;
        let mut prev = None;
        for ty in self.types.values() {
            if matches!(ty, Type::Execution(_)) {
                writeln!(
                    writer,
                    "    {:?}[shape={:?},style=filled,fillcolor=\"#{:02x}{:02x}{:02x}\"];",
                    ty.name(),
                    ty.shape(),
                    self.colors[&ty.id()].red,
                    self.colors[&ty.id()].green,
                    self.colors[&ty.id()].blue
                )?;
                if let Some(prev) = prev {
                    writeln!(
                        writer,
                        "{:?} -> {:?}[penwidth=0,arrowhead=none];",
                        prev,
                        ty.name()
                    )?;
                }
                prev = Some(ty.name());
            }
        }
        writeln!(writer, "  }}")?;

        writeln!(writer, "}}")?;
        Ok(())
    }
}
