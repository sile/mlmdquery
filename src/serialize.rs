use std::collections::BTreeMap;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Type {
    pub id: i32,
    pub name: String,
    pub properties: BTreeMap<String, PropertyType>,
}

impl From<mlmd::metadata::ArtifactType> for Type {
    fn from(x: mlmd::metadata::ArtifactType) -> Self {
        Self {
            id: x.id.get(),
            name: x.name,
            properties: x
                .properties
                .into_iter()
                .map(|(k, v)| (k, PropertyType::from(v)))
                .collect(),
        }
    }
}

impl From<mlmd::metadata::ExecutionType> for Type {
    fn from(x: mlmd::metadata::ExecutionType) -> Self {
        Self {
            id: x.id.get(),
            name: x.name,
            properties: x
                .properties
                .into_iter()
                .map(|(k, v)| (k, PropertyType::from(v)))
                .collect(),
        }
    }
}

impl From<mlmd::metadata::ContextType> for Type {
    fn from(x: mlmd::metadata::ContextType) -> Self {
        Self {
            id: x.id.get(),
            name: x.name,
            properties: x
                .properties
                .into_iter()
                .map(|(k, v)| (k, PropertyType::from(v)))
                .collect(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PropertyType {
    Int,
    Double,
    String,
}

impl From<mlmd::metadata::PropertyType> for PropertyType {
    fn from(x: mlmd::metadata::PropertyType) -> Self {
        use mlmd::metadata::PropertyType::*;

        match x {
            Int => Self::Int,
            Double => Self::Double,
            String => Self::String,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Event {
    pub artifact: i32,
    pub artifact_type: String,
    pub execution: i32,
    pub execution_type: String,
    #[serde(rename = "type")]
    pub event_type: EventType,
    pub path: Vec<EventStep>,
    pub time: f64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum EventType {
    Unknown,
    Input,
    DeclaredInput,
    InternalInput,
    Output,
    DeclaredOutput,
    InternalOutput,
}

impl From<mlmd::metadata::EventType> for EventType {
    fn from(x: mlmd::metadata::EventType) -> Self {
        use mlmd::metadata::EventType::*;

        match x {
            Unknown => Self::Unknown,
            Input => Self::Input,
            DeclaredInput => Self::DeclaredInput,
            InternalInput => Self::InternalInput,
            Output => Self::Output,
            DeclaredOutput => Self::DeclaredOutput,
            InternalOutput => Self::InternalOutput,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum EventStep {
    Index(i32),
    Key(String),
}

impl std::fmt::Display for EventStep {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Index(x) => write!(f, "{}", x),
            Self::Key(x) => write!(f, "{}", x),
        }
    }
}

impl From<mlmd::metadata::EventStep> for EventStep {
    fn from(x: mlmd::metadata::EventStep) -> Self {
        use mlmd::metadata::EventStep::*;

        match x {
            Index(x) => Self::Index(x),
            Key(x) => Self::Key(x),
        }
    }
}
