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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Artifact {
    pub id: i32,
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub type_name: String,
    pub uri: Option<String>,
    pub state: ArtifactState,
    pub ctime: f64,
    pub mtime: f64,
    pub properties: BTreeMap<String, PropertyValue>,
    pub custom_properties: BTreeMap<String, PropertyValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ArtifactState {
    Unknown,
    Pending,
    Live,
    MarkedForDeletion,
    Deleted,
}

impl From<mlmd::metadata::ArtifactState> for ArtifactState {
    fn from(x: mlmd::metadata::ArtifactState) -> Self {
        use mlmd::metadata::ArtifactState::*;

        match x {
            Unknown => Self::Unknown,
            Pending => Self::Pending,
            Live => Self::Live,
            MarkedForDeletion => Self::MarkedForDeletion,
            Deleted => Self::Deleted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum PropertyValue {
    Int(i32),
    Double(f64),
    String(String),
}

impl From<mlmd::metadata::PropertyValue> for PropertyValue {
    fn from(x: mlmd::metadata::PropertyValue) -> Self {
        use mlmd::metadata::PropertyValue::*;

        match x {
            Int(x) => Self::Int(x),
            Double(x) => Self::Double(x),
            String(x) => Self::String(x),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Execution {
    pub id: i32,
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub type_name: String,
    pub state: ExecutionState,
    pub ctime: f64,
    pub mtime: f64,
    pub properties: BTreeMap<String, PropertyValue>,
    pub custom_properties: BTreeMap<String, PropertyValue>,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionState {
    Unknown,
    New,
    Running,
    Complete,
    Failed,
    Cached,
    Canceled,
}

impl From<mlmd::metadata::ExecutionState> for ExecutionState {
    fn from(x: mlmd::metadata::ExecutionState) -> Self {
        use mlmd::metadata::ExecutionState::*;

        match x {
            Unknown => Self::Unknown,
            New => Self::New,
            Running => Self::Running,
            Complete => Self::Complete,
            Failed => Self::Failed,
            Cached => Self::Cached,
            Canceled => Self::Canceled,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Context {
    pub id: i32,
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub ctime: f64,
    pub mtime: f64,
    pub properties: BTreeMap<String, PropertyValue>,
    pub custom_properties: BTreeMap<String, PropertyValue>,
}
