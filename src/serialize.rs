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
