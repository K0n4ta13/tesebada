use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub(crate) tables: Vec<Table>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct Table {
    pub(crate) name: String,
    pub(crate) fields: Vec<Field>,
    #[serde(default)]
    pub(crate) fragments: Vec<Fragment>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct Fragment {
    pub(crate) name: String,
    pub(crate) connection: String,
    pub(crate) manager: String,
    pub(crate) zone: String,
    pub(crate) fields: Vec<FragmentField>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct Field {
    pub(crate) name: String,
    pub(crate) r#type: String,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct FragmentField {
    pub(crate) name: String,
    pub(crate) reference: String,
    pub(crate) r#type: String,
}
