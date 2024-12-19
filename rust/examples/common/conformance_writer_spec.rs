use std::collections::BTreeMap;

use serde_json::Value;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Variant {
    pub features: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SpecMeta {
    pub variant: Variant,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Record {
    #[serde(rename = "type")]
    pub record_type: String,
    pub fields: Vec<(String, Value)>,
}

impl Record {
    pub fn get_field(&self, name: &str) -> &Value {
        &self
            .fields
            .iter()
            .find(|f| f.0 == name)
            .unwrap_or_else(|| panic!("Invalid: {}", name))
            .1
    }

    pub fn get_field_data(&self, name: &str) -> Vec<u8> {
        self.get_field(name)
            .as_array()
            .unwrap_or_else(|| panic!("Invalid: {}", name))
            .iter()
            .filter_map(|v| v.as_u64())
            .filter_map(|n| u8::try_from(n).ok())
            .collect()
    }

    pub fn get_field_meta(&self, name: &str) -> BTreeMap<String, String> {
        let data = self
            .get_field(name)
            .as_object()
            .unwrap_or_else(|| panic!("Invalid: {}", name));
        let mut result = BTreeMap::new();
        for (key, value) in data.iter() {
            result.insert(key.to_string(), value.as_str().unwrap().to_string());
        }
        result
    }

    pub fn get_field_str(&self, name: &str) -> &str {
        self.get_field(name)
            .as_str()
            .unwrap_or_else(|| panic!("Invalid: {}", name))
    }

    pub fn get_field_u16(&self, name: &str) -> u16 {
        self.get_field(name)
            .as_str()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or_else(|| panic!("Invalid: {}", name))
    }

    pub fn get_field_u32(&self, name: &str) -> u32 {
        self.get_field(name)
            .as_str()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or_else(|| panic!("Invalid: {}", name))
    }

    pub fn get_field_u64(&self, name: &str) -> u64 {
        self.get_field(name)
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(|| panic!("Invalid: {}", name))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct WriterSpec {
    pub meta: SpecMeta,
    pub records: Vec<Record>,
}
