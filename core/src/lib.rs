pub mod feast {
    pub mod types {
        include!(concat!(env!("OUT_DIR"), "/feast.types.rs"));
    }
    pub mod core {
        include!(concat!(env!("OUT_DIR"), "/feast.core.rs"));
    }

    pub mod registry {
        include!(concat!(env!("OUT_DIR"), "/feast.registry.rs"));
    }
}

#[cfg(test)]
pub mod tests {
    use super::feast::core::*;
    use prost::Message;
    use std::fs;
    use std::io::Read;

    #[test]
    fn test_feature_row_creation() -> Result<(), Box<dyn std::error::Error>> {
        let registry_file = "/Users/pavel/work/rust/feast_rust/feast-protos/src/registry.pb";
        let mut file = fs::File::open(registry_file)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let registry = Registry::decode(&*buf);
        println!("Default Registry: {:?}", registry);
        Ok(())
    }
}
