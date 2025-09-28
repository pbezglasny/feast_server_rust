#![allow(unused)]

mod config;
mod feature_store;
mod key_serialization;
mod model;
mod util;

mod onlinestore;
mod registry;
mod proto_utils;

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

    pub mod serving {
        include!(concat!(env!("OUT_DIR"), "/feast.serving.rs"));
    }
}
