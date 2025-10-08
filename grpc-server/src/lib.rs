pub mod server;

pub mod proto {
    pub mod feast {
        pub mod serving {
            tonic::include_proto!("feast.serving");
        }
        pub mod types {
            tonic::include_proto!("feast.types");
        }
    }
}
