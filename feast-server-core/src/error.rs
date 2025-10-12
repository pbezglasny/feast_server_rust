use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeastCoreError {
    FeatureServiceNotFound { name: String },
    FeatureViewNotFound {
        feature_view_name: String,
        service_name: Option<String>,
    },
}

impl FeastCoreError {
    pub fn feature_service_not_found(name: impl Into<String>) -> Self {
        Self::FeatureServiceNotFound { name: name.into() }
    }

    pub fn feature_view_not_found(feature_view_name: impl Into<String>) -> Self {
        Self::FeatureViewNotFound {
            feature_view_name: feature_view_name.into(),
            service_name: None,
        }
    }

    pub fn feature_view_not_found_for_service(
        feature_view_name: impl Into<String>,
        service_name: impl Into<String>,
    ) -> Self {
        Self::FeatureViewNotFound {
            feature_view_name: feature_view_name.into(),
            service_name: Some(service_name.into()),
        }
    }

    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Self::FeatureServiceNotFound { .. } | Self::FeatureViewNotFound { .. }
        )
    }
}

impl Display for FeastCoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::FeatureServiceNotFound { name } => {
                write!(f, "Feature service '{}' not found", name)
            }
            Self::FeatureViewNotFound {
                feature_view_name,
                service_name: Some(service_name),
            } => write!(
                f,
                "Feature view '{}' not found for service '{}'",
                feature_view_name, service_name
            ),
            Self::FeatureViewNotFound {
                feature_view_name,
                service_name: None,
            } => write!(f, "Feature view '{}' not found", feature_view_name),
        }
    }
}

impl std::error::Error for FeastCoreError {}
