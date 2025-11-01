use lasso::ThreadedRodeo;
use std::sync::{Arc, OnceLock};

static GLOBAL_RODEO: OnceLock<Arc<ThreadedRodeo>> = OnceLock::new();

/// Returns a clone of the global `ThreadedRodeo`, initialising it on first use.
pub fn rodeo() -> Arc<ThreadedRodeo> {
    GLOBAL_RODEO
        .get_or_init(|| Arc::new(ThreadedRodeo::default()))
        .clone()
}

/// Returns a shared reference to the global `ThreadedRodeo`.
pub fn rodeo_ref() -> &'static ThreadedRodeo {
    GLOBAL_RODEO
        .get_or_init(|| Arc::new(ThreadedRodeo::default()))
        .as_ref()
}
