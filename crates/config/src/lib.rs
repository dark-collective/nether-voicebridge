#![allow(dead_code)] // secret fields are read in later phases

use thiserror::Error;

mod banlist;
mod optout;
mod tenants;
mod tenants_edit;
mod schema;
mod resolve;
mod validate;
#[cfg(test)]
mod tests;
pub use banlist::{BanKind, BanList, add_ban, banlist_path, load_banlist, remove_ban};
pub use optout::{
    ApprovalOutcome, OptoutList, add_optout, load_optout, optout_path, remove_optout,
};
pub use tenants::{TenantsFile, list_tenant_names, load_tenants, tenants_path};
pub use tenants_edit::{
    NewTenant, PriorContent, add_tenant_entry, remove_tenant_entries_matching,
    remove_tenant_entry, restore_tenants_file, tenant_owners,
};
pub use resolve::*;
pub use schema::*;
pub use validate::*;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file '{path}': {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("config validation error: {0}")]
    Validation(String),
    #[error("missing required environment variables: {0}")]
    MissingEnv(String),
    #[error("invalid environment variable value: {0}")]
    InvalidEnv(String),
}
