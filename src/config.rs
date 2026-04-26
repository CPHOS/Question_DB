//! Runtime configuration loaded from environment variables.

use std::{env, net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfig {
    pub database_url: String,
    pub bind_addr: SocketAddr,
    pub max_db_connections: u32,
    pub export_dir: PathBuf,
    pub object_store_dir: PathBuf,
    pub cors_origins: Vec<String>,
    pub jwt_secret: String,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let database_url =
            env::var("QB_DATABASE_URL").context("QB_DATABASE_URL is required for Rust API")?;

        let bind_addr = env::var("QB_BIND_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
            .parse()
            .context("QB_BIND_ADDR must be a valid socket address, e.g. 127.0.0.1:8080")?;

        let max_db_connections = env::var("QB_MAX_DB_CONNECTIONS")
            .unwrap_or_else(|_| "10".to_string())
            .parse()
            .context("QB_MAX_DB_CONNECTIONS must be a positive integer")?;

        let export_dir = env::var("QB_EXPORT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("exports"));

        let object_store_dir = env::var("QB_OBJECT_STORE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/var/lib/qb/objects"));

        let cors_origins = env::var("QB_CORS_ORIGINS")
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();

        let jwt_secret = env::var("QB_JWT_SECRET")
            .unwrap_or_else(|_| "qb-dev-secret-change-me-in-production".to_string());

        Ok(Self {
            database_url,
            bind_addr,
            max_db_connections,
            export_dir,
            object_store_dir,
            cors_origins,
            jwt_secret,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::AppConfig;
    use serial_test::serial;
    use std::env;

    #[test]
    #[serial]
    fn config_reads_env_and_uses_default_bind_addr() {
        let prev_db = env::var("QB_DATABASE_URL").ok();
        let prev_bind = env::var("QB_BIND_ADDR").ok();
        let prev_conn = env::var("QB_MAX_DB_CONNECTIONS").ok();
        let prev_export = env::var("QB_EXPORT_DIR").ok();
        let prev_object_store = env::var("QB_OBJECT_STORE_DIR").ok();
        let prev_cors = env::var("QB_CORS_ORIGINS").ok();

        env::set_var(
            "QB_DATABASE_URL",
            "postgres://postgres:postgres@localhost/qb",
        );
        env::remove_var("QB_BIND_ADDR");
        env::remove_var("QB_MAX_DB_CONNECTIONS");
        env::remove_var("QB_EXPORT_DIR");
        env::remove_var("QB_OBJECT_STORE_DIR");
        env::remove_var("QB_CORS_ORIGINS");

        let cfg = AppConfig::from_env().expect("config should load");
        assert_eq!(cfg.bind_addr.to_string(), "127.0.0.1:8080");
        assert_eq!(
            cfg.database_url,
            "postgres://postgres:postgres@localhost/qb"
        );
        assert_eq!(cfg.max_db_connections, 10);
        assert_eq!(cfg.export_dir.to_str().unwrap(), "exports");
        assert_eq!(
            cfg.object_store_dir.to_str().unwrap(),
            "/var/lib/qb/objects"
        );
        assert!(cfg.cors_origins.is_empty());

        // Restore
        match prev_db {
            Some(v) => env::set_var("QB_DATABASE_URL", v),
            None => env::remove_var("QB_DATABASE_URL"),
        }
        match prev_bind {
            Some(v) => env::set_var("QB_BIND_ADDR", v),
            None => {}
        }
        match prev_conn {
            Some(v) => env::set_var("QB_MAX_DB_CONNECTIONS", v),
            None => {}
        }
        match prev_export {
            Some(v) => env::set_var("QB_EXPORT_DIR", v),
            None => {}
        }
        match prev_object_store {
            Some(v) => env::set_var("QB_OBJECT_STORE_DIR", v),
            None => {}
        }
        match prev_cors {
            Some(v) => env::set_var("QB_CORS_ORIGINS", v),
            None => {}
        }
    }
}
