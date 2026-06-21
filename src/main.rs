use anyhow::Result;
use qb_api::{
    api::{hash_password, router, seed_admin_if_empty, AppState, ObjectStore},
    config::AppConfig,
    db::create_pool,
};
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = AppConfig::from_env()?;
    let pool = create_pool(&cfg.database_url, cfg.max_db_connections).await?;

    // Seed default admin account if the users table is empty.
    // Default password is "changeme" – MUST be changed after first login.
    match hash_password("changeme") {
        Ok(default_hash) => match seed_admin_if_empty(&pool, &default_hash).await {
            Ok(true) => {
                tracing::info!("seeded default admin user (username=admin, password=changeme)")
            }
            Ok(false) => {}
            Err(e) => tracing::warn!("could not seed admin user (table may not exist yet): {e}"),
        },
        Err(e) => tracing::warn!("could not hash default password: {e}"),
    }

    // Initialise filesystem-backed object store and migrate any legacy BYTEA
    // objects to disk.  This is idempotent and fast when nothing needs migrating.
    let object_store = ObjectStore::new(pool.clone(), cfg.object_store_dir.clone())?;
    object_store
        .migrate_legacy_objects()
        .await
        .inspect_err(|e| tracing::warn!("legacy object migration skipped: {e}"))
        .ok();

    let state = AppState {
        pool,
        object_store,
        database_url: cfg.database_url.clone(),
        export_dir: cfg.export_dir.clone(),
        jwt_secret: cfg.jwt_secret.clone(),
    };
    let app = router(state, &cfg.cors_origins);
    let listener = TcpListener::bind(cfg.bind_addr).await?;

    tracing::info!(addr = %cfg.bind_addr, "qb_api_rust listening");
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}
