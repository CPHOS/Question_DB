use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use qb_api::api::{router, AppState, ObjectStore};
use sqlx::postgres::PgPoolOptions;
use std::path::PathBuf;
use tower::ServiceExt;

#[tokio::test]
async fn health_route_returns_service_unavailable_when_db_is_unreachable() {
    let pool = PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(50))
        .max_connections(1)
        .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/qb")
        .unwrap();
    let temp_dir = std::env::temp_dir().join("qb_test_health_objects");
    let object_store = ObjectStore::new(pool.clone(), temp_dir).expect("create test object store");
    let app = router(
        AppState {
            pool,
            object_store,
            database_url: "postgres://postgres:postgres@127.0.0.1:1/qb".to_string(),
            export_dir: PathBuf::from("exports"),
            jwt_secret: "test-secret".to_string(),
        },
        &[],
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).expect("body should be JSON");
    assert!(json.get("error").is_some());
}
