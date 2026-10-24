//! HTTP handlers for the hand-written endpoints (the admin CRUD comes from
//! viewset, see [`crate::admin`]).
//!
//! | Method | Path                                  | Body/Query         |
//! |--------|---------------------------------------|--------------------|
//! | POST   | `/{namespace}/credit`                 | [`CreditRequest`]  |
//! | GET    | `/{namespace}/statement/{user_id}`    | `?since=<RFC3339>` |
//!
//! Errors are viewset's [`ApiError`], so they render as the same JSON bodies
//! as the admin endpoints (and 5xx details are never leaked).

use actix_web::{HttpResponse, web};
use uuid::Uuid;
use viewset::ApiError;

use crate::models::{CreditRequest, StatementQuery, StatementResponse};
use crate::service::PaymentService;

/// `POST /{namespace}/credit` → `200 OK` with no body.
pub async fn post_credit(
    service: web::Data<PaymentService>,
    body: web::Json<CreditRequest>,
) -> Result<HttpResponse, ApiError> {
    service.credit(body.into_inner()).await?;
    Ok(HttpResponse::Ok().finish())
}

/// `GET /{namespace}/statement/{user_id}?since=…` → [`StatementResponse`].
/// A missing or malformed `since` is rejected with 400 by the `Query` extractor.
pub async fn get_statement(
    service: web::Data<PaymentService>,
    path: web::Path<Uuid>,
    query: web::Query<StatementQuery>,
) -> Result<HttpResponse, ApiError> {
    let user_id = path.into_inner();
    let entries = service.statement(user_id, query.since).await?;
    Ok(HttpResponse::Ok().json(StatementResponse { user_id, entries }))
}
