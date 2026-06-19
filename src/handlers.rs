//! Actix-web HTTP handlers for the payment service.
//!
//! | Method | Path                              | Body/Query             | Description            |
//! |--------|-----------------------------------|-------------------------|------------------------|
//! | POST   | `/payment/credit`                 | [`CreditRequest`]       | Credit a user account  |
//! | GET    | `/payment/statement/{user_id}`    | `?since=<RFC3339>`      | Get a user's statement |

use std::sync::Arc;

use actix_web::{HttpResponse, web};
use tracing::error;
use uuid::Uuid;

use crate::{
    models::{CreditRequest, StatementQuery, StatementResponse},
    pending_store::PendingDebitStore,
    repository::PaymentRepository,
    service::PaymentService,
};

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

/// Application state injected into every handler via `web::Data`.
pub struct AppState<R, P> {
    pub service: Arc<PaymentService<R, P>>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `POST /payment/credit`
///
/// Deserialises a [`CreditRequest`] from the request body, delegates to
/// [`PaymentService::credit`], and returns:
/// - `200 OK` on success (no body),
/// - `500 Internal Server Error` on any error.
async fn post_credit<R, P>(
    state: web::Data<AppState<R, P>>,
    body: web::Json<CreditRequest>,
) -> HttpResponse
where
    R: PaymentRepository,
    P: PendingDebitStore,
{
    match state.service.credit(body.into_inner()).await {
        Ok(()) => HttpResponse::Ok().finish(),
        Err(e) => {
            error!(error = %e, "POST /payment/credit failed");
            HttpResponse::InternalServerError().finish()
        }
    }
}

// ---------------------------------------------------------------------------
// Statement endpoint
// ---------------------------------------------------------------------------

/// `GET /payment/statement/{user_id}?since=<RFC3339 timestamp>`
///
/// Returns the user's balance-affecting history (credits and settled debits)
/// at or after `since`, sorted newest first. Pending (not-yet-settled)
/// debits are not included.
///
/// - `200 OK` with [`StatementResponse`] on success,
/// - `400 Bad Request` if `since` is missing or malformed (handled by Actix's
///   `Query` extractor before this function runs),
/// - `500 Internal Server Error` on any other error.
async fn get_statement<R, P>(
    state: web::Data<AppState<R, P>>,
    path: web::Path<Uuid>,
    query: web::Query<StatementQuery>,
) -> HttpResponse
where
    R: PaymentRepository,
    P: PendingDebitStore,
{
    let user_id = path.into_inner();

    match state.service.statement(user_id, query.since).await {
        Ok(entries) => HttpResponse::Ok().json(StatementResponse { user_id, entries }),
        Err(e) => {
            error!(error = %e, %user_id, "GET /payment/statement failed");
            HttpResponse::InternalServerError().finish()
        }
    }
}

// ---------------------------------------------------------------------------
// Route configuration
// ---------------------------------------------------------------------------

/// Register all payment routes on `cfg`.
///
/// # Example
///
/// ```rust,ignore
/// App::new()
///     .app_data(web::Data::new(state))
///     .configure(handlers::configure::<SqliteRepository, MokaPendingDebitStore, MokaEventStore>)
/// ```
pub fn configure<R, P>(cfg: &mut web::ServiceConfig)
where
    R: PaymentRepository,
    P: PendingDebitStore,
{
    cfg.service(
        web::scope("/payment")
            .route("/credit", web::post().to(post_credit::<R, P>))
            .route("/statement/{user_id}", web::get().to(get_statement::<R, P>)),
    );
}

