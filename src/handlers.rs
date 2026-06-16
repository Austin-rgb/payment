//! Actix-web HTTP handlers for the payment service.
//!
//! Only one endpoint is exposed:
//!
//! | Method | Path              | Body            | Description          |
//! |--------|-------------------|-----------------|----------------------|
//! | POST   | `/payment/credit` | [`CreditRequest`] | Credit a user account |

use std::sync::Arc;

use actix_web::{web, HttpResponse};
use tracing::error;

use crate::{
    event_store::EventStore,
    models::CreditRequest,
    pending_store::PendingDebitStore,
    repository::PaymentRepository,
    service::PaymentService,
};

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

/// Application state injected into every handler via `web::Data`.
pub struct AppState<R, P, E> {
    pub service: Arc<PaymentService<R, P, E>>,
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
async fn post_credit<R, P, E>(
    state: web::Data<AppState<R, P, E>>,
    body:  web::Json<CreditRequest>,
) -> HttpResponse
where
    R: PaymentRepository,
    P: PendingDebitStore,
    E: EventStore,
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
pub fn configure<R, P, E>(cfg: &mut web::ServiceConfig)
where
    R: PaymentRepository,
    P: PendingDebitStore,
    E: EventStore,
{
    cfg.service(
        web::scope("/payment")
            .route("/credit", web::post().to(post_credit::<R, P, E>)),
    );
}

