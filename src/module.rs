//! The ferrumec [`Module`] for the payment service.

use std::sync::Arc;
use std::time::Duration;

use actix_web::web;
use ferrumec::event::Outbox;
use ferrumec::{Infra, Module};
use viewset::ViewSet as _;

use crate::admin::{Account, AdminViewSet, Credit, ProcessedDebit, admin_viewset};
use crate::handlers;
use crate::pending_store::CachePendingDebitStore;
use crate::service::PaymentService;
use crate::subscriptions;

/// How long a debit may wait for funds before `payment.debit.failed` is emitted.
const MAXIMUM_WAIT_MINUTES: i64 = 30;

/// Backstop expiry for the pending-debit caches; must comfortably exceed
/// [`MAXIMUM_WAIT_MINUTES`].
const PENDING_CACHE_TTL: Duration = Duration::from_mins(60);

/// Payment feature module. Register it under a namespace (conventionally
/// `"payment"`) with [`ferrumec::modules!`]; routes then live at
/// `/api/{namespace}/…` when run through `ferrumec::launch::launch`:
///
/// ```text
/// POST /api/payment/credit
/// GET  /api/payment/statement/{user_id}?since=<RFC3339>
/// *    /api/payment/admin/accounts[/{id}]          (viewset CRUD)
/// *    /api/payment/admin/credits[/{id}]
/// *    /api/payment/admin/processed-debits[/{id}]
/// ```
///
/// Building the module also starts the transactional outbox publisher and
/// subscribes to `payment.debit.request`.
pub struct PaymentModule {
    service: Arc<PaymentService>,
    accounts: Arc<AdminViewSet<Account>>,
    credits: Arc<AdminViewSet<Credit>>,
    processed_debits: Arc<AdminViewSet<ProcessedDebit>>,
}

impl Module for PaymentModule {
    async fn new(infra: impl Infra) -> Result<Self, Box<dyn std::error::Error>> {
        let pool = infra.database();
        let events = infra.event_stream();
        let cache_factory = infra.cache_factory();

        // Creates `outbox_events` if needed and starts the publisher task.
        let outbox = Outbox::new(pool.clone(), events.clone()).await?;
        let pending = Arc::new(CachePendingDebitStore::new(
            &cache_factory,
            PENDING_CACHE_TTL,
        ));
        let service = Arc::new(PaymentService::new(
            pool.clone(),
            outbox,
            pending,
            chrono::Duration::minutes(MAXIMUM_WAIT_MINUTES),
        ));

        // `EventError` is `Box<dyn Error + Send + Sync>`, which `?` can't turn into
        // `Box<dyn Error>` directly; go through its message.
        subscriptions::register(service.clone(), events)
            .await
            .map_err(|e| e.to_string())?;

        Ok(Self {
            service,
            accounts: admin_viewset(pool.clone()),
            credits: admin_viewset(pool.clone()),
            processed_debits: admin_viewset(pool),
        })
    }

    fn configure(self: Arc<Self>, cfg: &mut web::ServiceConfig, namespace: &str) {
        let accounts = self.accounts.clone();
        let credits = self.credits.clone();
        let processed_debits = self.processed_debits.clone();

        cfg.service(
            web::scope(namespace)
                .app_data(web::Data::from(self.service.clone()))
                .route("/credit", web::post().to(handlers::post_credit))
                .route(
                    "/statement/{user_id}",
                    web::get().to(handlers::get_statement),
                )
                .service(web::scope("/admin").configure(move |cfg| {
                    accounts.configure(cfg, "accounts");
                    credits.configure(cfg, "credits");
                    processed_debits.configure(cfg, "processed-debits");
                })),
        );
    }
}
