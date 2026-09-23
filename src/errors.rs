// src/error.rs
use actix_web::{HttpResponse, ResponseError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("DB error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("Payment failed: {0}")]
    Payment(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ApiError::Db(_) => HttpResponse::InternalServerError().json("db error"),
            ApiError::Payment(msg) => HttpResponse::BadRequest().json(msg),
            ApiError::Other(_) => HttpResponse::InternalServerError().json("internal error"),
        }
    }
}

pub type Result<T> = std::result::Result<T, ApiError>;
