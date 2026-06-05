use axum::{
    extract::FromRequestParts,
    http::{StatusCode, request::Parts},
};
use uuid::Uuid;

pub const USER_ID_HEADER: &str = "x-gridtokenx-user-id";

#[derive(Debug, Clone, Copy)]
pub struct UserContext {
    pub user_id: Uuid,
}

impl<S> FromRequestParts<S> for UserContext
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if let Some(user_id_str) = parts
            .headers
            .get(USER_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            && let Ok(user_id) = Uuid::parse_str(user_id_str)
        {
            return Ok(UserContext { user_id });
        }

        Err((
            StatusCode::UNAUTHORIZED,
            "Missing or invalid User ID header".to_string(),
        ))
    }
}
