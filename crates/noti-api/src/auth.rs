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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    /// Build request `Parts` carrying the given optional user-id header value.
    fn parts_with_header(value: Option<&str>) -> Parts {
        let mut builder = Request::builder();
        if let Some(v) = value {
            builder = builder.header(USER_ID_HEADER, v);
        }
        builder.body(()).expect("build request").into_parts().0
    }

    #[tokio::test]
    async fn extracts_valid_user_id() {
        let id = Uuid::new_v4();
        let mut parts = parts_with_header(Some(&id.to_string()));

        let ctx = UserContext::from_request_parts(&mut parts, &())
            .await
            .expect("valid header should extract");
        assert_eq!(ctx.user_id, id);
    }

    #[tokio::test]
    async fn rejects_missing_header() {
        let mut parts = parts_with_header(None);

        let result = UserContext::from_request_parts(&mut parts, &()).await;
        let (status, _) = result.expect_err("missing header must reject");
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rejects_malformed_uuid() {
        let mut parts = parts_with_header(Some("not-a-uuid"));

        let result = UserContext::from_request_parts(&mut parts, &()).await;
        let (status, _) = result.expect_err("malformed uuid must reject");
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
}
