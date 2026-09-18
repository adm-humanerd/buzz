use buzz_auth::{LimitType, RateLimiter};
use buzz_core::TenantContext;
use nostr::PublicKey;
use tokio::sync::mpsc::error::TrySendError;

// Desktop startup establishes several independent live subscriptions at once.
// Preserve the configured average rate while allowing that bounded burst. This
// is still a fixed-window limiter, so a Redis-backed token bucket would be a
// better long-term fit for smoother refill behavior.
const WS_BURST_WINDOW_SECS: u64 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdmissionError {
    Exceeded {
        current: u64,
        limit: u64,
        reset_in_secs: u64,
    },
    Unavailable,
}

pub(crate) async fn check_principal<L: RateLimiter>(
    limiter: &L,
    tenant: &TenantContext,
    pubkey: &PublicKey,
    limit_type: LimitType,
    window_secs: u64,
    limit: u64,
) -> Result<(), AdmissionError> {
    match limiter
        .check_and_increment(tenant, pubkey, limit_type, window_secs, limit)
        .await
    {
        Ok(result) if result.allowed => Ok(()),
        Ok(result) => Err(AdmissionError::Exceeded {
            current: result.current,
            limit: result.limit,
            reset_in_secs: result.reset_in_secs,
        }),
        Err(error) => {
            tracing::warn!(error = %error, "shared rate-limit admission unavailable");
            Err(AdmissionError::Unavailable)
        }
    }
}

/// Static, bounded details for a quota-rejection audit entry.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RateLimitAudit {
    /// Route label selected by the server.
    pub(crate) route: &'static str,
    /// Transport label selected by the server.
    pub(crate) transport: &'static str,
    /// Counter value returned by the limiter.
    pub(crate) current: u64,
    /// Configured quota.
    pub(crate) limit: u64,
    /// Seconds until the counter resets.
    pub(crate) reset_in_secs: u64,
}

/// Enqueue a bounded, sanitized audit record for a quota rejection.
///
/// Rate-limit handling is on the request hot path, so this deliberately uses
/// `try_send`: audit backpressure must never turn into request backpressure.
/// Callers provide only static route and transport labels; no request body,
/// token, subscription id, or other client-controlled text enters the entry.
pub(crate) fn audit_rate_limit_exceeded(
    state: &crate::state::AppState,
    tenant: &TenantContext,
    pubkey: &PublicKey,
    audit: RateLimitAudit,
) {
    let Some(audit_tx) = &state.audit_tx else {
        return;
    };

    enqueue_rate_limit_audit(audit_tx, tenant, pubkey, audit);
}

fn enqueue_rate_limit_audit(
    audit_tx: &tokio::sync::mpsc::Sender<buzz_audit::NewAuditEntry>,
    tenant: &TenantContext,
    pubkey: &PublicKey,
    audit: RateLimitAudit,
) {
    let entry = buzz_audit::NewAuditEntry {
        community_id: tenant.community(),
        action: buzz_audit::AuditAction::RateLimitExceeded,
        actor_pubkey: Some(pubkey.to_bytes().to_vec()),
        object_id: None,
        detail: serde_json::json!({
            "route": audit.route,
            "current": audit.current,
            "limit": audit.limit,
            "reset_in_secs": audit.reset_in_secs,
            "transport": audit.transport,
        }),
    };

    match audit_tx.try_send(entry) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            metrics::counter!("buzz_audit_rate_limit_drops_total", "reason" => "full").increment(1);
            tracing::warn!(
                route = audit.route,
                transport = audit.transport,
                "rate-limit audit queue full; entry dropped"
            );
        }
        Err(TrySendError::Closed(_)) => {
            metrics::counter!("buzz_audit_send_errors_total").increment(1);
            tracing::warn!(
                route = audit.route,
                transport = audit.transport,
                "rate-limit audit channel closed"
            );
        }
    }
}

pub(crate) fn ws_admission_budget(per_second_limit: u64) -> (u64, u64) {
    (
        WS_BURST_WINDOW_SECS,
        per_second_limit.saturating_mul(WS_BURST_WINDOW_SECS),
    )
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use buzz_auth::{AuthError, RateLimitResult, RateLimiter};
    use buzz_core::CommunityId;
    use nostr::Keys;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use super::*;

    enum StubOutcome {
        Denied,
        Failed,
    }

    struct StubLimiter {
        outcome: StubOutcome,
        calls: AtomicUsize,
    }

    impl RateLimiter for StubLimiter {
        async fn check_and_increment(
            &self,
            _ctx: &TenantContext,
            _pubkey: &PublicKey,
            _limit_type: LimitType,
            _window_secs: u64,
            _limit: u64,
        ) -> Result<RateLimitResult, AuthError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            match self.outcome {
                StubOutcome::Denied => Ok(RateLimitResult::denied(11, 10, 1)),
                StubOutcome::Failed => Err(AuthError::Internal("redis unavailable".to_owned())),
            }
        }

        async fn check_ip_connection(
            &self,
            _ip: &IpAddr,
            _window_secs: u64,
            _limit: u64,
        ) -> Result<RateLimitResult, AuthError> {
            match self.outcome {
                StubOutcome::Denied => Ok(RateLimitResult::denied(11, 10, 1)),
                StubOutcome::Failed => Err(AuthError::Internal("redis unavailable".to_owned())),
            }
        }
    }

    fn tenant() -> TenantContext {
        TenantContext::resolved(
            CommunityId::from_uuid(Uuid::from_u128(1)),
            "relay.example.com",
        )
    }

    #[test]
    fn websocket_budget_preserves_rate_with_a_bounded_burst() {
        assert_eq!(ws_admission_budget(10), (5, 50));
    }

    #[test]
    fn websocket_budget_saturates_on_overflow() {
        assert_eq!(ws_admission_budget(u64::MAX), (5, u64::MAX));
    }

    #[tokio::test]
    async fn denied_shared_counter_rejects_admission() {
        let limiter = StubLimiter {
            outcome: StubOutcome::Denied,
            calls: AtomicUsize::new(0),
        };
        let keys = Keys::generate();

        let result = check_principal(
            &limiter,
            &tenant(),
            &keys.public_key(),
            LimitType::WsEvents,
            1,
            10,
        )
        .await;

        assert_eq!(
            result,
            Err(AdmissionError::Exceeded {
                current: 11,
                limit: 10,
                reset_in_secs: 1,
            })
        );
        assert_eq!(limiter.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn shared_counter_failure_rejects_admission() {
        let limiter = StubLimiter {
            outcome: StubOutcome::Failed,
            calls: AtomicUsize::new(0),
        };
        let keys = Keys::generate();

        let result = check_principal(
            &limiter,
            &tenant(),
            &keys.public_key(),
            LimitType::ApiCalls,
            60,
            300,
        )
        .await;

        assert_eq!(result, Err(AdmissionError::Unavailable));
        assert_eq!(limiter.calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn rate_limit_audit_enqueue_is_bounded_and_non_blocking() {
        let (tx, mut rx) = mpsc::channel(1);
        let keys = Keys::generate();
        let tenant = tenant();

        let audit = RateLimitAudit {
            route: "/query",
            transport: "http",
            current: 11,
            limit: 10,
            reset_in_secs: 7,
        };
        enqueue_rate_limit_audit(&tx, &tenant, &keys.public_key(), audit);
        enqueue_rate_limit_audit(
            &tx,
            &tenant,
            &keys.public_key(),
            RateLimitAudit {
                current: 12,
                ..audit
            },
        );

        let entry = rx.try_recv().expect("first audit entry is queued");
        assert_eq!(entry.action, buzz_audit::AuditAction::RateLimitExceeded);
        assert_eq!(entry.detail["route"], "/query");
        assert_eq!(entry.detail["current"], 11);
        assert_eq!(entry.detail["limit"], 10);
        assert_eq!(entry.detail["reset_in_secs"], 7);
        assert_eq!(entry.detail["transport"], "http");
        assert!(rx.try_recv().is_err(), "full audit queue must not grow");
    }
}
