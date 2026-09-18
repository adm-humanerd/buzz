//! Relay-backed regression coverage for workflow deletion.
//!
//! # Running
//!
//! Start an isolated relay, then run:
//!
//! ```text
//! RELAY_URL=ws://localhost:3000 DATABASE_URL=postgres://… cargo test \
//!   -p buzz-test-client --test e2e_workflow_deletion -- --ignored
//! ```

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use buzz_test_client::BuzzTestClient;
use futures_util::FutureExt;
use nostr::{Alphabet, Event, EventBuilder, Filter, Keys, Kind, SingleLetterTag, Tag};
use sqlx::PgPool;
use uuid::Uuid;

const KIND_WORKFLOW_DEFINITION: u16 = 30_620;
const KIND_WORKFLOW_TRIGGER: u16 = 46_020;

fn relay_url() -> String {
    std::env::var("RELAY_URL").unwrap_or_else(|_| "ws://localhost:3000".to_string())
}

fn database_url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL required for failure injection")
}

fn sub_id(phase: &str) -> String {
    format!("e2e-workflow-deletion-{phase}-{}", Uuid::new_v4())
}

fn definition_filter(keys: &Keys, workflow_id: &str) -> Filter {
    Filter::new()
        .kind(Kind::Custom(KIND_WORKFLOW_DEFINITION))
        .author(keys.public_key())
        .custom_tags(SingleLetterTag::lowercase(Alphabet::D), [workflow_id])
}

async fn query_definition(
    client: &mut BuzzTestClient,
    keys: &Keys,
    workflow_id: &str,
) -> Vec<Event> {
    let subscription = sub_id("definition-query");
    client
        .subscribe(&subscription, vec![definition_filter(keys, workflow_id)])
        .await
        .expect("subscribe for fresh workflow definition query");
    client
        .collect_until_eose(&subscription, Duration::from_secs(5))
        .await
        .expect("query workflow definition")
}

async fn database_representation_counts(
    pool: &PgPool,
    keys: &Keys,
    workflow_id: Uuid,
) -> (i64, i64) {
    let owner = keys.public_key().to_bytes().to_vec();
    let d_tag = workflow_id.to_string();
    let workflow_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM workflows WHERE id = $1 AND owner_pubkey = $2")
            .bind(workflow_id)
            .bind(&owner)
            .fetch_one(pool)
            .await
            .expect("count executable workflow representation");
    let definition_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE kind = $1 AND pubkey = $2 AND d_tag = $3 \
         AND deleted_at IS NULL",
    )
    .bind(i32::from(KIND_WORKFLOW_DEFINITION))
    .bind(&owner)
    .bind(d_tag)
    .fetch_one(pool)
    .await
    .expect("count visible workflow definition representation");
    (workflow_rows, definition_rows)
}

struct FailureInjector {
    pool: PgPool,
}

impl FailureInjector {
    async fn install(pool: PgPool, keys: &Keys, workflow_id: Uuid) -> Self {
        // This fixture runs only against a dedicated disposable database. The
        // shared trigger is inert unless its private table contains this test's
        // exact owner + d-tag coordinate.
        sqlx::raw_sql(
            "CREATE TABLE workflow_delete_failure_injection (\
                 owner_pubkey BYTEA NOT NULL, d_tag TEXT NOT NULL, \
                 PRIMARY KEY (owner_pubkey, d_tag)\
             ); \
             CREATE FUNCTION reject_injected_workflow_delete() RETURNS trigger \
             LANGUAGE plpgsql AS $$ \
             BEGIN \
               IF OLD.kind = 30620 AND OLD.deleted_at IS NULL \
                  AND NEW.deleted_at IS NOT NULL \
                  AND EXISTS (SELECT 1 FROM workflow_delete_failure_injection \
                              WHERE owner_pubkey = OLD.pubkey AND d_tag = OLD.d_tag) \
               THEN RAISE EXCEPTION 'injected workflow deletion failure'; \
               END IF; \
               RETURN NEW; \
             END $$; \
             CREATE TRIGGER reject_injected_workflow_delete \
             BEFORE UPDATE ON events FOR EACH ROW \
             EXECUTE FUNCTION reject_injected_workflow_delete();",
        )
        .execute(&pool)
        .await
        .expect("install coordinate-scoped deletion failure injector");
        sqlx::query(
            "INSERT INTO workflow_delete_failure_injection (owner_pubkey, d_tag) VALUES ($1, $2)",
        )
        .bind(keys.public_key().to_bytes().to_vec())
        .bind(workflow_id.to_string())
        .execute(&pool)
        .await
        .expect("arm deletion failure injector for test coordinate");
        Self { pool }
    }

    async fn remove(&mut self) {
        sqlx::raw_sql(
            "DROP TRIGGER IF EXISTS reject_injected_workflow_delete ON events; \
             DROP FUNCTION IF EXISTS reject_injected_workflow_delete(); \
             DROP TABLE IF EXISTS workflow_delete_failure_injection;",
        )
        .execute(&self.pool)
        .await
        .expect("remove deletion failure injector");
    }
}

async fn create_channel(client: &mut BuzzTestClient, keys: &Keys, channel_id: &str) {
    let create_channel = EventBuilder::new(Kind::Custom(9007), "")
        .tags([
            Tag::parse(["h", channel_id]).expect("channel h tag"),
            Tag::parse(["name", "workflow-deletion-e2e"]).expect("channel name tag"),
            Tag::parse(["channel_type", "stream"]).expect("channel type tag"),
            Tag::parse(["visibility", "open"]).expect("channel visibility tag"),
        ])
        .sign_with_keys(keys)
        .expect("sign channel creation");
    let created = client
        .send_event(create_channel)
        .await
        .expect("create channel");
    assert!(
        created.accepted,
        "channel creation rejected: {}",
        created.message
    );
}

async fn create_workflow(
    client: &mut BuzzTestClient,
    keys: &Keys,
    channel_id: &str,
    workflow_id: &str,
) -> nostr::EventId {
    let definition = EventBuilder::new(
        Kind::Custom(KIND_WORKFLOW_DEFINITION),
        "name: deletion-regression\ntrigger:\n  on: message_posted\nsteps:\n  - id: pause\n    action: delay\n    duration: 1s\n",
    )
    .tags([
        Tag::parse(["d", workflow_id]).expect("workflow d tag"),
        Tag::parse(["h", channel_id]).expect("workflow h tag"),
    ])
    .sign_with_keys(keys)
    .expect("sign workflow definition");
    let definition_id = definition.id;
    let created = client
        .send_event(definition)
        .await
        .expect("create workflow");
    assert!(
        created.accepted,
        "workflow creation rejected: {}",
        created.message
    );
    definition_id
}

fn workflow_deletion(keys: &Keys, workflow_id: &str) -> Event {
    let coordinate = format!(
        "{KIND_WORKFLOW_DEFINITION}:{}:{workflow_id}",
        keys.public_key().to_hex()
    );
    EventBuilder::new(Kind::EventDeletion, "")
        .tags([Tag::parse(["a", coordinate.as_str()]).expect("workflow coordinate")])
        .sign_with_keys(keys)
        .expect("sign workflow deletion")
}

#[tokio::test]
#[ignore]
async fn deleting_workflow_removes_definition_and_rejects_manual_trigger() {
    let keys = Keys::generate();
    let channel_id = Uuid::new_v4().to_string();
    let workflow_id = Uuid::new_v4().to_string();
    let mut client = BuzzTestClient::connect(&relay_url(), &keys)
        .await
        .expect("connect");

    create_channel(&mut client, &keys, &channel_id).await;
    let definition_id = create_workflow(&mut client, &keys, &channel_id, &workflow_id).await;

    let before = query_definition(&mut client, &keys, &workflow_id).await;
    assert_eq!(
        before.len(),
        1,
        "definition must be queryable before deletion"
    );
    assert_eq!(before[0].id, definition_id);

    let deletion = workflow_deletion(&keys, &workflow_id);
    let deleted = client.send_event(deletion).await.expect("delete workflow");
    assert!(
        deleted.accepted,
        "workflow deletion rejected: {}",
        deleted.message
    );

    let after = query_definition(&mut client, &keys, &workflow_id).await;
    assert!(
        after.is_empty(),
        "deleted workflow definition remained queryable: {after:?}"
    );

    let trigger = EventBuilder::new(Kind::Custom(KIND_WORKFLOW_TRIGGER), "{}")
        .tags([Tag::parse(["d", workflow_id.as_str()]).expect("trigger d tag")])
        .sign_with_keys(&keys)
        .expect("sign workflow trigger");
    let triggered = client
        .send_event(trigger)
        .await
        .expect("submit post-deletion trigger");
    assert!(!triggered.accepted, "deleted workflow remained triggerable");
    assert!(
        triggered.message.contains("workflow not found"),
        "unexpected post-deletion trigger rejection: {}",
        triggered.message
    );

    client.disconnect().await.expect("disconnect");
}

async fn run_failed_deletion_replay_scenario(pool: &PgPool) {
    let keys = Keys::generate();
    let channel_id = Uuid::new_v4().to_string();
    let workflow_id = Uuid::new_v4();
    let workflow_id_text = workflow_id.to_string();
    let mut client = BuzzTestClient::connect(&relay_url(), &keys)
        .await
        .expect("connect");

    create_channel(&mut client, &keys, &channel_id).await;
    create_workflow(&mut client, &keys, &channel_id, &workflow_id_text).await;
    assert_eq!(
        database_representation_counts(pool, &keys, workflow_id).await,
        (1, 1),
        "workflow setup must create both representations"
    );

    let deletion = workflow_deletion(&keys, &workflow_id_text);
    let deletion_id = deletion.id;
    let mut injector = FailureInjector::install(pool.clone(), &keys, workflow_id).await;
    let rejected = client
        .send_event(deletion.clone())
        .await
        .expect("submit injected-failure deletion");
    assert!(
        !rejected.accepted,
        "injected deletion failure was acknowledged"
    );
    // WebSocket ingestion deliberately redacts internal database errors.
    assert_eq!(rejected.message, "error: internal server error");

    assert_eq!(
        database_representation_counts(pool, &keys, workflow_id).await,
        (1, 1),
        "failed deletion must retain both workflow representations"
    );
    let after_failure = query_definition(&mut client, &keys, &workflow_id_text).await;
    assert_eq!(
        after_failure.len(),
        1,
        "definition disappeared after rejected deletion"
    );

    injector.remove().await;
    let replayed = client
        .send_event(deletion)
        .await
        .expect("replay identical signed deletion");
    assert!(
        replayed.accepted,
        "identical deletion replay rejected: {}",
        replayed.message
    );
    assert_eq!(replayed.event_id, deletion_id.to_hex());
    assert!(
        replayed.message.starts_with("duplicate:"),
        "replay did not exercise duplicate-ingest path: {}",
        replayed.message
    );
    assert_eq!(
        database_representation_counts(pool, &keys, workflow_id).await,
        (0, 0),
        "successful replay must remove both workflow representations"
    );
    assert!(
        query_definition(&mut client, &keys, &workflow_id_text)
            .await
            .is_empty(),
        "definition remained queryable after successful replay"
    );
    client.disconnect().await.expect("disconnect");
}

#[tokio::test]
#[ignore]
async fn failed_workflow_deletion_rolls_back_and_identical_event_can_be_replayed() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to isolated workflow-deletion database");
    let result = AssertUnwindSafe(run_failed_deletion_replay_scenario(&pool))
        .catch_unwind()
        .await;

    // Clean up even when the scenario panics so a failed run cannot poison the
    // disposable relay for subsequent validation.
    sqlx::raw_sql(
        "DROP TRIGGER IF EXISTS reject_injected_workflow_delete ON events; \
         DROP FUNCTION IF EXISTS reject_injected_workflow_delete(); \
         DROP TABLE IF EXISTS workflow_delete_failure_injection;",
    )
    .execute(&pool)
    .await
    .expect("final deletion failure injector cleanup");

    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
