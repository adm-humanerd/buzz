-- Durable execution ownership for workflow runs.
--
-- Existing rows are intentionally not backfilled. A legacy running row may
-- only be reclaimed by the approval-recovery query when it has a matching
-- approval intent/continuation and a NULL lease. A normal running row remains
-- untouched until an operator resolves it.
ALTER TABLE workflow_runs
    ADD COLUMN execution_claim_token UUID,
    ADD COLUMN execution_lease_until TIMESTAMPTZ;

CREATE INDEX idx_workflow_runs_execution_lease
    ON workflow_runs (community_id, status, execution_lease_until)
    WHERE status = 'running';
