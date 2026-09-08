-- Dispatch accounting: separate "a new attempt started" from "the run came back
-- to the lane without having consumed one".
--
-- run_count is incremented by the dispatcher on every lease, and the engine
-- reads it as the attempt number when it decides whether a failure still has
-- retry budget left. That conflates two different events. A run that suspends —
-- a durable sleep, or a RunDeploymentAndWait parent waiting on its children —
-- returns to the lane and is leased again, so a flow that waits ten times has
-- burned ten attempts before doing any work the operator would call an attempt.
-- The same happens to a run that lands on a worker where its flow is not
-- registered and is handed straight back.
--
-- resume_requested marks those two cases. The dispatcher skips the increment
-- when it is set and clears it in the same statement, so the flag can never
-- outlive the lease it was written for. Existing rows default to false, which
-- is exactly how they behave today.
ALTER TABLE pf_flow_runs
    ADD COLUMN IF NOT EXISTS resume_requested BOOLEAN NOT NULL DEFAULT false;
