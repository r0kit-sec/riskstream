# Threat-Signal-Normalize Pre-Merge Plan

## Objective

Review and validate the `threat-signal-normalizer` branch before merge to `main`, using the canonical roadmap at `local_docs/ideas/canonicalized-roadmap.md` as the planning reference.

## Scope

- Whole branch diff versus `origin/main`
- Threat-signal normalizer service and schema contract
- URLhaus ingestion changes that feed normalization
- Kubernetes manifests, integration script, and CI coverage

## Non-Goals

- No production deployment
- No architecture expansion beyond the current Phase 1 data-foundation scope
- No unrelated refactors

## Acceptance Criteria

- Reviewer confirms Phase 1 alignment and no unresolved blocking issues in security, resilience, corruption risk, cost/resource creep, or observability.
- Threat-signal normalizer unit tests pass in the service image.
- URLhaus ingestion unit tests pass in the service image.
- Threat-signal normalization integration validation passes in `local-dev`, or the tester records a concrete environment blocker with exact command and impact.

## Expected Test Outcomes

- `riskstream/tests/unit/test_threat_signal_normalizer.py` passes in the normalizer image.
- `riskstream/tests/unit/test_urlhaus_ingestion.py` passes in the URLhaus image.
- `./scripts/run-threat-signal-normalization-integration-test.sh` completes successfully and the seeded raw artifacts produce the expected normalized outputs.

## Design Suggestions

- Keep the normalizer resilient to short-lived object-store visibility races when it is triggered immediately after raw artifact writes.
- Preserve the current `threat_signal.v1` contract and object-key layout as the stable Phase 1 interface for downstream ranking work.
