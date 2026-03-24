# Review Findings

## Resolved Blocking Finding

1. The integration workflow had a concrete resilience gap: the normalizer read path failed hard on transient `NoSuchKey` immediately after a raw artifact was written, even though the test client could already observe the object. This blocked the in-cluster test job and made the batch path too brittle for just-written artifacts.
2. The `local-dev` `minio` Service routed to both `local-minio` and `minio` pods in the same namespace. That created split object-store backends behind one service name, so the integration test could write through one MinIO pod and the subprocess could read through the other and receive `NoSuchKey`.

## What Looks Good

- The branch is aligned with the roadmap's Phase 1 goal of introducing a common normalized threat-signal contract without adding major new platform components.
- The URLhaus parsing change preserves commented CSV headers, which matches the observed feed shape and protects downstream normalization.
- The unit test surface covers schema validity, key layout, delta action mapping, and normalized write behavior.

## Remaining Concerns

- No unresolved merge-blocking findings remain from this review/test pass.
- A follow-up cleanup to remove the stale `local-minio` deployment from `local-dev` would reduce future confusion, but it is not required for this feature merge.

## Reviewer Checklist Summary

- Security and OWASP Top 10: no obvious new application-layer findings surfaced in the reviewed paths.
- Data corruption: the local-dev service selector fix removes a split-backend condition that could have made reads appear inconsistent across clients.
- Crash and undefined behavior: the resolved raw-read retry and single-backend MinIO routing remove the observed failure mode from the tested path.
- Logging and observability: existing JSON error logging on normalization failure is adequate for the current batch path.
- Cost and resource creep: no new obvious storage, memory, or compute regressions surfaced in the reviewed code change itself; scheduled normalization frequency should still be monitored in cluster.
