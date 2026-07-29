# Phase 6 — Leader Election Failover

Evidence for the Phase 6 DoD (`IMPLEMENTATION_PLAN.md` Section 8): kill the
leader container; a follower acquires the Postgres lease and continues
scheduling within one lease-TTL window; newly submitted jobs still succeed.

## Cluster

| Component | Config |
|---|---|
| Stack | `make phase6-up` (3 scheduler replicas + 1 worker) |
| Lease | TTL 5s, renew every 1s |
| Advertise | `host.docker.internal:7000/7001/7002` (host-gateway) |
| Workers | rotate across all three advertise addrs on `Unavailable` |

## Method

```bash
make phase6-up && make build
python3 scripts/measure_leader_failover.py --trials 5 --submit-after
```

Each trial:

1. Reads the live `leader_lease` row.
2. `docker kill`s that leader's container.
3. Polls until another replica holds a non-expired lease with a bumped epoch.
4. Restarts the killed replica.
5. Submits a 1-replica probe job to the new leader's host-mapped port and waits
   for success.

## Results

```
trial 1: 3967.0ms  (scheduler-2 → scheduler-3)
trial 2: 4249.4ms  (scheduler-3 → scheduler-1)
trial 3: 4990.3ms  (scheduler-1 → scheduler-2)
trial 4: 4245.9ms  (scheduler-2 → scheduler-3)
trial 5: 4386.4ms  (scheduler-3 → scheduler-1)

ok: max=4990.3ms, threshold=6000ms (TTL + renew)
```

All five probe jobs reached `succeeded` after failover. Election times sit
under one lease TTL plus one renew interval (the follower only notices expiry
on its renew tick).

## Notes

- Followers reject mutating RPCs with `NOT_LEADER: current leader at <addr>`;
  workers and `arbiterctl` follow that redirect (see `docs/design-decisions.md`
  Phase 6).
- Reads remain available on any replica; the measurement script dials the new
  leader directly after election so host-side submits do not depend on a
  killed seed port.
