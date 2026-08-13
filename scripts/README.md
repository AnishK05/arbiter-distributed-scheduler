# Scripts

Python / shell tooling (not core services — see `IMPLEMENTATION_PLAN.md` Section 3):

| Script | Phase | Purpose |
|---|---|---|
| `verify_demo.sh` | 10 | Smoke-check after `make demo-up`: healthz, ≥10 ready nodes, optional 3-replica submit |
| `measure_failover.py` | 2 | Kill→`dead` timing (preliminary); see `docs/benchmarks/phase2-failure-detection.md` |
| `measure_node_failover.py` | 10 | Kill→dead→reassignment p50/p95 (≥20 trials for the sub-3s claim) |
| `measure_leader_failover.py` | 6 / 10 | Kill leader; assert new lease within one TTL; optional probe submit |
| `load_test.py` | 4 / 10 | Burst submit + placement histogram; `--wait-complete` samples peak concurrent `running` |
| `chaos_monkey.py` | 5 | `docker kill` / `pause` / `unpause` while a job runs; assert all-succeeded |
| `workloads/` | 3 | `sleep_n.py`, `cpu_burn.py`, `fail_n.py` baked into `arbiter-workload:latest` |

## Common invocations

```bash
# After make demo-up && make build
bash scripts/verify_demo.sh

# Placement comparison (Phase 4)
python3 scripts/load_test.py --replicas 100 --cpu-millicores 50 --policy bin_pack
python3 scripts/load_test.py --replicas 100 --cpu-millicores 50 --policy spread

# Section 10 concurrency (500+ peak — see docs/local-setup.md)
docker pull busybox:1.36
python3 scripts/load_test.py --tasks 750 --cpu-millicores 30 --memory-mb 16 \
  --image busybox:1.36 --command sleep --command 55 \
  --wait-complete --policy bin_pack

# Node / leader failover
python3 scripts/measure_node_failover.py --trials 20 --threshold-ms 3000
python3 scripts/measure_leader_failover.py --trials 5 --threshold-ms 5000 --submit-after
```

Evidence for measured runs lives under [`docs/benchmarks/`](../docs/benchmarks/).
Full Windows/WSL2 walkthrough: [`docs/local-setup.md`](../docs/local-setup.md).
