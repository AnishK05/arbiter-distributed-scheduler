# Scripts

Host tooling (not core services — see `IMPLEMENTATION_PLAN.md` Section 3):

| Script | Phase | Purpose |
|---|---|---|
| `arbiter.ps1` | 10 | **Windows PowerShell** Make alternative (`demo-up`, `build`, `demo-verify`, phase stacks, `test`, …) |
| `verify_demo.ps1` | 10 | PowerShell smoke-check after demo-up |
| `verify_demo.sh` | 10 | bash smoke-check (`make demo-verify`) |
| `arbiterctl_path.py` | — | Resolves `./bin/arbiterctl` vs `arbiterctl.exe` for Python tools |
| `measure_failover.py` | 2 | Kill→`dead` timing (preliminary) |
| `measure_node_failover.py` | 10 | Kill→dead→reassignment p50/p95 |
| `measure_leader_failover.py` | 6 / 10 | Kill leader; assert new lease within one TTL |
| `load_test.py` | 4 / 10 | Burst submit + placement / peak concurrency |
| `chaos_monkey.py` | 5 | `docker kill` / `pause` / `unpause` while a job runs |
| `workloads/` | 3 | Baked into `arbiter-workload:latest` |

## PowerShell (Windows)

```powershell
.\scripts\arbiter.ps1 demo-up
.\scripts\arbiter.ps1 build
.\scripts\arbiter.ps1 demo-verify

python scripts\load_test.py --tasks 750 --cpu-millicores 30 --memory-mb 16 `
  --image busybox:1.36 --command sleep --command 55 --wait-complete --policy bin_pack

python scripts\measure_node_failover.py --trials 20 --threshold-ms 3000
python scripts\measure_leader_failover.py --trials 5 --threshold-ms 5000 --submit-after
```

## bash / WSL / Linux / macOS

```bash
make demo-up && make build && make demo-verify

python3 scripts/load_test.py --tasks 750 --cpu-millicores 30 --memory-mb 16 \
  --image busybox:1.36 --command sleep --command 55 \
  --wait-complete --policy bin_pack

python3 scripts/measure_node_failover.py --trials 20 --threshold-ms 3000
python3 scripts/measure_leader_failover.py --trials 5 --threshold-ms 5000 --submit-after
```

Evidence: [`docs/benchmarks/`](../docs/benchmarks/). Full local guide:
[`docs/local-setup.md`](../docs/local-setup.md).
