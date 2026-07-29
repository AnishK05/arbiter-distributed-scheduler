# Phase 4 — Bin-Pack vs Spread Placement Distribution

Comparison produced with `scripts/load_test.py` against the 5-worker Phase 4
overlay (`make phase4-up`). Validates that `scheduling_policy` actually changes
placement (IMPLEMENTATION_PLAN.md Phase 4 DoD).

## Cluster

| Component | Config |
|---|---|
| Workers | 5 (`worker-1` … `worker-5`) |
| Capacity / node | 2000 millicores CPU, 1024 MB memory |
| Task request | 50 millicores, 32 MB |
| Replicas | 100 (held running via `--command 60`) |
| Binding constraint | Memory caps a node at `1024/32 = 32` concurrent tasks |

Total demand = 100 × 50m = 5000m CPU and 3200 MB — well under the cluster's
10 000m / 5120 MB, so both policies have room to choose *where* to pack.

## Method

```bash
make phase4-up
make build

python3 scripts/load_test.py --replicas 100 --cpu-millicores 50 --memory-mb 32 \
  --policy bin_pack --command 60 --name load-binpack

python3 scripts/load_test.py --replicas 100 --cpu-millicores 50 --memory-mb 32 \
  --policy spread --command 60 --name load-spread
```

The script waits for the cluster to be idle (no `scheduled`/`running` tasks),
submits one job, waits until every task has an `assigned_node_id`, then prints
the hostname histogram. `--command 60` keeps containers alive long enough that
assignments reflect residual-capacity decisions rather than churn from early
completions.

## Results

### `bin_pack` (default)

```
policy=bin_pack tasks=100
placement by hostname:
  worker-1       32  ################################
  worker-2       32  ################################
  worker-3       32  ################################
  worker-4        4  ####
nodes_used=4 min=4 max=32 mean=25.00 pstdev=12.12 concentration(max/mean)=1.280
```

Bin-pack fills each node to its binding residual (here: memory → 32 tasks)
before spilling to the next. `worker-5` stays empty — load is concentrated onto
fewer near-full nodes.

### `spread`

```
policy=spread tasks=100
placement by hostname:
  worker-1       20  ####################
  worker-2       20  ####################
  worker-3       20  ####################
  worker-4       20  ####################
  worker-5       20  ####################
nodes_used=5 min=20 max=20 mean=20.00 pstdev=0.00 concentration(max/mean)=1.000
```

Spread always prefers the currently least-utilized ready node, so the same 100
tasks land evenly (20 apiece) across all five workers.

## Takeaway

| Policy | Nodes used | pstdev | concentration (max/mean) |
|---|---|---|---|
| `bin_pack` | 4 | 12.12 | 1.280 |
| `spread` | 5 | 0.00 | 1.000 |

Same cluster, same task shape — only `scheduling_policy` changed. This is the
measured evidence behind the resume claim of **bin-packing allocation** (and
that the alternative policy is genuinely pluggable).
