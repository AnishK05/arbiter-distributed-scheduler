# Local Setup & Run Guide

End-to-end instructions for running Arbiter on a **Windows + Docker Desktop + WSL2**
machine (the supported development path), with notes for native Linux/macOS.

This is the wrap-up guide for a finished Phase 0–10 tree: after these steps you should
have a live 10-node demo cluster, a working CLI, and the dashboard/Grafana/Prometheus
surfaces open in a browser.

> **Not supported:** running `make` / Compose / workers from native Windows PowerShell
> or `cmd.exe`. Always use a WSL2 shell (Ubuntu recommended). See
> [`IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md) Section 4 for why.

---

## 1. One-time Windows prerequisites

### 1.1 Docker Desktop + WSL2

1. Install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/)
   with the **WSL2 backend** (default in current versions).
2. If you do not already have a distro: open PowerShell as Administrator and run
   `wsl --install` (Ubuntu is fine), then reboot if prompted.
3. In Docker Desktop → **Settings → Resources → WSL Integration**, enable integration
   for your distro. Confirm Docker is running (whale icon).
4. Recommended Desktop resources for the full demo (Settings → Resources):
   - **CPU:** ≥ 4 cores
   - **Memory:** ≥ 8 GB (12+ GB if you plan the 750-task concurrency run)
   - **Disk image size:** ≥ 64 GB free in the Docker virtual disk

### 1.2 Tools inside WSL2 (Ubuntu)

Open an Ubuntu WSL terminal and install:

```bash
sudo apt update
sudo apt install -y build-essential make git curl python3 ca-certificates

# Go 1.22+ (repo pin is 1.22.2; GOTOOLCHAIN=local avoids auto-download surprises)
curl -fsSL https://go.dev/dl/go1.22.2.linux-amd64.tar.gz -o /tmp/go.tgz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf /tmp/go.tgz
echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' >> ~/.bashrc
source ~/.bashrc
go version   # expect go1.22.2
```

Verify Docker is reachable **from inside WSL**:

```bash
docker version
docker compose version
```

If `docker` is missing or permission-denied, re-check WSL Integration in Docker Desktop
and ensure your user can talk to the engine (`docker` group / Desktop “Use the WSL 2
based engine”).

### 1.3 Clone into the WSL filesystem

```bash
mkdir -p ~/dev && cd ~/dev
git clone https://github.com/AnishK05/arbiter-distributed-scheduler.git
cd arbiter-distributed-scheduler
```

Clone under `~/…` (the Linux filesystem), **not** `/mnt/c/...`. Bind mounts and file
watches are much slower on the Windows mount.

Optional: open the folder with VS Code / Cursor **Remote - WSL** so the editor and
terminal share the same Linux environment.

---

## 2. Bring up the full demo (recommended first run)

From the repo root in WSL:

```bash
make demo-up          # builds images + starts everything
make build            # local arbiterctl / scheduler / worker binaries → ./bin
```

Equivalent without Make:

```bash
docker compose -f deploy/docker-compose.demo.yml up -d --build
```

First boot pulls base images and compiles Go/Next.js containers — expect several
minutes. Subsequent `make demo-up` runs are much faster.

### What comes up

| Service | Count | Host URL / port |
|---|---|---|
| Scheduler (leader + 2 followers) | 3 | http://localhost:8080 · gRPC `:7000` (followers `:7001`/`:7002`, HTTP `:8086`/`:8087`) |
| Workers (varied capacity) | 10 | metrics on `:8081`–`:8090` |
| Postgres / Redis | 1 each | `:5432` / `:6379` |
| Prometheus | 1 | http://localhost:9090 |
| Grafana | 1 | http://localhost:3000 (admin/admin; anonymous Viewer) |
| Next.js dashboard | 1 | http://localhost:3100 |

Open the dashboard and Grafana in your **Windows browser** — Docker Desktop publishes
these ports to `localhost` on the Windows host.

### Smoke check

```bash
# or: bash scripts/verify_demo.sh
curl -sf http://localhost:8080/healthz && echo
curl -s http://localhost:8080/api/v1/nodes | python3 -c \
  "import sys,json; ns=json.load(sys.stdin)['nodes']; \
   print(f\"ready={sum(1 for n in ns if n['status']=='ready')}/{len(ns)}\")"

./bin/arbiterctl get nodes
./bin/arbiterctl submit hello --replicas 3 --wait
```

You should see **10 ready workers**, then three tasks succeed.

Tear down when finished:

```bash
make demo-down
```

---

## 3. Day-to-day commands

### Submit jobs

```bash
./bin/arbiterctl submit demo --replicas 5 --wait
./bin/arbiterctl submit burn --replicas 10 --cpu-millicores 100 --memory-mb 64 --command 30
./bin/arbiterctl describe task <task-id>
./bin/arbiterctl logs <task-id>
```

### Section 10-style concurrency (500+ peak)

Use a tiny image and memory requests that fit cluster memory (Σ ≈ 12032 MiB).
`--memory-mb 32` caps concurrent running tasks around **376**; use **16** for 500+:

```bash
docker pull busybox:1.36
python3 scripts/load_test.py --tasks 750 --cpu-millicores 30 --memory-mb 16 \
  --image busybox:1.36 --command sleep --command 55 \
  --wait-complete --policy bin_pack --name local-concurrency
```

### Failover demos

```bash
# Steady load, then kill workers (sub-3s detection target)
./bin/arbiterctl submit steady --replicas 100 --cpu-millicores 40 --memory-mb 16 \
  --image busybox:1.36 --command sleep --command 900 --scheduling-policy spread
python3 scripts/measure_node_failover.py --trials 5 --threshold-ms 3000

# Leader kill → new lease holder within one TTL (5s)
python3 scripts/measure_leader_failover.py --trials 3 --threshold-ms 5000 --submit-after
```

Archived numbers from the Phase 10 run live in
[`docs/benchmarks/phase10-resume-metrics.md`](benchmarks/phase10-resume-metrics.md).

### Smaller stacks (dev / phase work)

```bash
make up              # 1 scheduler + 1 worker
make phase6-up       # 3 schedulers (HA)
make phase8-up       # + Prometheus/Grafana/dashboard
make phase9-up       # + simulated autoscaler
make down            # stop the base stack (use matching *-down for overlays)
```

Edit/rebuild loop without rebuilding worker/scheduler images:

```bash
docker compose -f deploy/docker-compose.yml up -d postgres redis
make run-scheduler   # terminal 1
make run-worker      # terminal 2
```

---

## 4. Tests

```bash
make vet
make test            # unit tests; store/cache/failuredetector skip without DBs

# Full integration tests against Compose Postgres/Redis:
docker compose -f deploy/docker-compose.yml up -d postgres redis
ARBITER_TEST_POSTGRES_URL="postgres://arbiter:arbiter@localhost:5432/arbiter?sslmode=disable" \
ARBITER_TEST_REDIS_ADDR="localhost:6379" \
make test
```

Optional lint (installs via `make tools` if needed):

```bash
make tools && make lint
```

---

## 5. Troubleshooting (Windows / WSL2)

| Symptom | Likely cause | Fix |
|---|---|---|
| `Cannot connect to the Docker daemon` | Desktop not running or WSL integration off | Start Docker Desktop; enable WSL Integration for your distro |
| `permission denied` on `/var/run/docker.sock` | Socket group / Desktop quirk | Restart Desktop; ensure your user is in the `docker` group inside WSL (`sudo usermod -aG docker $USER`, then new shell) |
| Very slow builds / file watches | Repo under `/mnt/c/...` | Move/clone to `~/dev/...` on the Linux filesystem |
| Workers never reach `ready` | `host.docker.internal` / schedulers unhealthy | `docker compose -f deploy/docker-compose.demo.yml ps`; check `docker logs arbiter-scheduler-1` |
| Tasks stuck `scheduled`, no containers | Docker-out-of-Docker socket | Confirm workers mount `/var/run/docker.sock`; `docker logs arbiter-worker-1` |
| Disk fills during large bursts | Many task containers (worse on exotic storage drivers) | Prefer `busybox:1.36`; executor uses `AutoRemove`; run `make demo-down` and `docker system prune` between big runs |
| Port already in use (`5432`, `8080`, …) | Another stack or local Postgres | `make demo-down` / `make down`, or stop the conflicting service |
| CRLF / `bad interpreter` in containers | Line endings | Repo `.gitattributes` forces LF; re-clone if you overrode `core.autocrlf` |
| `make: command not found` | Missing build tools | `sudo apt install -y make build-essential` |
| Browser can't open dashboard | Wrong port or stack down | Use **http://localhost:3100** (not 3001); confirm `docker ps` shows `arbiter-dashboard` |

---

## 6. macOS / native Linux

Skip the Windows/WSL section. Install Docker Engine (or Docker Desktop on Mac), Go 1.22+,
`make`, and `python3`, then run the same `make demo-up` / `make build` flow. Compose
already sets `extra_hosts: host.docker.internal:host-gateway` so worker→scheduler
advertise addresses work on Linux too.

---

## 7. Project map (where to look next)

| Doc | Purpose |
|---|---|
| [`README.md`](../README.md) | Project overview + quick demo |
| [`IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md) | Full phase plan + design |
| [`docs/architecture.md`](architecture.md) | Diagrams + repo map |
| [`docs/design-decisions.md`](design-decisions.md) | Per-phase choices |
| [`docs/benchmarks/`](benchmarks/) | Measured DoD / resume evidence |
| [`scripts/README.md`](../scripts/README.md) | Load test / chaos / failover scripts |

Phases 0–10 are complete. Stretch goals (Raft, deeper partitions, etc.) are optional and
listed in the plan Section 11 — not required to run the demo locally.
