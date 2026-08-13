# Local Setup & Run Guide

End-to-end instructions for running Arbiter after Phases 0–10.

**Windows is a first-class host.** You can run the full demo from **PowerShell**
(Windows PowerShell 5.1 or PowerShell 7) with Docker Desktop. WSL2 remains a
supported alternative if you prefer a Linux shell / `make`.

After these steps you should have a live 10-node demo cluster, a working
`arbiterctl` CLI, and the dashboard / Grafana / Prometheus surfaces in a browser.

---

## Quick start (PowerShell)

```powershell
# Prerequisites already installed: Docker Desktop, Go 1.22+, Python 3, git
git clone https://github.com/AnishK05/arbiter-distributed-scheduler.git
cd arbiter-distributed-scheduler

# If scripts are blocked once:  Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
.\scripts\arbiter.ps1 demo-up
.\scripts\arbiter.ps1 build
.\scripts\arbiter.ps1 demo-verify

.\bin\arbiterctl.exe submit demo --replicas 5 --wait
# Browser: http://localhost:3100  (dashboard)
#          http://localhost:3000  (Grafana, admin/admin)
#          http://localhost:9090  (Prometheus)

.\scripts\arbiter.ps1 demo-down
```

`.\scripts\arbiter.ps1` is the Windows equivalent of the Makefile targets
(`demo-up`, `demo-down`, `demo-verify`, `build`, `up`/`down`, phase overlays, `test`, …).

---

## 1. Windows prerequisites (PowerShell path)

### 1.1 Docker Desktop

1. Install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/).
2. Use the **WSL2 backend** (Docker Desktop default). You do **not** need to develop
   inside a WSL distro for the PowerShell path — Desktop still uses a Linux VM to
   run containers, which is what Arbiter’s Compose files expect (including
   `/var/run/docker.sock` DooD mounts).
3. Confirm from PowerShell:

```powershell
docker version
docker compose version
```

4. Recommended Desktop resources (Settings → Resources) for the full demo:
   - **CPU:** ≥ 4 cores
   - **Memory:** ≥ 8 GB (12+ GB for the 750-task concurrency run)
   - **Disk image size:** ≥ 64 GB free

### 1.2 Go, Python, Git

| Tool | Notes |
|---|---|
| [Go 1.22+](https://go.dev/dl/) | Repo pin `1.22.2`. After install, open a **new** PowerShell and run `go version`. |
| [Python 3](https://www.python.org/downloads/) | Check **“Add python.exe to PATH”**. Verify with `python --version`. |
| [Git for Windows](https://git-scm.com/download/win) | Repo `.gitattributes` forces LF for container-facing files. |

Optional: [`curl`](https://curl.se/) is built into modern Windows 10/11 and used by some docs;
`demo-verify` uses `Invoke-WebRequest` instead.

### 1.3 Execution policy (once)

If PowerShell refuses to run the scripts:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

### 1.4 Clone

```powershell
cd $HOME\dev   # or any directory you prefer
git clone https://github.com/AnishK05/arbiter-distributed-scheduler.git
cd arbiter-distributed-scheduler
```

Cloning on the Windows filesystem (e.g. `C:\Users\…`) is fine for the PowerShell
path — Compose build context is sent to Docker Desktop’s Linux engine.

---

## 2. Demo cluster (what comes up)

| Service | Count | Host URL / port |
|---|---|---|
| Scheduler (leader + 2 followers) | 3 | http://localhost:8080 · gRPC `:7000` (followers `:7001`/`:7002`, HTTP `:8086`/`:8087`) |
| Workers (varied capacity) | 10 | metrics on `:8081`–`:8090` |
| Postgres / Redis | 1 each | `:5432` / `:6379` |
| Prometheus | 1 | http://localhost:9090 |
| Grafana | 1 | http://localhost:3000 (admin/admin; anonymous Viewer) |
| Next.js dashboard | 1 | http://localhost:3100 |

First `demo-up` pulls base images and builds Go/Next.js containers — expect several
minutes. Later runs are much faster.

Equivalent without the helper:

```powershell
docker compose -f deploy/docker-compose.demo.yml up -d --build
go build -o bin\arbiterctl.exe .\cmd\arbiterctl
```

---

## 3. Day-to-day commands (PowerShell)

### Submit jobs

```powershell
.\bin\arbiterctl.exe submit demo --replicas 5 --wait
.\bin\arbiterctl.exe submit burn --replicas 10 --cpu-millicores 100 --memory-mb 64 --command 30
.\bin\arbiterctl.exe describe task <task-id>
.\bin\arbiterctl.exe logs <task-id>
.\bin\arbiterctl.exe get nodes
```

### Section 10-style concurrency (500+ peak)

Use a tiny image and memory requests that fit cluster memory (Σ ≈ 12032 MiB).
`--memory-mb 32` caps concurrent running around **376**; use **16** for 500+:

```powershell
docker pull busybox:1.36
python scripts\load_test.py --tasks 750 --cpu-millicores 30 --memory-mb 16 `
  --image busybox:1.36 --command sleep --command 55 `
  --wait-complete --policy bin_pack --name local-concurrency
```

### Failover demos

```powershell
.\bin\arbiterctl.exe submit steady --replicas 100 --cpu-millicores 40 --memory-mb 16 `
  --image busybox:1.36 --command sleep --command 900 --scheduling-policy spread
python scripts\measure_node_failover.py --trials 5 --threshold-ms 3000

python scripts\measure_leader_failover.py --trials 3 --threshold-ms 5000 --submit-after
```

Archived numbers: [`docs/benchmarks/phase10-resume-metrics.md`](benchmarks/phase10-resume-metrics.md).

### Smaller stacks

```powershell
.\scripts\arbiter.ps1 up              # 1 scheduler + 1 worker
.\scripts\arbiter.ps1 phase6-up       # 3 schedulers (HA)
.\scripts\arbiter.ps1 phase8-up       # + Prometheus/Grafana/dashboard
.\scripts\arbiter.ps1 phase9-up       # + simulated autoscaler
.\scripts\arbiter.ps1 down
```

### Tests

```powershell
.\scripts\arbiter.ps1 vet
.\scripts\arbiter.ps1 test

# Integration tests against Compose Postgres/Redis:
docker compose -f deploy/docker-compose.yml up -d postgres redis
$env:ARBITER_TEST_POSTGRES_URL = "postgres://arbiter:arbiter@localhost:5432/arbiter?sslmode=disable"
$env:ARBITER_TEST_REDIS_ADDR = "localhost:6379"
.\scripts\arbiter.ps1 test
```

---

## 4. WSL2 / Linux / macOS alternative

If you prefer `make` and a bash shell:

```bash
make demo-up && make build && make demo-verify
```

On Windows, that means Docker Desktop **plus** an Ubuntu WSL distro with Go/`make`/
`python3` installed, and the repo cloned under `~/…` (not `/mnt/c`) for best
bind-mount performance. See historical notes in
[`IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md) Section 4.

Native Linux/macOS: install Docker Engine (or Desktop), Go, `make`, `python3`, then
use either `make` or the same `docker compose` commands.

---

## 5. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Cannot connect to the Docker daemon` | Desktop not running | Start Docker Desktop; wait until it is healthy |
| `running scripts is disabled on this system` | Execution policy | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |
| `go: command not found` / `python` missing | PATH / new shell needed | Re-open PowerShell after installs; confirm `go version` / `python --version` |
| `arbiterctl` / script can’t find CLI | Binaries not built | `.\scripts\arbiter.ps1 build` → use `.\bin\arbiterctl.exe` |
| Workers never reach `ready` | Schedulers unhealthy / advertise | `docker compose -f deploy/docker-compose.demo.yml ps`; `docker logs arbiter-scheduler-1` |
| Tasks stuck `scheduled` | DooD socket | Confirm workers mount Docker socket; check `docker logs arbiter-worker-1` |
| Disk fills on large bursts | Many task containers | Prefer `busybox:1.36`; run `.\scripts\arbiter.ps1 demo-down` and `docker system prune` |
| Port in use (`5432`, `8080`, …) | Another stack | `.\scripts\arbiter.ps1 demo-down` / `down` |
| Browser can’t open dashboard | Wrong port | Use **http://localhost:3100** |

Compose files keep Linux-style `/var/run/docker.sock` mounts on purpose: Docker Desktop
interprets them inside its Linux VM whether you launch Compose from PowerShell or WSL.

---

## 6. Project map

| Doc | Purpose |
|---|---|
| [`README.md`](../README.md) | Overview + quick demo |
| [`IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md) | Full phase plan |
| [`docs/architecture.md`](architecture.md) | Diagrams + repo map |
| [`docs/design-decisions.md`](design-decisions.md) | Per-phase choices |
| [`docs/benchmarks/`](benchmarks/) | Measured DoD / resume evidence |
| [`scripts/README.md`](../scripts/README.md) | Load test / chaos / failover / PowerShell helper |

Phases 0–10 are complete. Stretch goals in the plan Section 11 are optional.
