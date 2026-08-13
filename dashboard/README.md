# Arbiter web dashboard (Phase 8)

Next.js + TypeScript UI for the cluster control plane.

```powershell
# Full demo (recommended): dashboard at http://localhost:3100
.\scripts\arbiter.ps1 demo-up
```

```bash
make demo-up
# or: make phase8-up
```

Dev server against a running scheduler:

```bash
cd dashboard
npm install
NEXT_PUBLIC_API_BASE=http://localhost:8080 npm run dev
```

Local run (PowerShell + WSL): [`docs/local-setup.md`](../docs/local-setup.md).
