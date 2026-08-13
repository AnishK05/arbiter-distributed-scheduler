# Arbiter web dashboard (Phase 8)

Next.js + TypeScript UI for the cluster control plane.

```bash
# Full demo (recommended): dashboard at http://localhost:3100
make demo-up

# Or against a running scheduler only:
cd dashboard
npm install
NEXT_PUBLIC_API_BASE=http://localhost:8080 npm run dev
```

Also via `make phase8-up` → http://localhost:3100.

Windows/WSL2 walkthrough: [`docs/local-setup.md`](../docs/local-setup.md).
