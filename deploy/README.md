# Two-VM deployment

A control plane on one VM and a worker on another, connected over TLS. The
worked example below uses `mainvm.example.com` and `vm2`; substitute your own.

| Directory | Goes on | Runs |
|---|---|---|
| [`vm-server/`](vm-server/) | `mainvm` | Postgres, the server (API + console), nginx on 443 |
| [`vm-worker/`](vm-worker/) | `vm2` | one worker, no database credential |
| [`k8s/`](k8s/) | a cluster | the same thing as manifests |

Background on why each piece is shaped this way is in
[docs/DEPLOYMENT.md](../docs/DEPLOYMENT.md). This file is the sequence.

---

## 0. Build and push the image

Both VMs run the same image. Build it once, from anywhere:

```bash
docker login                                    # once, on the machine that builds
docker buildx build --platform linux/amd64 \
  -t detroitttttxp/primeflow:$(git describe --tags --always) \
  -t detroitttttxp/primeflow:latest --push .
```

`--platform` matters. The `Dockerfile` defaults `TARGETARCH` to `amd64`, so a
plain `docker build` on an arm64 machine produces an amd64 binary inside an
arm64-*labelled* image — it runs, under emulation, and the mislabelling is
confusing later. Naming the platform makes the manifest and the binary agree.

Deploy the `git describe` tag rather than `latest`, so a rollback is a tag and
not a question about what was pulled when.

**If the repository is private**, both VMs need a credential to pull it. Use a
Docker Hub access token with **Read-only** scope rather than the account
password — it can be revoked on its own, and a VM should not hold a credential
that can also push:

```bash
# on mainvm and on vm2, once
echo '<read-only access token>' | docker login -u detroitttttxp --password-stdin
```

Making the repository public instead removes that step, at the cost of
publishing a binary of your flows to anyone who looks.

> The image's worker is built from `examples/primex-worker`, and flows are
> compiled in. Once you have your own flows, build your own worker image —
> import `pkg/primeflow/worker` and `pkg/sdk`; you do not fork this repo.
> See [docs/DEPLOYMENT.md §3](../docs/DEPLOYMENT.md#3-your-flows-need-your-image).

---

## 1. `mainvm` — the control plane

```bash
scp -r deploy/vm-server mainvm:~/primeflow && ssh mainvm
cd ~/primeflow

cp server.env.example server.env && chmod 600 server.env
$EDITOR server.env                      # every CHANGE_ME

cat > .env <<'EOF'
POSTGRES_PASSWORD=<the same password you put in the DSN>
SERVER_HOST=mainvm.example.com
PRIMEFLOW_IMAGE=detroitttttxp/primeflow:<tag>
EOF
chmod 600 .env

docker compose up -d
docker compose logs -f server
```

`SERVER_HOST` becomes the certificate's SAN and must be **the exact name vm2
will dial**. A worker reaching the server by any other name — an IP, a short
hostname — fails verification even when it connects.

Check it:

```bash
curl -k https://localhost/api/v1/health      # {"status":"ok",...}
```

Then open `https://mainvm.example.com/` and log in with the seeded admin. The
browser will warn about the self-signed certificate; that is expected.

Only 443 and 80 are published. Postgres and the server are on the compose
network and nowhere else — no firewall rule needed to keep them private.

### The pool and its key

In the console: **Pools → Create pool…**, name it `vm2`. Then **Workers → Add
worker… → Issue key…**, scoped to that pool. Copy the secret; it is shown once.

Or from the VM:

```bash
PRIMEFLOW_URL=https://mainvm.example.com SITES="vm2" ./sites/issue-keys.sh
```

The key's `pools` list is a boundary the server enforces: a lease for another
pool comes back empty, and a run in another pool is refused with 403 and
recorded in the key's audit trail.

---

## 2. `vm2` — the worker

Send the directory over first: it is what creates `~/primeflow` on vm2, and the
certificate lands inside it.

```bash
scp -r deploy/vm-worker vm2:~/primeflow
```

The worker has to trust the control plane's certificate, so copy that across
too — from `mainvm`, where it was generated:

```bash
# on mainvm
docker compose -f ~/primeflow/docker-compose.yml cp certs:/certs/server.crt ./server.crt
# if that image has no shell, read it out of the volume instead:
#   docker run --rm -v primeflow_certs:/c alpine cat /c/server.crt > server.crt
scp server.crt vm2:~/primeflow/server.crt
```

Then:

```bash
ssh vm2
cd ~/primeflow

cp worker.env.example worker.env && chmod 600 worker.env
$EDITOR worker.env                      # API URL, token, queues

echo "PRIMEFLOW_IMAGE=detroitttttxp/primeflow:<tag>" > .env

docker compose up -d
docker compose logs -f worker
```

Two log lines say it worked:

```
worker store: primeflow api   url=https://mainvm.example.com  queues=["vm2"]
wake-up stream connected
```

Nothing is published on vm2. Outbound 443 to `mainvm` is the only firewall rule
a worker VM needs.

---

## 3. Confirm they are connected

The worker appears under **Workers** with its pool. Create a deployment on the
`vm2` pool, press **Run now**, and watch the time from scheduled to running:

| Observed | Meaning |
|---|---|
| well under a second | the wake-up stream is working |
| a steady ~15s | something is buffering SSE — re-check `nginx.conf` |
| never starts | the pool is paused, or `PRIMEFLOW_QUEUES` is outside the key's pools |

The Deployments and Runs pages badge the pool column with the reason a queued
run is not starting: `pool paused`, `no worker`, `at limit`, `no endpoint`.
There is deliberately no badge for the third case above, because the worker
genuinely is online — check the key's pools.

---

## Operating notes

**Upgrading.** Migrations are additive and idempotent and run at start-up, so an
upgrade is a new tag and a restart, control plane first:

```bash
# mainvm, then vm2
docker compose pull && docker compose up -d
```

**Rotating a worker key.** Edit `worker.env`, then
`docker compose up -d --force-recreate`. A plain `restart` reuses the container
and keeps the old value: Compose reads `env_file` when a container is created,
not when it starts.

**Restarting a worker** is safe at any point, including mid-run. The lease
lapses, the janitor marks the run crashed, and it resumes from its last
checkpoint on whichever worker leases it next.

**Backups.** Postgres holds everything — runs, checkpoints, users, keys,
settings. Nothing on a worker VM is worth preserving.

```bash
docker compose exec -T postgres pg_dump -U primeflow primeflow | gzip > pf-$(date +%F).sql.gz
```

**Replacing the certificate.** Put a real `server.crt` / `server.key` into the
`certs` volume and restart `edge`; the generator leaves an existing certificate
alone. Then drop `SSL_CERT_FILE` and the `server.crt` mount from vm2.
