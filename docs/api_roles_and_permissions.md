# API roles and permissions

PrimeFlow has two authentication surfaces. They are independent: a credential for
one is never accepted by the other.

| Surface | Who | Credential |
|---|---|---|
| `/api/v1/*` — operator API and console | humans, plus workers and the CLI | a **login session** (cookie) with an operator role, or the static `PRIMEFLOW_API_TOKEN` for machine clients |
| `/api/external/v1/*` — External API | external integrations (PrimeX, BI, webhooks) | an **issued API key** with an API role and per-key security controls |

`GET /api/v1/health` is always open so probes need no credential.

---

## 1. Operator roles (`/api/v1`)

Every operator account has exactly one role. Roles are fixed in code
(`internal/authn`).

| Role | Read (`GET`) | Mutations¹ | Users, API keys, settings |
|---|---|---|---|
| `viewer` | ✓ | — | — |
| `operator` | ✓ | ✓ | — |
| `admin` | ✓ | ✓ | ✓ |

¹ Mutations are every non-`GET` call under `/api/v1` except `auth/login` and
`auth/logout`: triggering, cancelling, retrying and reprioritising runs; pausing
queues; editing deployments and automations.

The static `PRIMEFLOW_API_TOKEN` is treated as `admin` and is exempt from the
CSRF check (it is not an ambient browser credential). Use it only for workers,
the CLI, and server-to-server calls on a trusted network.

**Session mechanics.** `POST /api/v1/auth/login` sets an `HttpOnly` `pf_session`
cookie and a readable `pf_csrf` cookie. Browser (cookie-authenticated) writes
must echo the CSRF value in an `X-CSRF-Token` header. Sessions last
`PRIMEFLOW_SESSION_TTL` (default 168h) and slide forward on use. Changing an
account's role or password, or deactivating it, revokes its sessions
immediately.

**Bootstrapping.** On a fresh database, `PRIMEFLOW_ADMIN_EMAIL` +
`PRIMEFLOW_ADMIN_PASSWORD` seed the first `admin`. Otherwise create one with
`primeflow user add -email you@org -password '…' -role admin`. The last active
admin cannot be demoted or deactivated.

**Admin-only routes** (`admin` role or the static token): everything under
`/api/v1/users`, `/api/v1/api-keys`, `/api/v1/api-roles`, and
`/api/v1/settings/*` (`external-api`, `log-retention`).

**Read routes any role can call:** `GET /api/v1/flows/{name}`,
`GET /api/v1/queues/{name}`, `GET /api/v1/runs/{id}/children`,
`GET /api/v1/stats` (the Dashboard's time-bucketed activity).
**Mutations (`operator`+):** `POST /api/v1/queues` (work-pool settings, including
the autoscaling envelope).

**Unauthenticated:** `GET /api/v1/health` and `GET /metrics` (Prometheus
scrapers carry no credential).

---

## 2. API-key roles (`/api/external/v1`)

API-key roles are fixed in code (`internal/apiauth`). Each is a bundle of
scopes; every External API route requires one scope.

| Role id | Label | Scopes |
|---|---|---|
| `api-admin` | API Administrator | `read:runs`, `read:deployments`, `read:queues`, `read:events`, `write:runs`, `write:deployments`, `write:queues` |
| `api-trigger` | API Trigger | `read:deployments`, `read:runs`, `write:runs` |
| `api-readonly` | API Read-Only | `read:runs`, `read:deployments`, `read:queues`, `read:events` |

### Routes and the scope each needs

| Method | Path | Scope |
|---|---|---|
| GET | `/api/external/v1/health` | *(any valid key)* |
| GET | `/api/external/v1/runs` | `read:runs` |
| POST | `/api/external/v1/runs` | `write:runs` |
| GET | `/api/external/v1/runs/{id}` | `read:runs` |
| GET | `/api/external/v1/runs/{id}/tasks` · `/logs` · `/artifacts` | `read:runs` |
| GET | `/api/external/v1/deployments` | `read:deployments` |
| GET | `/api/external/v1/deployments/{id}` | `read:deployments` |
| POST | `/api/external/v1/deployments/{id}/run` | `write:runs` |
| GET | `/api/external/v1/queues` | `read:queues` |
| GET | `/api/external/v1/queues/{name}/pending` | `read:queues` |
| GET | `/api/external/v1/events` | `read:events` |

The console's **Settings → External API → API Explorer** tab renders this table
live from the same source the server enforces.

### Status codes

| Situation | Code |
|---|---|
| Master switch off | `401` |
| Missing / malformed / unknown / inactive / expired key | `401` |
| Client address not in the key's IP allowlist | `401` |
| Key requires mutual TLS and it was not verified | `401` |
| Valid key, but its role lacks the route's scope | **`403`** |
| Rate limit exceeded | `429` (with `Retry-After`) |

The 401-vs-403 split is deliberate: a `403` means the key is real but its role is
wrong, so an operator knows to change the role rather than reissue the key.

---

## 3. Per-key security controls

Set on the key, enforced on every request it makes.

- **IP allowlist** — comma-separated IPs or CIDR blocks. Empty means any address.
  Only meaningful behind a proxy whose network is in `PRIMEFLOW_TRUSTED_PROXY_CIDRS`;
  otherwise the observed address is the proxy's, not the caller's.
- **Rate limit (requests/minute)** — a per-key token bucket, burst = the limit.
  Blank uses the External API's global default (`default_rate_limit_per_min`,
  600). In-process and therefore per server replica — a cluster-wide limiter
  would need Redis and is out of scope.
- **Require mutual TLS** — the request is refused unless it arrived via a trusted
  proxy that verified a client certificate and forwarded
  `X-SSL-Client-Verify: SUCCESS`. This process terminates plain HTTP only; TLS is
  the edge proxy's job.
- **Redact PII** — nulls run `parameters` and `result`, task `result`, log
  `fields`, artifact `data`, deployment `parameters` and event `payload` in
  responses. Counts, states, timings, names, queues and priorities are
  untouched, so analytics still work.

Rotation (`POST /api/v1/api-keys/{id}/rotate`) issues a new secret and prefix on
the same key; the previous secret stops working the instant it commits. Every
create, rotation, edit and denied attempt is recorded and visible under the
key's **History**.
