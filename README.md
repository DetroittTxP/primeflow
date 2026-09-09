# PrimeFlow

การจัดคิวและเรียงลำดับเวิร์กโฟลว์แบบทนทาน (durable workflow orchestration) เขียนด้วยภาษา Go สร้างมาเพื่อ PrimeX

PrimeFlow หยิบแนวคิดที่ทำให้ Prefect เวอร์ชันโอเพนซอร์สทำงานได้ดี — flow เป็นโค้ดธรรมดา,
ติดตามสถานะอัตโนมัติ, retry, work pool, automation ที่ขับด้วยเหตุการณ์ — แล้วสร้างใหม่ให้อยู่ในรูป
ไบนารี Go แบบ static ไฟล์เดียว โดยมี PostgreSQL เป็นเบื้องหลัง flow คือฟังก์ชัน Go
ไม่มี DAG ให้ประกาศ ไม่มี Python runtime ให้แพ็กไปด้วย และไม่มี broker ให้คอยดูแล

```go
sdk.Flow("provision-vm", func(c *sdk.Context) (any, error) {
    p, err := sdk.Params[ProvisionParams](c)
    if err != nil {
        return nil, err
    }

    vm, err := sdk.Task(c, "create-vm", func(c *sdk.Context) (VM, error) {
        return vcd.CreateVM(c, p.OrgID, p.Template)
    }, sdk.TaskRetries(3), sdk.TaskRetryDelay(10*time.Second))
    if err != nil {
        return nil, err
    }

    // คืน worker slot; งานจะกลับมาทำงานต่อที่จุดนี้ในอีกสองนาที
    if err := sdk.Sleep(c, "settle", 2*time.Minute); err != nil {
        return nil, err
    }

    return vm, sdk.Do(c, "register-metering", func(c *sdk.Context) error {
        return metering.Register(c, vm.ID)
    })
}, sdk.Retries(2), sdk.Timeout(30*time.Minute))
```

---

## "ทนทาน" ในที่นี้หมายความว่าอย่างไร

ถ้า worker ถูกฆ่ากลางคัน lease ของงานจะหมดอายุ janitor จะทำเครื่องหมายว่า crashed
แล้ว worker อีกตัวจะรับงานไปทำต่อ ฟังก์ชันจะถูกเรียกใหม่ **ตั้งแต่ต้น** — แต่ `create-vm`
จะคืนผลลัพธ์ที่เก็บไว้แทนการสร้าง VM ตัวที่สอง และการทำงานจะไปต่อจากจุดที่ค้างไว้

นี่คือรูปแบบ checkpoint-and-replay ซึ่งเป็นโมเดลเดียวกับที่ Prefect ใช้ มันมีต้นทุนเพียงกฎการเขียนโค้ดข้อเดียว:

> **side effect ต้องอยู่ภายใน `Task` โค้ดที่อยู่ระหว่าง task อาจถูกรันมากกว่าหนึ่งครั้ง**

แลกกับสิ่งนั้น คุณเขียน Go แบบปกติได้ — loop, เงื่อนไข, early return, `time.Now()`,
goroutine — โดยไม่มีข้อจำกัดเรื่อง determinism ที่เอนจินแบบ replay อย่าง Temporal บังคับ

---

## ตารางเทียบฟีเจอร์

| Prefect OSS | PrimeFlow |
|---|---|
| decorator `@flow` / `@task` | `sdk.Flow` / `sdk.Task[T]` เป็น generic และ type-safe |
| การทำงานแบบทนทาน, การเก็บผลลัพธ์ | task checkpoint ใน Postgres เล่นซ้ำตอน resume |
| การติดตามสถานะ | 9 สถานะ พร้อมตารางเปลี่ยนสถานะที่บังคับใช้ |
| retry อัตโนมัติ | ต่อ task และต่อ flow พร้อม exponential backoff |
| cache ผลลัพธ์ของ task | cache key ข้ามการรัน มี TTL |
| deployment และตารางเวลา | cron (5/6 ฟิลด์ รู้ timezone) และ interval พร้อมคุมการ catch-up |
| work pool / work queue | คิวที่มีชื่อ พร้อม concurrency limit และสวิตช์ pause |
| worker | ใช้ lease, ส่ง heartbeat, drain อย่างนุ่มนวล |
| artifact | Markdown, ตาราง, ลิงก์ และ JSON แนบกับงาน |
| observability | log แบบมีโครงสร้าง, timeline ของงาน, feed เหตุการณ์, สตรีม SSE สด |
| เหตุการณ์และ automation | event log พร้อมกฎแบบ threshold ที่มี action หกชนิด |
| UI แบบ self-hosted | console ไฟล์เดียวรวมมาในตัว ไม่ต้อง build |
| — | **การคุมคิวโดยผู้ดูแล: แถบลำดับความสำคัญ, ปักหมุดขึ้นหน้าสุด, จัดลำดับใหม่สด ๆ** |
| — | **GitOps worker delivery: เรนเดอร์ + commit แมนิเฟสต์ของ worker เข้ารีโปฝั่ง server** |

สองแถวสุดท้ายคือส่วนที่ Prefect ไม่มี และเป็นเหตุผลว่าทำไมโปรเจกต์นี้จึงมีอยู่ แทนที่จะใช้ Prefect

---

## การคุมคิว

ทุกงานมี **priority** (0–100 ค่าเริ่มต้น 50) และ **pin** ที่ใส่หรือไม่ก็ได้
ลำดับการ dispatch เป็นดังนี้เป๊ะ ๆ:

1. งานที่ถูก pin มาก่อน เรียงตามลำดับการ pin
2. จากนั้นเรียง priority จากมากไปน้อย
3. จากนั้นงานที่ถูก schedule ก่อนมาก่อน

ผู้ดูแลเปลี่ยนได้ทั้งหมดขณะรันจริง ผ่าน console หรือ API และการเปลี่ยนมีผลใน lease ถัดไป —
โดยไม่รบกวนงานที่กำลังรันอยู่:

```bash
primeflow queue                                  # ความลึกของแต่ละเลน
primeflow queue show vcd                         # ลำดับการ dispatch ที่แน่นอน
primeflow queue front  <run-id>                  # ให้งานนี้ทำเป็นลำดับถัดไป
primeflow queue priority <run-id> 100            # เลื่อนขึ้นเป็นด่วน
primeflow queue pause  vcd                       # หยุดเลน แต่เก็บงานไว้
```

`GET /api/v1/queues/{name}/pending` คืนลำดับเดียวกับที่เควรีการ lease ใช้ ดังนั้นสิ่งที่ผู้ดูแลเห็นบนจอ
คือสิ่งที่จะเกิดขึ้นจริง

คิวยังมี **concurrency limit** — วิธีที่คุณหยุดไม่ให้ flow provisioning สี่สิบตัวที่รันขนานกันถล่ม
endpoint ของ vCD โดยไม่ต้องแก้โค้ด flow แม้แต่บรรทัดเดียว

---

## สถาปัตยกรรม

```
   PrimeX backend ─┐
   Web console ────┼──► PrimeFlow server ──► PostgreSQL   (สถานะ + คิว, แหล่งความจริงเดียว)
   CLI / webhooks ─┘      │  scheduler + janitor      │
                          │  automations              │
                          │  REST + SSE + /metrics    ▼
                          └──────────────────────►  NATS / Redis   (ปลุก + กระจายสด)
                                                       ▲
                              Workers  ─────────────────┘
                              (ไบนารีของคุณ + pkg/sdk)
```

**Postgres คือแหล่งความจริงเดียว** รวมถึงลำดับคิว การ dispatch เป็นคำสั่งเดียว
`SELECT … FOR UPDATE SKIP LOCKED` ดังนั้น worker จำนวนเท่าไรก็ดึงจากเลนเดียวกันได้โดยไม่ต้องมี broker
และไม่มีการทำงานซ้ำซ้อน คำสั่งนั้นจับและปล่อย row lock หนึ่งแถว ไม่มีลำดับ lock ไม่มีการรอข้ามแถว
และไม่มี advisory lock ที่ถือค้างข้ามการเรียก — worker ทำ deadlock ใส่กันไม่ได้ จุดเดียวที่วงจร (cycle)
อาจก่อตัวได้คือการเรียก sub-flow ซ้อนกัน ซึ่งถูกจำกัดความลึกไว้ (ดูด้านล่าง)

**bus เป็นตัวเร่ง ไม่ใช่สิ่งที่ต้องพึ่ง** มันส่ง "มีงานใหม่ในคิว X", สัญญาณยกเลิก และสตรีมสดของ UI
ถ้ามันล่ม ทุกอย่างยังทำงานได้ แค่มี latency แบบ polling แทนการปลุกทันที เลือก transport ด้วย
`PRIMEFLOW_NATS_URL` หรือ `PRIMEFLOW_REDIS_URL` (NATS ชนะถ้าตั้งทั้งคู่) ถ้าไม่ตั้งเลย
bus แบบ in-process จะรันทั้งระบบบนไบนารีเดียวบวก Postgres

**server สเกลออกด้านข้างได้** scheduler, janitor และตัวประเมิน automation ถูกเลือกเป็น leader
ผ่านแถว lease ดังนั้น replica N ตัวสร้างชุดงานตามตารางหนึ่งชุด ไม่ใช่ N ชุด worker pool สเกลตามสัญญาณ
ปริมาณงานที่ PrimeFlow เผยแพร่ที่ `/metrics` — KEDA `ScaledObject` หรือ HPA ทำงานบน
`primeflow_queue_desired_workers` PrimeFlow ไม่เคยสั่งรัน worker เอง

บันทึกการออกแบบฉบับเต็ม: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

---

## เริ่มต้นอย่างรวดเร็ว

```bash
docker compose up --build     # Postgres, NATS, server, worker 2 ตัว
open http://localhost:8080     # ล็อกอินด้วย admin@primeflow.local / primeflow-admin
```

หรือรันตรง ๆ:

```bash
export PRIMEFLOW_DATABASE_URL="postgres://primeflow:primeflow@localhost:5432/primeflow?sslmode=disable"
export PRIMEFLOW_REDIS_URL="redis://localhost:6379"

go run ./cmd/primeflow server           # API + UI + scheduler + automations
go run ./examples/primex-worker         # worker ที่มี flow ตัวอย่างสองตัว
```

จากนั้นเติมข้อมูลตัวอย่างให้ครบชุด — queue, deployment และบัญชี demo:

```bash
make seed-compose                # ใช้ image ของ compose ไม่ต้องมี Go บนเครื่อง
make seed-compose SEED_RUNS=4    # เติม run ตัวอย่างให้คอนโซลมีของให้ดูด้วย
```

`seed` เขียนลงฐานข้อมูลตรง ๆ เหมือน `migrate` และ `user` จึงใช้ได้ตั้งแต่ก่อน server ขึ้น
และไม่ต้องถือ API token สิ่งที่ได้คือ work queue สามเส้น (`default`, `vcd` ที่คุมไว้ที่ 2 งาน,
`metering`), deployment ครอบคลุมทุก flow ในตัวอย่าง worker และบัญชี demo หนึ่งบัญชีต่อหนึ่งบทบาท
รันซ้ำได้ปลอดภัย: ไม่สร้างของซ้ำ ไม่ปลด pause ที่คุณตั้งไว้ และไม่รีเซ็ตรหัสผ่านของบัญชีที่มีอยู่แล้ว
— รวมถึง `admin@primeflow.local` ที่ `PRIMEFLOW_ADMIN_PASSWORD` สร้างไว้ตอน server บูตครั้งแรก
ถ้ามี Go toolchain บนเครื่องใช้ `make seed` ได้ ซึ่งยิงไปที่ `TEST_DB`

หรือจะสร้าง deployment เองผ่าน API:

```bash
docker compose up -d server        # pick up the new token

export PRIMEFLOW_API_URL=http://localhost:8080     # match PRIMEFLOW_HTTP_PORT
export PRIMEFLOW_API_TOKEN=local-dev-token

curl -sS -X POST $PRIMEFLOW_API_URL/api/v1/deployments \
  -H "Authorization: Bearer $PRIMEFLOW_API_TOKEN" \
  -H 'Content-Type: application/json' -d '{
    "name": "provision-vm-standard",
    "flow_name": "provision-vm",
    "work_queue": "vcd",
    "priority": 50,
    "retries": 1,
    "retry_delay": "30s",
    "timeout": "30m"
  }'

primeflow run provision-vm-standard -param org_name=acme -param name=web-01
primeflow runs
```

Without the token both the `curl` and the CLI get `401 authentication required`.
`GET /api/v1/health` and `GET /metrics` are the only open routes; the console
also accepts `?token=<PRIMEFLOW_API_TOKEN>` on a URL, which is the quick way to
open a view without typing the admin password.

---

## การเขียน worker

worker คือไบนารีของคุณเอง ลงทะเบียน flow แล้วส่งต่อการควบคุม:

```go
package main

import (
    "context"
    "log"

    "github.com/DetroittTxP/primeflow/pkg/primeflow"
    "github.com/DetroittTxP/primeflow/pkg/sdk"
)

func main() {
    sdk.Flow("provision-vm", provisionVM, sdk.Retries(2))
    sdk.Flow("collect-metering", collectMetering)

    log.Fatal(primeflow.RunWorker(context.Background(), primeflow.Options{}))
}
```

### ใช้จาก repository อื่น

worker ไม่จำเป็นต้องอยู่ในรีโปนี้ — และไม่ควรอยู่ มันคือโมดูล Go ของตัวเองที่ *พึ่งพา* PrimeFlow
ไม่ใช่การ fork:

```bash
go mod init github.com/you/my-worker
go get github.com/DetroittTxP/primeflow@v0.2.0
# เขียน main.go ที่ import pkg/sdk แล้วค่อยดึง dependency ที่เหลือของกราฟ
go mod tidy
```

`go get` ดึงมาแค่โมดูลนี้โมดูลเดียว ยังไม่ได้เขียน go.sum ของ dependency ทางอ้อมที่แพ็กเกจใช้
ถ้าข้าม `go mod tidy` การ build จะล้มด้วย `missing go.sum entry`

tag `v0.1.0` ยังประกาศ module path เดิม (`github.com/primex/primeflow`) จึงดึงไม่ได้
ต้องใช้ tag ที่ออกหลังการเปลี่ยนชื่อ path เท่านั้น

รีโปเป็น public จึงไม่ต้องใช้ credential ไม่ต้องตั้ง `GOPRIVATE` และไม่ต้องเขียน URL ใหม่เป็น SSH —
มันผ่าน module proxy และ checksum database เหมือน dependency ตัวอื่น ซึ่งเป็นสิ่งที่ควรเป็น
เพราะ proxy ช่วย cache และ sumdb ทำให้การถูกแก้ไขระหว่างทางตรวจจับได้

[`myworker/`](myworker/) คือโครงที่พร้อมคัดลอกออกไปตั้งเป็นรีโปของตัวเอง: go.mod ของตัวเอง,
flow สองตัวที่ใช้งานได้จริง และ Dockerfile

โค้ดของ flow ไม่ได้อยู่บน server เลย การเพิ่มหรือแก้ flow จึงเป็นการ build และ deploy image ของ
worker เท่านั้น — server ไม่ต้องขยับ และ worker ที่ lease งานของ flow ที่ตัวเองไม่ได้ลงทะเบียนไว้
จะไม่ทำให้งาน fail แต่คืนงานกลับคิวเป็น `AwaitingWorker` การทยอย rollout จึงไม่ใช่การล่ม

### แยกโปรเซสต่อหนึ่งงาน

โดยปกติทุกงานที่ worker lease มาจะรันเป็น goroutine ในโปรเซสเดียวกัน ตั้ง
`PRIMEFLOW_EXEC_MODE=process` แล้ว worker จะ fork ไบนารีตัวเองขึ้นมาหนึ่งโปรเซสต่อหนึ่งงานแทน
main() ของคุณไม่ต้องแก้อะไรเลย — ลูกคือโปรแกรมเดิมที่ถูกเรียกใหม่พร้อมตัวแปร `PRIMEFLOW_RUN_ID`
และ `primeflow.RunWorker` เห็นตัวแปรนั้นแล้วรันแค่งานนั้นงานเดียวแล้วจบ:

```yaml
environment:
  PRIMEFLOW_QUEUES: vcd
  PRIMEFLOW_CONCURRENCY: "4"      # ยังหมายถึง 4 งานพร้อมกัน แต่เป็น 4 โปรเซส
  PRIMEFLOW_EXEC_MODE: process
```

สิ่งที่ได้คือ blast radius เท่ากับหนึ่งงาน: panic ที่หลุดออกมาจาก flow, goroutine ที่ค้าง,
หน่วยความจำที่รั่ว หรือ OOM kill จะกระทบแค่งานเดียว ไม่ลากงานข้างๆ ไปด้วย

ฝั่ง orchestrator ไม่มีอะไรเปลี่ยน: พ่อยังเป็นคน lease (concurrency limit ของเลนจึงยังนับถูก),
ยังส่ง heartbeat ต่อ lease ให้ลูก และการสั่งยกเลิกจะถูกส่งต่อเป็น SIGTERM ซึ่งลูกแปลงเป็นการยกเลิก
context แบบเดียวกับที่ `Engine.Cancel` ทำในโหมดปกติ ถ้าลูกตายโดยยังไม่ปิดสถานะงาน พ่อจะหมดอายุ
lease ให้ทันที งานจึงตกไปเข้าทาง crash ของ janitor เร็วกว่ารอ lease หมดเอง — และได้ retry
ตามงบเดิมทุกประการ

ต้นทุนคือการ start โปรเซสต่อหนึ่งงาน และ `/metrics` ของ worker จะไม่เห็นเวลาของ flow/task
อีกต่อไป เพราะมันเกิดในโปรเซสที่จบไปแล้ว flow ที่สั้นและถี่มากจึงควรอยู่ในเลนที่เป็น `inline` ตามเดิม
โหมดนี้ใช้กับ pull pool เท่านั้น — push receiver รันในโปรเซสตัวเองเสมอ

### เรียก flow จากโค้ดอื่น

ไม่มีแพ็กเกจ client ฝั่ง Go — `internal/store/remote` เป็น `internal/` และพูดเฉพาะ worker API
ระบบอื่นสร้างงานผ่าน External API ด้วย key ที่มี scope `write:runs`:

```bash
curl -X POST https://primeflow.example.com/api/external/v1/runs \
  -H "X-API-Key: $PRIMEFLOW_KEY" -H 'Content-Type: application/json' \
  -d '{"flow_name":"http-healthcheck","work_queue":"default",
       "parameters":{"targets":["https://example.com/health"]}}'
```

การตั้งค่ามาจาก environment ดังนั้น image เดียวกันรันได้ทุกที่:

| ตัวแปร | ความหมาย | ค่าเริ่มต้น |
|---|---|---|
| `PRIMEFLOW_DATABASE_URL` | Postgres DSN | จำเป็น |
| `PRIMEFLOW_NATS_URL` | NATS URL สำหรับอัปเดตสด (ชนะ Redis) | ไม่บังคับ |
| `PRIMEFLOW_REDIS_URL` | Redis URL สำหรับอัปเดตสด | ไม่บังคับ |
| `PRIMEFLOW_QUEUES` | เลนที่จะ poll คั่นด้วยจุลภาค | `default` |
| `PRIMEFLOW_CONCURRENCY` | จำนวนงานที่รันขนานกัน | `4` |
| `PRIMEFLOW_EXEC_MODE` | `inline` = งานเป็น goroutine, `process` = หนึ่งโปรเซสต่อหนึ่งงาน | `inline` |
| `PRIMEFLOW_LEASE` | ระยะเวลา lease | `60s` |
| `PRIMEFLOW_POLL` | ช่วงเวลา poll สำรอง | `2s` |
| `PRIMEFLOW_MAX_SUBFLOW_DEPTH` | `RunDeployment` ซ้อนได้ลึกแค่ไหน | `8` |
| `PRIMEFLOW_LOG_RETENTION` | ตัด `pf_logs` ที่เก่ากว่านี้ (Go duration) | `720h` |
| `PRIMEFLOW_GITSYNC_INTERVAL` | ทุกกี่ครั้งที่ตัว reconciler push worker spec ที่ตั้ง auto-sync แล้ว drift | `2m` |
| `PRIMEFLOW_METRICS_ADDR` | listener `/metrics` ของ worker เอง (เวลาของ flow/task) | `:9090` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | ส่ง trace ไปที่นี่ ไม่ตั้ง = ปิด tracing ไม่มีต้นทุน | ไม่มี |
| `PRIMEFLOW_HTTP_ADDR` | address ที่ server ฟัง | `:8080` |
| `PRIMEFLOW_API_TOKEN` | bearer token สำหรับ client ที่เป็นเครื่อง (worker, CLI) บน `/api/v1` | ไม่มี |
| `PRIMEFLOW_CORS_ORIGIN` | อนุญาตให้ console ของ PrimeX เรียก API | ไม่มี |
| `PRIMEFLOW_ADMIN_EMAIL` / `PRIMEFLOW_ADMIN_PASSWORD` | seed บัญชีผู้ดูแลคนแรกบนฐานข้อมูลว่าง | ไม่มี |
| `PRIMEFLOW_SESSION_TTL` | อายุการล็อกอินของผู้ดูแล (เลื่อนออกเมื่อใช้งาน) | `168h` |
| `PRIMEFLOW_COOKIE_SECURE` | ทำเครื่องหมาย cookie ของ session เป็น `Secure` | อัตโนมัติ (เปิดเมื่อ proxy ถูก trust) |
| `PRIMEFLOW_TRUSTED_PROXY_CIDRS` | เครือข่ายที่เชื่อ header `X-Forwarded-For` / client-cert | ไม่มี |
| `PRIMEFLOW_PUSH_ADDR` / `PRIMEFLOW_PUSH_SECRET` | ตัวรับ push-pool: address ที่ฟัง และ secret HMAC ของการ dispatch | `:8090` / ไม่มี |
| `PRIMEFLOW_OIDC_ISSUER` / `_CLIENT_ID` / `_CLIENT_SECRET` | เปิด OIDC SSO (ต้องมี issuer + client id) | ไม่มี |
| `PRIMEFLOW_OIDC_REDIRECT_URL` | callback ของ OIDC (ไม่ตั้งจะ derive จาก request) | derive |
| `PRIMEFLOW_OIDC_DEFAULT_ROLE` / `_ROLE_CLAIM` / `_ROLE_MAP` | บทบาทแบบ JIT: ค่า fallback, ชื่อ claim, map `group=role,…` | `viewer` / — / — |
| `PRIMEFLOW_RESET_TTL` | อายุลิงก์รีเซ็ตรหัสผ่านที่ผู้ดูแลออกให้ | `1h` |

เมื่อตั้ง `PRIMEFLOW_REDIS_URL` — แม้จะใช้ NATS เป็น bus — ตัว throttle การล็อกอินและ rate limiter
ต่อ API key จะใช้ **token bucket ร่วมบน Redis** ดังนั้นลิมิตคงอยู่ข้าม replica ของ server มิฉะนั้นจะเป็นแบบต่อ process

### เอกสารอ้างอิง SDK

**Flow**

```go
sdk.Flow(name, fn, sdk.Retries(2), sdk.RetryDelay(30*time.Second),
    sdk.Timeout(time.Hour), sdk.Description("…"), sdk.Tags("primex"))

sdk.Flow("resize", sdk.Typed(func(c *sdk.Context, p ResizeParams) (any, error) { … }))
```

**Task**

```go
v, err := sdk.Task(c, "name", fn,
    sdk.TaskKey("stable-key"),          // ตรึง checkpoint key (ดูด้านล่าง)
    sdk.TaskRetries(3),
    sdk.TaskRetryDelay(5*time.Second),  // exponential มีเพดาน
    sdk.TaskTimeout(2*time.Minute),
    sdk.TaskCache("vcd:org:acme", time.Hour), // แชร์ผลลัพธ์ข้ามการรัน
    sdk.TaskEphemeral(),                // ไม่เก็บถาวร รันทุกครั้ง
)

err := sdk.Do(c, "name", fn)            // สำหรับขั้นตอนที่ไม่มีค่าคืน
```

checkpoint key มีค่าเริ่มต้นเป็น `<ชื่อ task>-<n>` ตามลำดับที่ไปถึง **ภายใน loop ที่วนบนข้อมูล
ซึ่งอาจเปลี่ยนระหว่างครั้ง ให้ระบุ key ที่ derive จากข้อมูลเอง** — มิฉะนั้นการเพิ่มรายการจะทำให้ key
ที่ตามมาทั้งหมดเลื่อน และรันงานที่สำเร็จไปแล้วซ้ำ:

```go
for _, org := range orgs {
    row, err := sdk.Task(c, "collect", collectFn, sdk.TaskKey("collect:"+org+":"+day))
}
```

**การรอ**

```go
sdk.Sleep(c, "settle", 2*time.Minute)   // เกิน 30 วินาที จะคืน worker slot
sdk.WaitUntil(c, "window", startOfDay)
sdk.Suspend(until, "รออนุมัติ")
```

**ข้อผิดพลาด**

```go
return sdk.Permanent(err)               // ข้าม retry budget ทั้งหมด
```

**Log, artifact, fan-out**

```go
c.Info("provisioning", "org", p.OrgName)
c.Markdown("summary", "### VM created…")
c.Table("usage", rows)
c.Link("console", vm.Href, "เปิดใน Cloud Director")
c.RunDeployment("notify-oncall", payload, sdk.TriggerPriority(100)) // ยิงแล้วลืม
child, err := c.RunDeploymentAndWait("provision-vm-standard", params) // รอแบบทนทาน — ดู Sub-flows
```

---

## การตั้งตารางเวลา

```json
{
  "name": "nightly-metering",
  "flow_name": "collect-metering",
  "work_queue": "metering",
  "schedule_kind": "cron",
  "schedule": "0 2 * * *",
  "timezone": "Asia/Bangkok",
  "catchup": false
}
```

- `cron` รับนิพจน์ 5 ฟิลด์และ 6 ฟิลด์ (มีวินาทีนำหน้า) และ descriptor แบบ `@hourly`
- `interval` รับ Go duration เช่น `15m` ยึด phase กับเวลาที่สร้าง deployment เพื่อไม่ให้ restart ทำให้เพี้ยน
- `catchup: false` (ค่าเริ่มต้น) สร้างงานหนึ่งครั้งหลังหยุดทำงาน แทนที่จะสร้างทุกช่วงที่พลาดไป
- ตารางเวลาถูกตรวจตอนบันทึก deployment ไม่ใช่แอบตรวจทุกรอบหลังจากนั้น

งานถูกสร้างล่วงหน้าหนึ่งชั่วโมง ซึ่งเป็นสิ่งที่ทำให้คุณเห็น จัดลำดับใหม่ หรือยกเลิกงานของพรุ่งนี้ได้ตั้งแต่วันนี้

---

## เหตุการณ์และ automation

ทุกการเปลี่ยนสถานะเขียน event (`flow-run.FAILED`, `flow-run.COMPLETED`, …)
automation จับคู่ event — พร้อม threshold และช่วงเวลาที่ใส่หรือไม่ก็ได้ — แล้วลงมือ:

```json
{
  "name": "escalate-repeated-provisioning-failures",
  "match_event": "flow-run.FAILED",
  "match_flow": "provision-vm",
  "threshold": 3,
  "window": "10m",
  "action": "run-deployment",
  "action_config": {"deployment": "notify-oncall", "pass_event": true, "priority": 100}
}
```

action: `run-deployment`, `cancel-run`, `set-priority`, `pause-queue`,
`resume-queue`, `webhook`

ระบบภายนอกกระตุ้น flow ในทางกลับกันผ่าน `POST /api/v1/webhooks/{deployment}` —
body ของ request กลายเป็น parameter ของงาน ซึ่งเพียงพอที่จะต่อ vCD หรือระบบ billing เข้ากับ flow ได้ตรง ๆ

---

## การยืนยันตัวตน

สองพื้นผิวที่แยกจากกัน รายละเอียดอยู่ใน [`docs/api_roles_and_permissions.md`](docs/api_roles_and_permissions.md):

- **Operator API และ console** (`/api/v1`) มนุษย์ล็อกอิน (`POST /api/v1/auth/login`)
  แล้วได้ session cookie ที่ถือหนึ่งในสามบทบาท — `viewer` (อ่านอย่างเดียว),
  `operator` (คุมการรัน/คิว/deployment ประจำวัน), `admin` (เพิ่มการจัดการผู้ใช้, API key
  และการตั้งค่า) การเขียนจาก browser ต้องมี CSRF token แบบ double-submit worker
  และ CLI ยังใช้ `PRIMEFLOW_API_TOKEN` เป็น bearer token ซึ่งถือเป็น `admin`
  seed admin คนแรกด้วย `PRIMEFLOW_ADMIN_*` หรือ `primeflow user add`
  - **SSO** ตั้ง `PRIMEFLOW_OIDC_ISSUER` + `_CLIENT_ID` (+ `_CLIENT_SECRET`) แล้วหน้าล็อกอิน
    จะมีปุ่ม **Sign in with SSO** การล็อกอินครั้งแรกจะ provision บัญชี `oidc` แบบ JIT
    บทบาทมาจาก `_ROLE_MAP` บน `_ROLE_CLAIM` มิฉะนั้นใช้ `_DEFAULT_ROLE`
    บัญชีท้องถิ่นและบัญชี SSO อยู่ร่วมกันได้
  - **รีเซ็ตรหัสผ่าน** ไม่มี SMTP ผู้ดูแลออกลิงก์ใช้ครั้งเดียว — console
    **Settings → Users → Reset link** หรือ `primeflow user reset-link -email …` —
    แล้วผู้ใช้ตั้งรหัสผ่านใหม่ที่ `/reset.html`
- **External API** (`/api/external/v1`) เป็น projection ของ run, deployment, queue, event
  ที่ป้องกันด้วยบทบาทและยืนยันด้วย key สำหรับการเชื่อมต่อภายนอก จัดการจาก
  **Settings → External API** ใน console: สวิตช์หลักระดับ global, key ที่ออกให้แต่ละอันมีบทบาท
  (ชุด scope) พร้อม IP allowlist ต่อ key, rate limit, ข้อกำหนด mutual-TLS และการกลบข้อมูล PII
  และ audit trail ต่อ key key ที่บทบาทไม่มี scope ของ route จะได้ `403` key ที่เสียหรือปิดอยู่จะได้ `401`

## API

route ของผู้ดูแลทั้งหมดอยู่ใต้ `/api/v1` `GET /api/v1/health` ไม่ต้องยืนยันตัวตนเสมอ เพื่อให้ probe ไม่ต้องมี credential

| | |
|---|---|
| `GET /summary` | ตัวนับของ dashboard ในการเรียกครั้งเดียว |
| `GET /flows`, `GET /workers` | แคตาล็อกและความมีชีวิตของ worker |
| `GET/POST /deployments`, `POST /deployments/{id}/run` | จัดการและกระตุ้น |
| `POST /deployments/{id}/pause` · `/resume` | หยุดหรือเริ่มการตั้งตารางเวลา |
| `GET/POST /runs`, `GET /runs/{id}` | ลิสต์ สร้าง ตรวจ |
| `GET /runs/{id}/tasks` · `/logs` · `/artifacts` | timeline ของงาน |
| `POST /runs/{id}/cancel` · `/retry` · `/reschedule` | วงจรชีวิต |
| `POST /runs/{id}/priority` · `/front` · `/back` · `/unpin` · `/queue` | **การคุมคิว** |
| `GET/POST /queues`, `GET /queues/{name}/pending` | เลนและลำดับการ dispatch |
| `POST /queues/{name}/pause` · `/resume` | throttle เลน |
| `GET/POST /worker-specs`, `GET/POST/DELETE /worker-specs/{id}`, `POST /worker-specs/{id}/sync` | worker spec สำหรับ GitOps delivery |
| `GET /events`, `GET/POST /automations` | feed เหตุการณ์และกฎ |
| `POST /webhooks/{deployment}` | trigger จากภายนอก |
| `GET /stream` | Server-Sent Events สด |
| `POST /auth/login` · `/auth/logout` · `GET /auth/me` · `GET /auth/config` | ล็อกอินผู้ดูแล |
| `GET /auth/oidc/login` · `/auth/oidc/callback` | OIDC SSO (เมื่อกำหนดค่าไว้) |
| `POST /auth/reset` · `POST /users/{id}/reset-link` | ลิงก์รีเซ็ตรหัสผ่าน |
| `GET/POST /users`, `PATCH/DELETE /users/{id}` | บัญชีผู้ดูแล (admin) |
| `GET/PUT /settings/external-api` | สวิตช์หลัก External API (admin) |
| `GET/POST /api-keys`, `PATCH/DELETE /api-keys/{id}`, `POST /api-keys/{id}/rotate`, `GET /api-keys/{id}/history` | External API key (admin) |
| `GET /api-roles` | แคตาล็อกบทบาท / scope / route (admin) |
| `GET /flows/{name}` | flow หนึ่งตัว: เวอร์ชัน, param schema, run ล่าสุด |
| `GET /queues/{name}` | work pool หนึ่งตัว: worker + `desired_workers` ที่คำนวณ |
| `GET /runs/{id}/children` | sub-flow run ที่งานนี้เริ่ม |
| `GET/PUT /settings/log-retention` | นโยบายเคลียร์ `pf_logs` (admin) |
| `GET/PUT /settings/git` | รีโป GitOps ปลายทางสำหรับ worker delivery — repo URL, branch, base path, PAT (เขียนอย่างเดียว), ธง auto-sync (admin) |
| `GET /stats?window=8h` | กิจกรรมแบ่งช่วงเวลาสำหรับ Dashboard |
| `GET /metrics` | Prometheus (ไม่ต้องยืนยันตัวตน) |

การสร้างงานทุกทาง — `POST /runs`, `POST /deployments/{id}/run`, `POST /webhooks/{deployment}`
และคู่แฝดฝั่ง External — ตรวจ parameter กับ schema ที่ flow ประกาศไว้ก่อนเข้าคิว และตอบ `400`
พร้อมชื่อฟิลด์ที่ผิด แทนที่จะปล่อยงานไป fail ในตัว worker การตรวจนี้ปฏิเสธเฉพาะสิ่งที่ตัว decode
ของ flow จะปฏิเสธอยู่แล้วเท่านั้น: key ที่ไม่รู้จักผ่านได้ (automation ที่ตั้ง `pass_event` แทรก `_event`),
ฟิลด์ที่หายไปผ่านได้แม้ schema จะบอกว่า required (ธงนั้นมาจาก reflection ไม่ได้บอกว่า flow
ต้องการค่าจริงไหม), `null` ผ่านได้ทุกชนิด และ flow ที่ยังไม่มี worker ลงทะเบียนก็ยังสร้างงานได้ตามเดิม
งานตามตารางเวลาไม่ผ่านด่านนี้ เพราะ scheduler สร้างงานตรงจาก parameter ของ deployment

`POST /runs/{id}/cancel` ตอบ `409` ถ้างานจบไปแล้ว (`COMPLETED` / `FAILED` / `CANCELLED`)
ไม่มีอะไรให้หยุด และการรับคำสั่งไว้จะทิ้ง `cancel_requested` ค้างบนงานที่สำเร็จตลอดไป
`CRASHED` ไม่นับว่าจบ — janitor ยังอาจ reschedule ได้ ผู้ดูแลจึงยังสั่งยกเลิกดักไว้ก่อนได้

External API อยู่ใต้ `/api/external/v1` และมีเอกสารใน
[`docs/api_roles_and_permissions.md`](docs/api_roles_and_permissions.md) เป็น projection
ที่ยืนยันด้วย key และกั้นด้วย scope:

| | scope |
|---|---|
| `GET /runs`, `POST /runs`, `GET /runs/{id}` (+ `/tasks` `/logs` `/artifacts`) | `read:runs` / `write:runs` |
| `GET /deployments`, `GET /deployments/{id}`, `POST /deployments/{id}/run` | `read:deployments` / `write:runs` |
| `GET /queues`, `GET /queues/{name}/pending` | `read:queues` |
| `POST /queues` — **สร้าง / อัปเดต work pool** (ผู้เรียกแบบ IaC และ GitOps) | `write:queues` |
| `GET /workers` — ตาราง heartbeat ของ worker สด (อ่านอย่างเดียว; worker ลงทะเบียนเอง) | `read:workers` |
| `GET /worker-specs` · `GET/POST/DELETE /worker-specs/{id}` · `POST /worker-specs/{id}/sync` | `read:worker-specs` / `write:worker-specs` |
| `GET /events` | `read:events` |

---

## การ deploy

**Self-hosted เครื่องเดียว** — [`selfhost/`](selfhost/) คือชุดที่ส่งต่อให้คนอื่นได้ทั้งก้อน:
compose ที่ **ดึง image อย่างเดียว ไม่ต้อง build** (Postgres, NATS, migrate, server + console, worker),
ไฟล์ `.env.example` และ [`selfhost/worker/`](selfhost/worker/) — โมดูล Go ที่ import SDK
และ build ได้โดยไม่ต้องมีซอร์สของรีโปนี้เลย เหมาะกับคนที่จะเอา PrimeFlow ไปติดตั้งเองแล้วต่อ flow ของตัวเอง

```bash
cd selfhost && cp .env.example .env && $EDITOR .env
docker compose up -d
```

เผยแพร่ image ที่ทั้งสองฝั่งดึงด้วย `make docker-push` (buildx, amd64 + arm64)
ต่างจาก `myworker/` ตรงที่โครงใน `selfhost/worker/` ไม่มี `replace` ชี้กลับมาที่รีโปนี้
— มันดึง PrimeFlow ผ่าน module proxy จึงคัดลอกออกไปเป็นรีโปของตัวเองได้ทันที

**Kubernetes** — [`deploy/k8s/primeflow.yaml`](deploy/k8s/primeflow.yaml) มี Deployment ของ server
(สเกลได้อย่างปลอดภัย: การเลือก leader จัดการ loop แบบ singleton) และ Deployment ของ worker หนึ่งตัวต่อคิว
เพื่อให้เลนที่ช้าสเกลได้อิสระ

ให้ worker มี `terminationGracePeriodSeconds` ยาวพอที่จะไปถึง checkpoint ถัดไป เกินจากนั้นก็ไม่มีอะไรหาย —
lease หมดอายุแล้ว worker อีกตัวรับงานไปทำต่อ

**การกำหนดขนาด** เควรีการ dispatch เป็นคำสั่งเดียวที่มี index ต่อคิวต่อการ poll
Postgres หนึ่งตัวรับงานหลายหมื่นครั้งต่อวันได้สบาย ตาราง `pf_logs` คือตัวที่โต ดังนั้นเพิ่มงาน retention
เมื่อเปิดใช้จริง

---

## การทดสอบ

```bash
make test-unit          # ไม่ต้องใช้ฐานข้อมูล
make test-integration   # ทั้งหมด กับ Postgres จริง
```

แพ็กเกจ integration แต่ละตัวรีเซ็ตฐานข้อมูลเดียวกัน จึงรันพร้อมกันไม่ได้ — `make test-integration`
ใส่ `-p 1` ด้วยเหตุนี้

ชุดทดสอบครอบคลุมการรับประกันที่สำคัญ: การ resume แบบทนทานที่ข้าม task ที่เสร็จแล้ว, retry budget,
permanent error ที่ข้าม retry, durable sleep ที่คืน worker, การยกเลิก flow ที่กำลังรัน, ลำดับ priority
และ pin, concurrency limit, ไม่มีการ lease ซ้ำภายใต้ worker ที่รันพร้อมกัน, การหมดอายุ lease และการกู้คืน,
การบังคับใช้กฎการเปลี่ยนสถานะ และการสร้างงานตามตารางแบบ idempotent

---

## โครงสร้างของ repository

```
cmd/primeflow/          server, ตัว migrate และ CLI สำหรับผู้ดูแล
pkg/sdk/                พื้นผิวการเขียน — flow, task, การรอ, artifact
pkg/primeflow/          การต่อสายไฟ ให้ main() ของ worker เหลือห้าบรรทัด
internal/core/          โมเดลโดเมนและตารางการเปลี่ยนสถานะ
internal/store/         interface การเก็บข้อมูล + การ implement บน PostgreSQL
internal/engine/        การรันแบบทนทาน: checkpoint, retry, การยกเลิก
internal/worker/        การ lease, heartbeat, การ drain อย่างนุ่มนวล
internal/server/        REST API, สตรีม SSE, console ที่ฝังมา
internal/scheduler/     การสร้างงานตามตารางและ janitor ของ lease
internal/automations/   กฎที่ขับด้วยเหตุการณ์
internal/bus/           NATS และ Redis pub/sub พร้อม fallback แบบ in-process
internal/gitsync/       การเรนเดอร์ worker spec + เอนจิน git (go-git) + reconciler
internal/metrics/       Prometheus registry + ตัวเก็บคิว ณ เวลา scrape
internal/otelinit/      การตั้งค่า OTLP tracing (ไม่ทำงานถ้าไม่ตั้ง endpoint)
examples/primex-worker/ เดโม provisioning VM, metering และ sub-flow fleet
myworker/               โครงของ worker เป็นโมดูลแยก — จุดเริ่มต้นของ image ของคุณเอง
selfhost/               ชุดสำหรับ self-host: compose ที่ดึง image ล้วน + โครง worker ที่ build ได้ลำพัง
deploy/k8s/             แมนิเฟสต์ + ตัวอย่าง autoscaling KEDA/HPA
```

---

## สำหรับนักพัฒนาใหม่

**ทำให้มันรัน** `docker compose up --build` เรียก Postgres, NATS, Redis, server (`:8080`),
worker สองตัว และตัวรับ push ล็อกอินด้วย `admin@primeflow.local` / `primeflow-admin`
(ค่าเริ่มต้นของ compose) `make build` สร้าง `bin/primeflow` (server + CLI) และ `bin/primex-worker`
สำหรับรันกับ Postgres/NATS ของ compose ตรง ๆ — worker ต้องการแค่ `PRIMEFLOW_DATABASE_URL` +
`PRIMEFLOW_NATS_URL` ไม่จำเป็นต้องเป็น container

**console ฝังมาในตัว ไม่ต้อง build** `internal/server/ui/*.html` ถูกคอมไพล์เข้าไบนารีผ่าน `//go:embed`
([`internal/server/ui.go`](internal/server/ui.go)) `index.html` คือ SPA ทั้งตัว — `<style>` หนึ่งอัน,
`<script>` หนึ่งอัน, section มุมมอง `#v-<name>` สลับด้วย `show(name)`, ข้อมูลผ่าน `api('/path')`
แก้ไขแล้ว rebuild image ของ server (`docker compose up -d --build server`) แล้ว hard-refresh
(favicon และ cache ของ bundle ดื้อ)

**เพิ่ม endpoint**
1. handler ใน `internal/server/*.go` (`handlers.go`, `flows.go`, `apikeys.go`, …)
   ใช้ `decode(r, &body)`, `writeJSON`, `writeErr`, `fail`
2. route ใน `internal/server/server.go` (`mux.HandleFunc("METHOD /api/v1/…", s.h)`)
   `/api/v1/settings/*`, `/api/v1/users*`, `/api/v1/api-keys*` ถูกกั้นเป็น admin โดย `adminOperatorPath`
3. คู่แฝดฝั่ง External (ถ้าต้องการ): handler ใน `internal/server/external.go`, route ใน
   `externalMux()` และเพิ่มลง `apiauth.Routes` พร้อม `Scope` — slice นั้นคือแหล่งความจริงเดียว
   ที่ router, auth middleware และ API Explorer อ่านตรงกัน

**เพิ่ม store method + setting** การเก็บข้อมูลเป็น interface
([`internal/store/store.go`](internal/store/store.go)) ที่มีการ implement เดียว
(`internal/store/postgres`) instance setting เป็นแถว JSON ใน `pf_settings` โดยมี key เป็น string —
ลอก `GetGitConnection` / `PutGitConnection`
([`internal/store/postgres/gitconn.go`](internal/store/postgres/gitconn.go)): ไม่ต้อง migrate,
`INSERT … ON CONFLICT (key) DO UPDATE` และเก็บ secret ให้พ้นเส้นทางการอ่าน

**เพิ่ม schema migration** วางไฟล์ `internal/store/postgres/migrations/000N_name.sql` —
เป็น additive และ idempotent (`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`)
ฝังมาและรันตามลำดับตอน server เริ่ม เว้นแต่ใส่ `-no-migrate`

**การทดสอบ** `make test-unit` (ไม่ต้องมี DB) และ `make test-integration`
(`PRIMEFLOW_TEST_DATABASE_URL`, `-p 1` เพราะแพ็กเกจใช้ DB ร่วมกัน) หมายเหตุ:
`TestLogPartitionMaintenance` ไวต่อปฏิทินและอาจ fail ใกล้รอยต่อของเดือนโดยไม่เกี่ยวกับการแก้ของคุณ

---

## Transport

bus การแจ้งเตือนมีสามการ implement เบื้องหลัง interface เดียว (`internal/bus`)
ลำดับความสำคัญ: **NATS → Redis → in-process**

| ตั้ง | Transport | หมายเหตุ |
|---|---|---|
| `PRIMEFLOW_NATS_URL` | core NATS pub/sub | เหมาะกับ cluster; `nats://host:4222` |
| `PRIMEFLOW_REDIS_URL` | Redis pub/sub | ใช้กับสตรีม UI ได้เช่นกัน |
| ไม่ตั้งทั้งคู่ | in-process | ไบนารีเดียว + Postgres; worker ถอยไปใช้ `PRIMEFLOW_POLL` |

การ dial ล้มเหลวตอนเริ่มจะ log คำเตือนและถอยไป polling — bus ไม่เคยเป็นตัวแบกน้ำหนัก

## Observability

**Metric** `GET /metrics` (ไม่ต้องยืนยันตัวตน เหมือน `/api/v1/health`) เปิดเผย:

| Metric | ชนิด | ความหมาย |
|---|---|---|
| `primeflow_queue_ready{queue}` | gauge | งานที่ถูก schedule และถึงเวลาแล้ว |
| `primeflow_queue_scheduled{queue}` · `_running{queue}` | gauge | backlog และงานที่กำลังทำ |
| `primeflow_queue_desired_workers{queue}` | gauge | `clamp(ceil(ready/target), min, max)` — เป้า autoscale |
| `primeflow_workers_online` · `_total` | gauge | ความมีชีวิตของกองเรือ |
| `primeflow_flow_run_transitions_total{to_state}` | counter | การเปลี่ยนสถานะ |
| `primeflow_flow_run_duration_seconds` · `primeflow_task_run_duration_seconds{outcome}` | histogram | เวลาการรัน |
| `primeflow_http_requests_total{route,method,code}` · `_duration_seconds{route}` | counter/histogram | RED ของ API |

**Trace** ตั้ง `OTEL_EXPORTER_OTLP_ENDPOINT` (env มาตรฐานของ OTEL) แล้ว PrimeFlow จะปล่อย span
`flow_run` → `task_run` ผ่าน OTLP/HTTP โดย trace context ถูกส่งต่อจากผู้เรียก HTTP และข้าม
`RunDeployment` เข้าไปยัง child run ถ้าไม่ตั้ง tracer จะเป็น no-op และไม่มีต้นทุน

## Sub-flow

`RunDeployment(name, params)` fan out แล้วคืนค่าทันที ถ้าจะรอ:

```go
child, err := c.RunDeploymentAndWait("provision-vm-standard", params)
if err != nil { return nil, err }      // child ที่ล้มเหลวคือ permanent error
var vm VM
_ = child.Into(&vm)
```

มันเป็น checkpoint แบบทนทาน: child ถูก trigger เพียงครั้งเดียวไม่ว่า parent จะ replay กี่ครั้ง,
parent คืน worker slot ระหว่างที่ child รัน และ resume ทันทีที่ child ตัวสุดท้ายจบ
(หรือ poll ซ้ำทุก 30 วินาทีเป็น backstop) child ทุกตัวบันทึก `parent_run_id` ดังนั้น console แสดงเป็นต้นไม้
การเรียกซ้อนถูกจำกัดด้วย `PRIMEFLOW_MAX_SUBFLOW_DEPTH` (ค่าเริ่มต้น 8) และ child ที่ deployment
ปรากฏอยู่ในสายบรรพบุรุษแล้วจะถูกปฏิเสธ

## Work pool และ autoscaling

work queue ทำหน้าที่เป็น **work pool** ด้วย: กำหนด `min_workers`, `max_workers`,
`target_ready_per_worker` และ `owner` (`POST /api/v1/queues` หรือ **Work Pools → Autoscale…**
ของ console) จากนั้น server เผยแพร่ `primeflow_queue_desired_workers{queue}` และ KEDA `ScaledObject`
หรือ HPA สเกล Deployment ของ worker ที่ตรงกัน PrimeFlow **ไม่เคยสั่งรัน worker เอง** — มันเผยแพร่เป้า
Kubernetes ลงมือ ดูทั้งสองอย่างใน [`deploy/k8s/primeflow.yaml`](deploy/k8s/primeflow.yaml)

**ทีมภายนอกรัน worker ของตัวเอง**: ชี้ `primex-worker` ไปที่ pool ของคุณด้วย
`PRIMEFLOW_QUEUES=<pool>` ฟิลด์ `owner` จัดกลุ่มมันใน console worker ไม่บล็อกกัน — การ dispatch
เป็นคำสั่ง `SKIP LOCKED` เดียวต่อการ poll

### Push pool

ตั้ง `pool_type: "push"` และ `push_endpoint` (console **Work Pools → Push endpoint…**
หรือ `POST /api/v1/queues`) แล้ว pool จะไม่มี worker ที่ poll แทนที่ด้วย leader จะ `POST`
งานที่พร้อมทุกงาน — `{run_id, flow_name, …}` เซ็นด้วย HMAC โดยใช้ `push_secret` ของ pool เป็น
`X-PrimeFlow-Signature: sha256=…` — ไปที่ endpoint ตัวรับคือไบนารี PrimeFlow ของคุณที่รันเป็น
`primeflow.RunPushWorker` (หรือ `primex-worker -push`): มันตรวจลายเซ็น, claim งานนั้นหนึ่งงาน,
รันด้วยเอนจิน และรายงานตามปกติ การ dispatch ที่ไม่มีใคร claim จะหมดอายุและถูก retry แล้วถูก reclaim
เป็น `CRASHED` โดย janitor เหมือนงานที่ถูกทิ้งอื่น ๆ ชี้ `push_endpoint` ไปที่ Knative `Service` /
Cloud Run URL เพื่อ scale-to-zero — server ping ก็ต่อเมื่อมีงาน

## Console

หน้าเดียวฝังมาในตัว ไม่ต้อง build เปิดมาที่ **Dashboard** — กราฟ flow-run / task-run / event
แบ่งช่วงเวลา 8 ชม. · 24 ชม. · 1 สัปดาห์ (`GET /api/v1/stats`, ต้อง Postgres 14+ สำหรับ `date_bin`
ถ้าไม่มี dashboard จะแสดงยอดรวมเปล่า ๆ) พร้อมการ์ด flow ล่าสุดและ work pool **Runs** มีแถบ timeline
และตัวกรองแบบ segment เปิด run ไหนก็ได้เพื่อดู **Timeline** การรันสไตล์ Temporal (หนึ่งเลนต่อ
checkpoint และ sub-flow บนแกนเวลาร่วมกัน) **Event feed** เป็น timeline แบบราง **Flows** ลิสต์
flow ที่ลงทะเบียนทุกตัวพร้อม param schema และฟอร์ม quick-run แบบมีชนิด — ประกาศ schema เพื่อให้ฟอร์มมีชนิด:

```go
sdk.Flow("provision-vm", provisionVM,
    sdk.ParamsSchema(ProvisionParams{OrgName: "acme", CPU: 2}))
```

### Work Pools

**Create pool…** เปิดฟอร์ม (name, ชนิด `pull`/`push`, concurrency limit, owner และ —
สำหรับ pull pool — envelope autoscaling `min` / `max` / `target-ready-per-worker` สำหรับ push pool —
URL ของ endpoint และ secret HMAC) แต่ละแถวมี **Pause/Resume**, **Limit…**, **Autoscale…** และ
**Make push…**

### Workers

ตารางลิสต์แถว heartbeat สด (worker ลงทะเบียนเองตอนเริ่ม) **คลิกที่แถว** เพื่อดู dialog รายละเอียด:
พารามิเตอร์ (id, pool, concurrency, งานที่กำลังทำ, heartbeat), env `PRIMEFLOW_*` ที่ resolve แล้ว,
**Deployment + Secret YAML** ที่เรนเดอร์จากคอนฟิกสดของ worker ตัวนั้น และ **รายการแพ็กเกจ** ที่ host
ต้องมี (ส่วนประกอบพื้นฐานบวกส่วนที่เจาะจง flow ซึ่งอนุมานจาก tag ของ flow ที่ลงทะเบียน — เช่น tag `vcd`
เพิ่ม "VMware Cloud Director API + client library") เมื่อมี worker spec หนุนอยู่ dialog จะมีแผง
**GitOps delivery** พร้อมสถานะการ sync ปุ่ม **Sync now** และสวิตช์ **Auto-sync**

**Add worker…** เป็นหน้าเต็ม ไม่ใช่ modal มันเก็บ name / concurrency / image, pool ที่มันให้บริการ
(**+ Create pool…** แบบ inline), **checklist ข้อกำหนดของ host ที่กั้นการสร้าง** และวิธี **Delivery**:

- **Git commit + PR/MR** (ค่าเริ่มต้น) — เรนเดอร์ `secret.yaml`, `deployment.yaml`,
  `kustomization.yaml`
- **Argo CD Application** — บวก CR `argoproj.io/v1alpha1 Application`
- **Flux Kustomization** — บวก `kustomize.toolkit.fluxcd.io/v1 Kustomization`
- **Script** — Docker `run` / systemd unit / `kubectl apply` (คัดลอกไปวางเท่านั้น ไม่มี spec ฝั่ง server)

สำหรับวิธีที่ไม่ใช่ script ปุ่ม **Save spec** เก็บ deployment ของ worker เป็นแถว `pf_worker_specs`
และ **Save & sync** ยัง commit แมนิเฟสต์เข้ารีโปทันที สวิตช์ **Auto-sync** ต่อ spec ให้ตัว reconciler
push drift โดยไม่ต้อง sync มือ ฟิลด์ git เติมล่วงหน้าจาก **Settings → Git connection**

### Settings → Git connection

เก็บรีโป GitOps ปลายทาง (`GET/PUT /api/v1/settings/git`): repo URL, branch, base path, ผู้ commit
และ **PAT แบบเขียนอย่างเดียว** (เก็บไว้ ไม่เคยคืน การอ่านรายงานแค่ `has_token`) provider
(`github` / `gitlab` / `other`) derive จาก URL การ **Sync commit ลง branch ตรง ๆ** ไม่มี pull request
ดังนั้นชี้ไปที่รีโปที่คาดหวังพฤติกรรมนั้น (รีโป config แบบ GitOps) ค่า secret ในแมนิเฟสต์ที่เรนเดอร์เป็น
placeholder แทนที่ด้วย SealedSecret / SOPS ในรีโป

---

## GitOps worker delivery

PrimeFlow ไม่ deploy worker เอง แต่มันจะเขียนแมนิเฟสต์ให้ แถว `pf_worker_specs` คือ input
ที่ตัวช่วย Add-worker เก็บ (name, image, pool, concurrency, replica, ชนิด delivery, namespace)
แพ็กเกจ `internal/gitsync` เรนเดอร์แถวนั้นเป็น Kubernetes YAML — เป็นโค้ด Go ชุดเดียวกับที่
พรีวิวใน console, ปุ่ม "Sync now" และ reconciler เรียก จึงมี renderer เพียงตัวเดียว

เอนจิน git (`internal/gitsync/git.go`, [go-git] แบบ pure-Go) clone รีโปจาก
**Settings → Git connection** **เข้าหน่วยความจำ** (credential และแมนิเฟสต์ไม่แตะดิสก์),
เขียนไฟล์ที่เรนเดอร์ใต้ path ของ spec, commit ด้วยผู้เขียนที่กำหนด แล้ว push **ลง branch ตรง ๆ** —
ไม่มี pull request รีโปใหม่ที่ยังไม่มี commit จะถูก bootstrap ให้ ถ้า worktree ไม่เปลี่ยนหลังเขียนไฟล์
การ sync จะเป็น no-op ที่ยังรายงาน HEAD ปัจจุบัน

spec ที่ตั้ง `auto_sync` ถูกขับด้วย reconciler ที่เลือกเป็น leader (บทบาท `gitsync`,
`PRIMEFLOW_GITSYNC_INTERVAL` ค่าเริ่มต้น 2 นาที): แต่ละรอบมันเรนเดอร์ spec auto-sync ทุกตัวใหม่,
เทียบ tree hash กับ `last_synced_hash` แล้ว push เฉพาะตัวที่ drift การ push ที่ล้มเหลวบันทึก
`sync_state='error'` และ `last_error` แล้ว retry รอบถัดไป — ไม่เคยบล็อก server image แบบ distroless
ไม่เปลี่ยน: go-git เป็น pure Go จึงยังไม่มีไบนารี `git` หรือ shell ใน runtime

**ผู้เรียกแบบ External / IaC** ได้ครึ่งเขียนของการจัดการ pool ด้วย
`POST /api/external/v1/queues` (scope `write:queues`), จัดการ worker spec ด้วย
`GET/POST/DELETE /api/external/v1/worker-specs` (+ `/sync`, scope `write:worker-specs`)
และอ่านกองเรือด้วย `GET /api/external/v1/workers` (scope `read:workers`)

[go-git]: https://github.com/go-git/go-git

ขั้นตอนทั่วไป:

1. ตั้งค่า **Settings → Git connection** ครั้งเดียว (repo URL, branch, ผู้ commit, PAT)
2. **Workers → Add worker…** เลือก pool + วิธี delivery + auto-sync ยืนยัน checklist ข้อกำหนดของ host
3. กด **Save & sync** — server เรนเดอร์แมนิเฟสต์และ commit เข้ารีโป CD controller ของคุณ reconcile
   worker ลงทะเบียนตอนเริ่มและปรากฏในตาราง Workers ซึ่งคอนฟิกสดของมัน round-trip กลับเป็น YAML เดิม
4. สำหรับ drift ต่อเนื่อง เปิด **Auto-sync** บน spec แล้ว reconciler push ให้เอง

---

## สิ่งที่ยังไม่ได้ทำ

รายการตามตรงว่าอะไรยังไม่มี:

- **migration การแบ่ง partition `pf_logs` บนตารางใหญ่** การติดตั้งใหม่และเล็กแปลงทันที
  การแปลง `pf_logs` ที่มีข้อมูลหลายล้านแถวจะทำ validation scan หนึ่งครั้งตอน `ATTACH` — รันในช่วง
  maintenance window หลังจากนั้น janitor จะ `DROP` partition รายเดือนที่หมดอายุทั้งก้อน
- **SMTP** การรีเซ็ตรหัสผ่านเป็นลิงก์ใช้ครั้งเดียวที่ผู้ดูแลออกให้ ไม่ใช่ flow "ลืมรหัสผ่าน" แบบ self-service ทางอีเมล
- **API key ต่อผู้ใช้** key ของ External API เป็นของ instance และสร้างโดยผู้ดูแล
- **rate limiting ผ่าน NATS** ลิมิตร่วมใช้ Redis หรือถอยไป in-process ไม่มี limiter บน NATS/JetStream
- **push pool ไม่ build หรือส่งโค้ดให้** ตัวรับยังเป็นไบนารี PrimeFlow ของคุณที่เข้าถึงฐานข้อมูล ไม่มีขั้นตอน upload โค้ด
- **OTLP exporter ตัวเดียว** trace เท่านั้น ไม่มี metric-over-OTLP ไม่มีการ export log
- **GitOps delivery ไม่ครอบคลุมการลบ** การลบ worker spec ไม่แตะรีโป แมนิเฟสต์ที่ push ไปแล้ว
  ยังอยู่จนกว่าผู้ดูแลจะ prune เอง และ push commit ลง branch ตรง ๆ (ไม่มีโหมด PR)
