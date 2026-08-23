# API Ingestion → Landing — Design Doc

**Status:** Draft · **Last updated:** 2026-08-12 · **Scope:** Batch / micro-batch ingestion of data from external REST/GraphQL APIs **into the Landing (raw) area only.** Everything downstream (bronze → silver → gold) is out of scope.

---

## 1. Goals & non-goals

**Goals**

- Pull data reliably from many heterogeneous external APIs on a schedule (minutes-to-hourly freshness).
- Deliver each source payload into **Landing** immutably and as-is, so any downstream layer can be built or rebuilt without re-hitting the API.
- Support incremental pulls (deltas) with full-refresh as a fallback.
- Guarantee at-least-once delivery into Landing, with enough metadata for downstream dedupe.
- Make failures isolated, observable, and recoverable per source.

**Non-goals**

- Any transformation, typing, dedupe, or modeling. That is the Landing → Bronze hop and beyond — **not covered here.**
- Sub-second streaming / webhooks / CDC. The design leaves a clean seam for it (§8) but does not build it.

The pipeline in scope is exactly two moves: **extract → land raw.** Nothing reads the payload; nothing reshapes it.

---

## 2. Reference stack (opinionated)

| Concern | Production choice | Why this one | PoC — start here |
|---|---|---|---|
| Extraction / connectors | **Lightweight custom Python connectors** (`httpx` + a paginator + cursor) implementing the §4.1 interface | Full control over auth, pagination, incremental, and writing raw-as-is files to Landing — one paradigm, no second platform to run. Deliberately *not* a normalizing framework: **dlt** infers schema and produces typed tables, which is the Landing → Bronze hop, not raw Landing (see §5). **Airbyte** is likewise a heavier catalog tool for later; neither is used to land raw. | **Required.** Custom Python scripts for the 1–2 sources you're proving. No dlt, no Airbyte — both do more than raw landing needs at PoC scale. |
| Landing store | **MinIO** (S3-compatible object storage) | Cheap, immutable raw zone; S3 API means zero rewrite if you move to real S3/GCS. | **Required**, but the local filesystem or a single MinIO container is plenty. |
| Landing format | **Raw response files** — NDJSON/JSON, gzipped, one object per fetched batch | Landing stays truly raw: byte-for-byte what the API returned, no schema, no table format. Max fidelity, trivially replayable. (Turning these into an Iceberg table is the **Bronze** hop — out of scope, §5.) | **Same — raw files on disk.** Write `.json.gz` under `source/entity/ingest_date/`. Nothing to set up. |
| Orchestration | **Dagster** | Asset-based model fits "each source partition is a landed asset"; retries, backfills, scheduling. | **Required.** Run `dagster dev` on one node — the PoC must demonstrate real orchestration (scheduling, retries, backfills), not a cron placeholder. Only full multi-node deployment can wait. |
| Concurrency / parallelism | **Dagster concurrency limits** (across sources + per-source partition cap) + **`asyncio` bounded worker pool** for concurrent requests, all under the per-source **rate limiter** (§4.3, §4.5) | Three levels of parallelism with one shared throttle so throughput is bound by API quota, not workers; opaque-cursor pagination stays sequential. | **Demonstrate it.** Same mechanisms, small numbers: run 2 sources concurrently, cap partition backfill fan-out, and use an async pool with a semaphore for a partitionable source. Proves the model without production tuning. |
| Metadata / state store | **PostgreSQL** | Cursors / high-water marks, run metadata, connector config. | **Deferrable.** Cursor in a SQLite/JSON file (or full-refresh, no cursor); config in YAML; run history from Dagster. Add Postgres when you have concurrent workers or many sources. |
| Secrets | **HashiCorp Vault** | Centralized API keys / OAuth tokens with rotation and audit. | **`.env` file.** Simplest thing that works on one machine — move to Vault when going to prod (rotation, audit, shared access). |
| Observability | **Prometheus + Grafana**, structured logs to **Loki** | Freshness, rows landed, error/retry rate, rate-limit hits. | **Deferrable.** Structured logs to stdout are enough to prove it works. |
| Packaging / runtime | **Docker** + **Kubernetes** (or Docker Compose for a single node) | Reproducible connector runtimes, isolated per-source resources. | **Optional.** Run locally or in one Docker Compose file; K8s only at scale. |

> **PoC in one line:** custom Python connector → SQLite/JSON cursor → **raw `.json.gz` files on local disk (or one MinIO container)**, **orchestrated by Dagster (`dagster dev`)** so the PoC shows real scheduling, retries, and backfills. Secrets in `.env`, logs to stdout. That proves extract → land *with proper orchestration* end to end. Everything else in the "Production choice" column is what you graduate to under real concurrency, volume, and multi-source load.
>
> Lighter production footprint (between PoC and full): Docker Compose + MinIO + Dagster + Postgres on one box, Vault→SOPS. Same architecture, smaller blast radius.

---

## 3. Architecture overview

### 3.1 Production

```
                        ┌───────────────────────────────────────────┐
   External APIs        │              Dagster (orchestrate)         │
   ┌──────────┐         │   schedule · retries · backfills · assets  │
   │ REST/GQL │◄────────┤                                            │
   └──────────┘         └───────┬───────────────┬─────────────┬──────┘
        │ extract               │               │             │
        ▼                       ▼               ▼             ▼
 ┌─────────────┐        ┌──────────────┐  ┌───────────┐ ┌──────────┐
 │ Connectors  │        │  Postgres    │  │  Vault    │ │Prometheus│
 │ custom py   │──state─┤ cursors/meta │  │ secrets   │ │ /Grafana │
 │ (+Airbyte*) │        └──────────────┘  └───────────┘ └──────────┘
 └──────┬──────┘
        │ raw JSON (as-is, untouched)
        ▼
 ┌───────────────────────────────────────────┐
 │ LANDING — raw response files (immutable)   │   storage: MinIO/S3
 │ <source>/<entity>/<ingest_date>/*.json.gz  │   format: NDJSON/JSON, gzipped
 │ (append-only, no schema, no table format)  │   byte-for-byte as returned
 └───────────────────────────────────────────┘
                     │
                     ▼  (out of scope) → Bronze (raw → Iceberg table) → Silver → Gold
```

Two stages only: **extract** (connectors) and **land** (Landing zone). Downstream layers consume Landing but are not part of this design.

### 3.2 PoC

Same two stages and the same core path — the supporting services collapse to their local stand-ins (cursor → SQLite/JSON file, secrets → `.env`, metrics → stdout logs). Dagster stays, because the PoC must demonstrate real orchestration; Landing is just raw files on disk.

```
                        ┌───────────────────────────────────────────┐
   External APIs        │        Dagster (`dagster dev`, one node)   │
   ┌──────────┐         │   schedule · retries · backfills · assets  │
   │ REST/GQL │◄────────┤                                            │
   └──────────┘         └───────────────────┬────────────────────────┘
        │ extract                            │
        ▼                                    │ reads/writes
 ┌─────────────┐                     ┌───────▼────────┐
 │ Connectors  │──── cursor state ───┤ SQLite/JSON    │   secrets: .env
 │ custom py   │                     │ file           │   logs: stdout
 └──────┬──────┘                     └────────────────┘
        │ raw JSON (as-is)
        ▼
 ┌───────────────────────────────────────────┐
 │ LANDING — raw response files (immutable)   │   storage: local disk (or 1 MinIO)
 │ <source>/<entity>/<ingest_date>/*.json.gz  │   format: NDJSON/JSON, gzipped
 │ (append-only, no schema, no table format)  │   byte-for-byte as returned
 └───────────────────────────────────────────┘
```

No Postgres, no Vault, no Prometheus/Grafana — those appear only in the production diagram (§3.1). Everything you build in the PoC carries forward; going to prod swaps the *backends* (local disk→MinIO/S3, file→Postgres, `.env`→Vault, stdout→Prometheus), not the design.

---

## 4. Extraction layer

Most of the engineering lives here. Every connector implements one small interface (§4.1) and handles the same API realities the same way — auth, rate limits, incremental pulls, pagination, schema drift. The rest of this section is that checklist.

### 4.1 Connector interface

One contract, so orchestration treats every source the same:

```python
class Connector(Protocol):
    def discover(self) -> list[Stream]: ...          # what entities exist
    def read(self, stream: Stream, state: State) -> Iterator[RawRecord]: ...
    # yields raw records + emits the new cursor as it goes
```

`state` carries the incremental cursor (§4.4). `read` yields the payload **untouched** — no typing, no reshaping.

> **PoC:** don't over-formalize. A plain function per source is fine at 1–2 sources — extract this interface once you have a few and can see what's genuinely common. The value of the contract is uniformity across *many* sources, which is a production concern.

### 4.2 Authentication

Keys, OAuth secrets, and refresh tokens live in **Vault** — never in code. A shared helper refreshes OAuth tokens ahead of expiry and injects them per request. Rotate in Vault; no connector redeploy.

### 4.3 Rate limiting & retries

One **token-bucket limiter per source** keeps you under the API's quota. Respect `Retry-After` / `X-RateLimit-*` headers. On 429/5xx, **back off exponentially with jitter**, cap attempts, then dead-letter the batch (§6). Auto-retry GETs only — never blind-retry non-idempotent calls.

### 4.4 Incremental pulls (high-water mark)

Each stream keeps a **cursor** (`updated_at`, a sequence ID, or an opaque API cursor) in Postgres per `(source, entity)`, and pulls only records newer than it.

- **Commit-after-land:** advance the cursor *only after* the batch is written to Landing. A crash re-pulls instead of skipping → at-least-once.
- **Full refresh** when the source has no reliable delta field, the cursor is lost, or on a periodic reconciliation run to catch silent updates/deletes.
- Overlapping re-pulls are expected — Landing keeps everything and **does not dedupe** (that's downstream).

### 4.5 Concurrency & parallelism

Three levels, from safest to most constrained:

1. **Across sources** — separate Dagster assets run at once. Free and safe (separate quotas); cap with a global run limit.
2. **Across partitions of one source** — a backfill fans out many partitions; cap with a **per-source limit** so it doesn't blow the quota.
3. **Within one pull** — concurrent page requests. Highest throughput, most limited: only works if you can **split the keyspace** (date/ID ranges, or a known page count). Opaque `next_cursor` paging is **sequential** — page N+1 needs page N first. Use a bounded pool (`asyncio` + semaphore).

All of it **shares the one per-source rate limiter** (§4.3) — parallel workers don't each get their own. So concurrency only helps up to the quota; past that you're quota-bound, not CPU-bound, and more workers just trip 429s.

Parallel fetches can land out of order — fine here: Landing is append-only and unordered (§5), the cursor advances by the max watermark seen, and ordering is a downstream concern.

### 4.6 Pagination

Hide the shape inside the connector so nothing upstream cares: cursor/token, offset/limit, or `Link`-header (RFC 5988). The connector walks pages; it yields records.

### 4.7 Schema drift

APIs add and rename fields silently, so **don't impose a schema at extract time** — land the JSON exactly as received. Detect drift by diffing keys against the last-seen set and **alert instead of failing** — Landing must keep accepting data. Enforcing schema is a downstream (Bronze) job.

---

## 5. Landing zone (raw files, immutable)

Landing is a **pure raw file drop** — byte-for-byte what the API returned, with **no schema, no table format, no per-record parsing beyond what's needed to write it out.** This is the strictest definition of raw and keeps maximum fidelity for replay.

**Layout.** One object per fetched batch, keyed by source, entity, and ingest date:

```
s3://landing/<source>/<entity>/ingest_date=2026-08-14/
    <batch_id>.json.gz          -- the raw response body/records, gzipped (NDJSON or JSON)
    <batch_id>.meta.json        -- sidecar: fetch context (see below)
```

**Sidecar metadata** travels next to each batch (not mixed into the payload), so the raw file stays untouched:

```
{
  "ingested_at":   "2026-08-14T09:15:03Z",   // when we fetched it
  "source":        "shopify",
  "entity":        "orders",
  "source_cursor": "2026-08-14T09:14:58Z",    // cursor value at fetch
  "request_params": { "...": "..." },          // how we asked (replay/audit)
  "batch_id":      "run-8f3c-part-2026-08-14", // Dagster run / partition key
  "record_count":  480,
  "response_sha256": "…"                       // integrity / optional dedupe hint
}
```

- Landing is the **replay buffer and audit trail**. Every downstream layer (starting with Bronze) is rebuilt from these files — no re-hitting the API.
- Cheap object storage is the point: retain Landing effectively forever (or a long TTL) so reprocessing is always possible.
- **Immutability:** append-only. No updates, no deletes (except TTL). Never mutate a landed file — corrections happen downstream.
- **What Landing is *not*:** it is not a queryable table. Making these files into an Iceberg table (parsing records into rows/columns, typing, dedupe) is the **Landing → Bronze** hop — deliberately out of scope here. **This is where a tool like [dlt](https://dlthub.com) fits:** its schema inference + normalization + typed load is exactly Bronze, reading from these raw files. It is intentionally *not* used to land raw, because normalizing would defeat the strict raw zone.

---

## 6. Error handling & recovery

- **Dead-letter** batches that fail to fetch/write or exhaust retries to a parallel path `landing/_dead_letter/<source>/<entity>/…`, with the error context and (where available) the partial payload — never drop silently.
- **Isolated failures:** one source failing does not block others (independent Dagster assets).
- **Backfills:** Dagster partitioned assets rematerialize any historical window by re-running partitions. Because Landing is append-only and cursors commit-after-land, backfills are safe and repeatable.

---

## 7. Orchestration, scheduling & observability

- **Dagster** models each `(source, entity, partition)` as an asset with typed inputs (cursor) and outputs (landed partition), retries with backoff, and per-source schedules (e.g. every 15 min for hot sources, hourly/daily for cold ones).
- **Metrics** (Prometheus → Grafana): freshness (time since last successful land per source), records/files landed per run, error/retry rate, rate-limit hits, dead-letter volume.
- **Freshness SLA alerts** per source; page on stale sources and repeated failures; warn on schema drift and volume anomalies.
- Note: content quality checks (nulls, uniqueness, types) belong to Bronze, not Landing. Landing only asserts *delivery* health — did we land, on time, without errors.

---

## Appendix — Stack at a Glance

🐍 **Custom Python Connectors** ·
🔄 **[Airbyte](https://docs.airbyte.com/)** (optional) ·
📦 **[MinIO](https://min.io/docs/minio/linux/index.html)** ·
❄️ **[Apache Iceberg](https://iceberg.apache.org/docs/latest/)** ·
🔄 **[Dagster](https://docs.dagster.io/)** ·
🐘 **[PostgreSQL](https://www.postgresql.org/docs/)** ·
🔐 **[HashiCorp Vault](https://developer.hashicorp.com/vault/docs)** ·
📊 **[Prometheus](https://prometheus.io/docs/introduction/overview/)** / 📈 **[Grafana](https://grafana.com/docs/)** / 📜 **[Loki](https://grafana.com/docs/loki/latest/)** ·
🐳 **[Docker](https://docs.docker.com/)** / ⚓ **[Kubernetes](https://kubernetes.io/docs/home/)**

*All open source. Scope ends at Landing.*

---

### **[MinIO](https://min.io/docs/minio/linux/index.html)**
Open-source, high-performance **object storage** compatible with AWS S3 APIs. Used for storing unstructured data (e.g., images, logs, backups) in private/cloud environments.

#### **✅ Pros**
- **S3-Compatible**: Works seamlessly with existing S3 tools and libraries.
- **Self-Hostable**: Full control over data (on-prem or cloud).
- **Scalable**: Handles petabytes of data with distributed architecture.
- **Lightweight**: Minimal overhead compared to full cloud solutions.

#### **❌ Cons**
- **No Built-in Analytics**: Requires integration with other tools for data processing.
- **Maintenance**: Self-hosting requires infrastructure management.
- **Limited Features**: Lacks advanced features like lifecycle policies (vs. AWS S3).

---

### **[Apache Iceberg](https://iceberg.apache.org/docs/latest/)**
Open **table format** for huge analytic datasets. Enables ACID transactions, schema evolution, and time travel for data lakes (e.g., on S3, HDFS).

#### **✅ Pros**
- **ACID Compliance**: Supports transactions for reliable data updates.
- **Schema Evolution**: Handles schema changes without breaking queries.
- **Time Travel**: Query historical data snapshots.
- **Performance**: Optimized for large-scale analytics (e.g., with Spark, Trino).

#### **❌ Cons**
- **Complexity**: Requires setup and integration with query engines.
- **Young Ecosystem**: Fewer native integrations than older formats (e.g., Parquet).
- **Not a Database**: Needs a query engine (e.g., Spark) to interact with data.

---
### **[Dagster](https://docs.dagster.io/)**
Open-source **data orchestration** platform for building, running, and monitoring data pipelines. Combines ETL, workflows, and observability.

#### **✅ Pros**
- **Unified Pipeline Management**: Combines data ingestion, transformation, and observability.
- **Python-Native**: Easy to define pipelines in Python.
- **Extensible**: Supports custom integrations and plugins.
- **Observability**: Built-in tools for debugging and monitoring.

#### **❌ Cons**
- **Learning Curve**: Requires familiarity with Python and data engineering concepts.
- **Resource-Intensive**: Needs infrastructure for scaling.
- **Younger Community**: Smaller ecosystem than Airflow or Luigi.

---
### **[PostgreSQL](https://www.postgresql.org/docs/)**
Open-source **relational database** known for its extensibility, SQL compliance, and performance. Supports JSON, full-text search, and ACID transactions.

#### **✅ Pros**
- **ACID-Compliant**: Reliable transactions and data integrity.
- **Extensible**: Supports custom functions, data types, and extensions (e.g., PostGIS for geospatial).
- **Performance**: Optimized for complex queries and high concurrency.
- **Community**: Mature ecosystem with widespread adoption.

#### **❌ Cons**
- **Scalability Limits**: Vertical scaling can be costly; horizontal scaling requires tools like Citus.
- **Complexity**: Advanced features (e.g., partitioning) require expertise.
- **Not NoSQL**: Less flexible for unstructured or rapidly changing data models.

---
### **[HashiCorp Vault](https://developer.hashicorp.com/vault/docs)**
Open-source **secrets management** tool for securely storing and accessing sensitive data (e.g., API keys, passwords, certificates).

#### **✅ Pros**
- **Centralized Secrets**: Securely store and manage secrets in one place.
- **Dynamic Secrets**: Generate short-lived credentials (e.g., database passwords).
- **Encryption as a Service**: Encrypt/decrypt data without managing keys.
- **Audit Logs**: Track access and changes for compliance.

#### **❌ Cons**
- **Complex Setup**: Requires careful configuration for high availability.
- **Operational Overhead**: Self-hosting needs maintenance and monitoring.
- **Learning Curve**: Steep for teams new to secrets management.

---
### **[Prometheus](https://prometheus.io/docs/introduction/overview/) / [Grafana](https://grafana.com/docs/) / [Loki](https://grafana.com/docs/loki/latest/)**
- **Prometheus**: Open-source **monitoring and alerting** toolkit for metrics (e.g., CPU, memory, custom app metrics).
- **Grafana**: Open-source **visualization** platform for metrics, logs, and traces.
- **Loki**: Open-source **log aggregation** system (like Prometheus for logs).

#### **✅ Pros**
- **Prometheus**:
  - Pull-based model for metrics collection.
  - Powerful querying (PromQL) and alerting.
  - Scalable and cloud-native.
- **Grafana**:
  - Rich dashboards with plugins for many data sources.
  - Unified visualization for metrics, logs, and traces.
- **Loki**:
  - Cost-effective log storage (indexes only metadata).
  - Integrates seamlessly with Grafana.

#### **❌ Cons**
- **Prometheus**:
  - No long-term storage by default (requires integrations like Thanos).
  - Limited to metrics (not logs or traces natively).
- **Grafana**:
  - Resource-intensive for large dashboards.
  - Configuration can be complex.
- **Loki**:
  - Less mature than alternatives like ELK for advanced log analysis.
  - Query performance can degrade with high-cardinality labels.

---
### **[Docker](https://docs.docker.com/)**
Open-source **containerization** platform to package, distribute, and run applications in isolated environments.

#### **✅ Pros**
- **Portability**: Run applications consistently across environments.
- **Isolation**: Containers share the host OS kernel but run in isolated user spaces.
- **Lightweight**: Faster and more efficient than virtual machines.
- **Ecosystem**: Huge library of pre-built images (Docker Hub).

#### **❌ Cons**
- **Security Risks**: Containers share the host kernel (vulnerabilities can affect all containers).
- **Complexity**: Managing networks, volumes, and orchestration can be tricky.
- **Not for All Workloads**: Some applications (e.g., high-performance computing) may not benefit.

---
### **[Kubernetes](https://kubernetes.io/docs/home/)**
Open-source **container orchestration** platform for automating deployment, scaling, and management of containerized applications.

#### **✅ Pros**
- **Scalability**: Automatically scale applications based on demand.
- **Self-Healing**: Restart failed containers, replace them, or reschedule them.
- **Extensible**: Supports custom workloads and integrations.
- **Portability**: Run on any cloud or on-prem infrastructure.

#### **❌ Cons**
- **Steep Learning Curve**: Complex to set up and manage.
- **Operational Overhead**: Requires expertise for production-grade deployments.
- **Resource-Intensive**: Needs significant infrastructure for the control plane.

---

### **[Airbyte](https://airbyte.com)**

Open-source data integration platform designed to help organizations extract, transform, and load (ETL/ELT) data from various sources (like databases, APIs, SaaS applications, and files) into data warehouses, lakes, or other destinations.

#### **✅ Pros**
- **Open-Source**: Free to use, customizable, and no vendor lock-in.
- **600+ Connectors**: Pre-built integrations for databases, APIs, SaaS apps, and files.
- **Flexible Syncs**: Supports **batch and real-time (CDC)** data replication.
- **Self-Hostable**: Full control over infrastructure (Docker/Kubernetes).
- **User-Friendly UI**: Easy setup and monitoring via a web interface.
- **Scalable**: Suitable for startups to enterprises.

#### **❌ Cons**
- **Resource-Intensive**: Requires **server resources** (Docker/Kubernetes knowledge) for self-hosting.
- **Overkill for Simple Use Cases**: Heavy for small-scale or single-source syncs.
- **Maintenance Overhead**: Self-hosting requires **ongoing upkeep** (updates, monitoring).
- **Learning Curve**: Steeper for non-technical teams compared to no-code tools.

---
