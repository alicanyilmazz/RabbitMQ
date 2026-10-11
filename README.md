# RabbitMQ: The Complete Practical Guide (RabbitMQ 4.3 + Modern .NET)

> **From zero to production-aware messaging** — a hands-on, English-language reference to brokers, exchanges, queues, routing, durability, acknowledgements, retries, RabbitMQ Streams, observability, and the modern RabbitMQ .NET client.
>
> **Reviewed:** 11 October 2026 · **Broker:** RabbitMQ **4.3.6** · **AMQP 0-9-1 .NET client:** `RabbitMQ.Client` **7.2.2** · **Development:** Docker Desktop, Docker Compose, .NET 8+.
>
> **Learning philosophy:** first understand *what happens to the message*, then implement it. This repository starts with topology, experiments, and carefully explained C# code. Later chapters/projects can implement production-ready services on top of these foundations.

**GitHub project:** [alicanyilmazz/RabbitMQ](https://github.com/alicanyilmazz/RabbitMQ)

**Version accuracy note:** These are the latest stable server/client releases I could verify at the review date: RabbitMQ 4.3.6 (official September 2026 server release) and RabbitMQ.Client 7.2.2 (NuGet August 2026). The broker image is deliberately **pinned** to `rabbitmq:4.3.6-management` rather than the moving `latest` tag. Check the official release pages before upgrading.

> ⚠️ **Local development only:** examples bind ports to loopback, use demonstration credentials, and run a single-node broker. A local Docker container is *not* highly available, even when you declare a quorum queue. Never deploy this Compose file directly as an internet-facing production cluster.

---

## Table of contents

**Part I — Foundations and environment**

1. [What RabbitMQ is (and isn't)](#1-what-rabbitmq-is-and-isnt)
2. [Your first mental model: the entire message journey](#2-your-first-mental-model-the-entire-message-journey)
3. [Local installation on Windows with Docker](#3-local-installation-on-windows-with-docker)
4. [Understanding every Docker setting](#4-understanding-every-docker-setting)
5. [The Management UI, CLI, and operational views](#5-the-management-ui-cli-and-operational-views)
6. [AMQP, protocols, TCP connections, channels, heartbeats, and virtual hosts](#6-amqp-protocols-tcp-connections-channels-heartbeats-and-virtual-hosts)

**Part II — Routing and topology**

7. [Exchanges, bindings, queues, and routing keys](#7-exchanges-bindings-queues-and-routing-keys)
8. [Direct exchange: exact routing](#8-direct-exchange-exact-routing)
9. [Fanout exchange: broadcast](#9-fanout-exchange-broadcast)
10. [Topic exchange: wildcard routing](#10-topic-exchange-wildcard-routing)
11. [Headers exchange: attribute-based routing](#11-headers-exchange-attribute-based-routing)
12. [Default, alternate, exchange-to-exchange, and plugin exchanges](#12-default-alternate-exchange-to-exchange-and-plugin-exchanges)
13. [Exchange comparison and design scenarios](#13-exchange-comparison-and-design-scenarios)

**Part III — Storage and delivery guarantees**

14. [Queue types: classic, quorum, stream, and super stream](#14-queue-types-classic-quorum-stream-and-super-stream)
15. [Durable, persistent, transient, exclusive, auto-delete: separate concepts](#15-durable-persistent-transient-exclusive-auto-delete-separate-concepts)
16. [Message lifecycle: exactly when is a message removed?](#16-message-lifecycle-exactly-when-is-a-message-removed)
17. [Producer safety: publisher confirms, returns, unroutable messages](#17-producer-safety-publisher-confirms-returns-unroutable-messages)
18. [Consumer safety: auto ACK, manual ACK, NACK, reject, requeue](#18-consumer-safety-auto-ack-manual-ack-nack-reject-requeue)
19. [Prefetch, concurrency, ordering, fairness, and backpressure](#19-prefetch-concurrency-ordering-fairness-and-backpressure)
20. [TTL, queue expiration, maximum length, and overflow](#20-ttl-queue-expiration-maximum-length-and-overflow)
21. [Dead-letter exchanges, poison messages, and retry patterns](#21-dead-letter-exchanges-poison-messages-and-retry-patterns)
22. [New in RabbitMQ 4.3: delayed retry, priority, consumer timeouts](#22-new-in-rabbitmq-43-delayed-retry-priority-consumer-timeouts)
23. [Replication, clustering, failover, and what durability cannot promise](#23-replication-clustering-failover-and-what-durability-cannot-promise)

**Part IV — .NET and architecture**

24. [Modern .NET client 7.x migration and API map](#24-modern-net-client-7x-migration-and-api-map)
25. [A small complete .NET 7.x producer and consumer](#25-a-small-complete-net-7x-producer-and-consumer)
26. [C# recipes for routing, properties, acknowledgment, and policies](#26-c-recipes-for-routing-properties-acknowledgment-and-policies)
27. [Message contracts, versioning, correlation, idempotency, and outbox](#27-message-contracts-versioning-correlation-idempotency-and-outbox)
28. [Architecture scenarios: pick a topology for the requirement](#28-architecture-scenarios-pick-a-topology-for-the-requirement)
29. [Operations: monitoring, security, alarms, and performance](#29-operations-monitoring-security-alarms-and-performance)
30. [Hands-on labs: 15 controlled experiments](#30-hands-on-labs-15-controlled-experiments)
31. [Troubleshooting and common misconceptions](#31-troubleshooting-and-common-misconceptions)
32. [Glossary, learning path, and official references](#32-glossary-learning-path-and-official-references)

---

# Part I — Foundations and environment

## 1. What RabbitMQ is (and isn't)

RabbitMQ is a **message broker**: software that receives messages from publishers and routes them to consumers through broker-managed entities. Publishers and consumers usually don't need to be running simultaneously.

Consider a website that generates PDFs. If generating a file takes 40 seconds, keeping the HTTP request open is fragile and inefficient. Instead:

```text
Client                ASP.NET Core API                RabbitMQ                 Worker
  | POST /reports          |                            |                       |
  |----------------------->|                            |                       |
  |                        | publish ReportRequested    |                       |
  |                        |--------------------------->| enqueue               |
  | HTTP 202 + jobId       |                            |                       |
  |<-----------------------|                            | deliver               |
  |                        |                            |---------------------->|
  |                        |                            |                       | generate PDF
  | GET /reports/{jobId}   |                            |                       | save status
  |----------------------->| read job status            |                       |
  |<-----------------------|                            |                       |
```

**Important:** HTTP `202 Accepted` means an API accepted a request for asynchronous processing; it does not say the report is complete. Your application needs its own job status store and business-level success/failure model.

### Advantages

- **Temporal decoupling:** producers and consumers can be online at different times, provided messages are retained appropriately.
- **Work distribution:** multiple workers can compete for jobs from one queue.
- **Selective routing:** publish once to an exchange, let bindings decide destinations.
- **Failure isolation:** a slow downstream service can lag without immediately blocking an upstream request.
- **Operational control:** queues expose backlog, throughput, redeliveries, and consumer counts.

### What RabbitMQ does not automatically provide

- Exactly-once *business effects*.
- Infinite storage or unlimited buffering.
- Automatic consumer-side transaction rollback.
- Database-to-broker atomicity without an outbox or comparable solution.
- A message history you can replay indefinitely using ordinary classic/quorum queues.
- High availability merely because the broker runs in Docker.

### Key distinction: **commands vs. events**

| Message | Meaning | Example | Typical topology |
|---|---|---|---|
| Command | “Please do this.” | `GenerateReport` | Work queue, usually one worker processes each delivery |
| Event | “This already happened.” | `OrderPlaced` | Fanout or topic exchange with independent subscriber queues |
| Request/reply | “Do this and send me a response.” | `GetPriceRequest` | Reply queue or Direct Reply-To with correlation IDs; use sparingly |

RabbitMQ transports data; **the message schema, semantics, ownership, and business state are designed by your application**.

## 2. Your first mental model: the entire message journey

```text
Publisher                     Broker                                              Consumer
    |                            |                                                   |
    | PUBLISH(exchange, key)     |                                                   |
    |--------------------------->|                                                   |
    |                         +--------+                                            |
    |                         |Exchange|                                            |
    |                         +---+----+                                            |
    |                             | look up bindings                                |
    |                  +----------+-----------+                                     |
    |                  v                      v                                     |
    |              Queue A                Queue B                                  |
    |                  |                      |                                     |
    |                  |       DELIVERY       |------------------------------------>|
    |                  |--------------------- |------------------------------------>|
    |                  |                      |                       process         |
    |                  |                      |<------------------------------------|
    |                  |                      |          consumer ACK (if manual)     |
```

There are **two distinct confirmation loops**:

1. **Publisher confirm:** broker confirms accepting the publish under its relevant guarantees. It says **nothing** about whether a consumer completed the job.
2. **Consumer acknowledgement:** consumer acknowledges a **delivery** after processing. It says **nothing** about whether an external database transaction was committed unless your code orders those operations correctly.

**Mental checkpoint:** Exchange is for *routing*. Queue is for *buffering/delivery*. Consumer is for *doing work*. Acknowledgments and confirms answer *different questions*.

### Vocabulary for message movement

- **Published:** client attempted to send a message to an exchange.
- **Routed:** exchange matched one or more destination queues/streams.
- **Ready:** queued and available for delivery.
- **Unacked:** delivered to a consumer that has not yet acknowledged.
- **Acknowledged:** consumer confirmed a delivery; a classic/quorum queue can remove it.
- **Redelivered:** broker delivered it again after a previous unsuccessful delivery attempt; this is normal in at-least-once systems.
- **Dead-lettered:** broker republishes to a configured DLX after a defined condition.
- **Expired / dropped / purged:** message was discarded due to TTL, capacity, administrative action, or other policy.

## 3. Local installation on Windows with Docker

This is the recommended developer setup for a Windows machine because it is reproducible and avoids manually matching Erlang/OTP versions.

### Prerequisites

1. Windows with WSL 2 available; enable virtualization in firmware if needed.
2. Docker Desktop using the **WSL 2 backend** and running Linux containers.
3. PowerShell or Windows Terminal.
4. A browser to open the Management UI.
5. Optional for C# exercises: .NET 8+ SDK and Visual Studio / VS Code.

Run in PowerShell:

```powershell
docker --version
docker compose version
docker info
dotnet --info       # Optional until the .NET sections
```

If Docker reports that the engine is unavailable, start Docker Desktop and check that Linux containers are enabled.

### Recommended repository layout

```text
RabbitMQ/
├── README.md                      # This guide
├── docker-compose.yml             # Reproducible local broker
├── .env.example                   # Sample development settings
├── .gitignore
└── examples/                     # Optional; add .NET projects as you learn
```

### Step A — Create local environment variables

Copy `.env.example` to `.env` (never commit `.env`):

```powershell
Copy-Item .env.example .env
```

Open `.env` and choose your own local-development password:

```dotenv
RABBITMQ_USER=developer
RABBITMQ_PASSWORD=change-this-local-password
RABBITMQ_VHOST=learning
```

`learning` is a **virtual host name**, not a Windows path. We avoid `/` here to keep URLs and CLI examples readable.

### Step B — Start the broker

From the directory containing `docker-compose.yml`:

```powershell
docker compose pull
docker compose up -d
docker compose ps
docker compose logs -f rabbitmq
```

The Compose file uses:

```yaml
services:
  rabbitmq:
    image: rabbitmq:4.3.6-management
    container_name: rabbitmq-local
    hostname: rabbitmq-local
    restart: unless-stopped
    ports:
      - "127.0.0.1:5672:5672"       # AMQP 0-9-1
      - "127.0.0.1:15672:15672"     # Management HTTP UI/API
    environment:
      RABBITMQ_DEFAULT_USER: ${RABBITMQ_USER}
      RABBITMQ_DEFAULT_PASS: ${RABBITMQ_PASSWORD}
      RABBITMQ_DEFAULT_VHOST: ${RABBITMQ_VHOST}
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "check_running"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 20s

volumes:
  rabbitmq_data:
```

### Step C — Log in

- **Management UI:** <http://localhost:15672>
- **Username:** value of `RABBITMQ_USER`
- **Password:** value of `RABBITMQ_PASSWORD`
- **AMQP endpoint:** `localhost:5672`
- **Virtual host:** `learning`

### Step D — Verify the server

```powershell
docker exec rabbitmq-local rabbitmq-diagnostics -q ping
docker exec rabbitmq-local rabbitmq-diagnostics status
docker exec rabbitmq-local rabbitmqctl list_vhosts
docker exec rabbitmq-local rabbitmqctl list_users
docker exec rabbitmq-local rabbitmq-plugins list -e
```

Verify the UI shows the broker and the **Exchanges** and **Queues and Streams** pages. The `-management` image includes the management plugin.

### Step E — Stop, restart, and reset safely

```powershell
# Stop/start while retaining the Docker volume:
docker compose stop
docker compose start

# Recreate the container while retaining named volumes:
docker compose down
docker compose up -d

# DESTRUCTIVE — deletes local persistent RabbitMQ data:
docker compose down -v
```

**The `-v` flag is destructive:** it removes the named Docker volume and with it your local queues, retained messages, users, and broker metadata. Do not use it if you want to test recovery across restarts.

> **Important initialization behavior:** `RABBITMQ_DEFAULT_USER`, `RABBITMQ_DEFAULT_PASS`, and `RABBITMQ_DEFAULT_VHOST` affect the initialization of a **fresh broker database**. Editing `.env` does not automatically recreate or rename accounts and virtual hosts stored in an existing data volume. Use the UI/CLI to change an existing instance, or explicitly reset a disposable local volume.

### Alternative installation approaches

- **Native Windows:** install a compatible Erlang/OTP version and RabbitMQ Server, enable `rabbitmq_management`, then use `rabbitmqctl.bat` and Windows services. Use the [official compatibility matrix](https://www.rabbitmq.com/docs/which-erlang) rather than guessing Erlang versions. Docker is simpler for this course.
- **Linux/macOS:** Docker Compose commands remain largely the same. Replace `Copy-Item` with `cp` and use a shell suitable for the OS.
- **Existing remote broker:** do **not** use this tutorial's demo passwords or publish UI ports publicly; follow your organization's TLS, credentials, vhost, and networking standards.

## 4. Understanding every Docker setting

| Setting | Meaning | Why it matters |
|---|---|---|
| `services.rabbitmq` | Logical Compose service | Other Compose services can access it by `rabbitmq` DNS name |
| `image` | Pinned official broker image | Consistent behavior across learners and CI runs |
| `container_name` | Friendly Docker container name | Convenient for `docker exec`; can reduce flexibility of scaling the service |
| `hostname` | Container hostname | Stable node identity with persisted broker state |
| `restart: unless-stopped` | Restart policy | Restarts after Docker/host restart, unless manually stopped |
| `127.0.0.1:5672:5672` | Host IP : host port : container port | AMQP accessible **locally**, not exposed on all interfaces |
| `127.0.0.1:15672:15672` | Local management HTTP | Browser and management API; **not** AMQP traffic |
| `RABBITMQ_DEFAULT_USER` | Initial account | Development bootstrap; don't rely on defaults for production |
| `RABBITMQ_DEFAULT_PASS` | Initial password | Store outside source control |
| `RABBITMQ_DEFAULT_VHOST` | Initial logical namespace | Separates development topology from other workloads |
| `rabbitmq_data:/var/lib/rabbitmq` | Persistent named volume | Preserves node database and on-disk messages across container replacement |
| `healthcheck` | Broker liveness readiness signal | `docker compose ps` can show health; app startup sequencing needs separate logic |

**Ports you may encounter:**

| Port | Usage | Should you publish locally? |
|---:|---|---|
| `5672` | AMQP (plain TCP for local labs) | Yes, loopback only |
| `5671` | AMQP over TLS (when configured) | Not needed in this lab |
| `15672` | Management HTTP UI and HTTP API | Yes, loopback only |
| `15671` | Management HTTPS (when configured) | Not needed in this lab |
| `4369`, `25672` | Erlang discovery/inter-node distribution | Not needed outside this single-node container |
| `5552` | RabbitMQ Stream protocol (plugin) | Only when doing Stream labs |
| `15692` | Prometheus metrics endpoint (plugin) | Only when monitoring is configured |

### Hostnames that confuse beginners

- **Console app on your Windows host:** `HostName = "localhost"` because Docker exposes port 5672 to the host.
- **Another container in this Compose project:** `HostName = "rabbitmq"` because Compose provides internal DNS.
- **From inside the RabbitMQ container:** `localhost` means *that container*.
- Do not copy a Windows host URL into another container without checking its network context.

## 5. The Management UI, CLI, and operational views

Open <http://localhost:15672>. Page names can evolve; the conceptual views are stable:

| Area | What to inspect | Questions answered |
|---|---|---|
| **Overview** | message rates, totals, nodes, alarms | Is the broker healthy? Is work accumulating? |
| **Connections** | connected apps, users, addresses, heartbeats | Which application is connected? |
| **Channels** | logical AMQP channels, unacked counts | Where are operations and deliveries happening? |
| **Exchanges** | exchange name/type/durability, bindings | Where does a publish route? |
| **Queues and Streams** | ready, unacked, consumers, arguments | Which backlog or consumers are stuck? |
| **Admin** | users, virtual hosts, permissions, policies | Who can access what? What behavior is configured? |

### Queue metrics: `Ready`, `Unacked`, `Total`

```text
Queue: jobs.email
Ready   = 8   (waiting to be delivered)
Unacked = 2   (delivered, awaiting manual ACK)
Total   = 10  (ready + unacked; UI counters can lag briefly)
```

**Scenario:** Consumer receives two jobs and sleeps for ten seconds. With manual acknowledgements, you may see `Unacked=2`. When it ACKs, those deliveries disappear from `Unacked`. If its channel closes before ACK, they can become ready for redelivery.

### The “Get messages” button is not a harmless peek

Management UI **Get messages** is an actual *consumption* operation. Depending on the selected acknowledgement mode, it may remove a message, requeue it, or affect ordering/delivery. Use a throwaway learning queue, not a live business queue. For observation, prefer rates, totals, tracing tools, or a dedicated tap/subscriber queue.

### Handy CLI commands

```powershell
# Queues in our vhost:
docker exec rabbitmq-local rabbitmqctl list_queues -p learning name type durable messages_ready messages_unacknowledged consumers

# Exchanges:
docker exec rabbitmq-local rabbitmqctl list_exchanges -p learning name type durable

# Bindings (source -> destination):
docker exec rabbitmq-local rabbitmqctl list_bindings -p learning source_name destination_name destination_kind routing_key

# Status and connection details:
docker exec rabbitmq-local rabbitmqctl status
docker exec rabbitmq-local rabbitmqctl list_connections name user vhost state
```

`-p learning` selects the virtual host. The CLI can report a different set of fields depending on broker version; use `rabbitmqctl help list_queues` if a field name changes.

## 6. AMQP, protocols, TCP connections, channels, heartbeats, and virtual hosts

### Protocol vs. product vs. client

- **RabbitMQ:** broker software.
- **AMQP 0-9-1:** wire protocol used by this README's `RabbitMQ.Client` examples, with exchanges, bindings, queues, channels, `basic.publish`, `basic.consume`, and acknowledgments.
- **AMQP 1.0:** different wire protocol with different link/disposition semantics; RabbitMQ supports it, but **do not** assume the .NET APIs are interchangeable.
- **RabbitMQ Stream protocol:** optimized access to replayable RabbitMQ Streams; use the appropriate Stream client and plugin.
- **MQTT/STOMP:** supported via related protocol plugins where configured.
- **`RabbitMQ.Client` 7.2.2:** the official modern .NET AMQP 0-9-1 client used here.

### Connection vs. channel

```text
.NET application
  └── IConnection (typically a long-lived TCP connection)
        ├── IChannel (publisher)
        ├── IChannel (consumer A)
        └── IChannel (consumer B)
```

**Connection** is relatively expensive: network socket, protocol negotiation, authentication, heartbeats. **Channel** is a lighter AMQP session multiplexed over that connection. Reuse healthy long-lived connections; create/manage channels according to your application's concurrency and lifecycle needs.

**Critical concurrency rule:** **do not concurrently publish over the same channel without an explicit, correct synchronization strategy.** Concurrent writes can interleave protocol frames. A dedicated publishing channel or serialized writer is often simpler. Consumer acknowledgment delivery tags are **channel-scoped**—ACK a delivery on the channel that received it.

### Heartbeats and recovery

- AMQP heartbeats help detect dead connections that TCP might otherwise notice slowly.
- Client automatic recovery can reconnect and recover topology after certain network failures, but it does not replay *application messages* that were published while unavailable or whose outcome is ambiguous.
- A connection recovered successfully does not mean your external side effects were rolled back or that every in-flight publish succeeded.
- Use publisher confirms plus application retry with stable message IDs to handle uncertainty safely.

### Virtual hosts (`vhost`)

A vhost is a logical namespace for exchanges, queues, bindings, policies, and authorization. Two vhosts may both contain a queue named `orders`, but they are different queues.

```text
RabbitMQ node
  ├── vhost: learning
  │   ├── exchange: orders.events
  │   └── queue:    orders.email
  └── vhost: staging
      ├── exchange: orders.events
      └── queue:    orders.email
```

**A binding does not cross virtual hosts.** Permissions are generally granted per user and per vhost. For production, use separate users and narrowly scoped permissions rather than sharing a powerful administrator account.

---

# Part II — Routing and topology

## 7. Exchanges, bindings, queues, and routing keys

An AMQP 0-9-1 producer **publishes to an exchange**, not directly to a queue. The exchange evaluates **bindings** and routes a copy of the publication to destination queues/streams as appropriate.

```text
                          Routing key: order.created
Producer -> [Exchange] ---------- [Binding] ----------> [orders.email.queue]
                              \---- [Binding] ----------> [orders.analytics.queue]
```

**Definitions and properties:**

| Entity | What it is | Typical declaration properties |
|---|---|---|
| Exchange | Routing table and routing algorithm | `exchange`, `type`, `durable`, `autoDelete`, `arguments` |
| Queue | Delivery/backlog data structure | `queue`, `durable`, `exclusive`, `autoDelete`, `arguments` |
| Binding | Rule connecting exchange to destination | `exchange`, `queue`, `routingKey`, optional `arguments` |
| Routing key | String provided on publication | Matched by direct/topic; ignored by fanout; generally ignored by headers |
| Message properties | Transport metadata (not routing key) | `MessageId`, `ContentType`, `DeliveryMode`, `Priority`, `Expiration`, `Headers` |

**One publication can route to zero, one, or multiple queues.** It is not correct to assume “one message always goes to one queue.” Multiple matching bindings to the same queue do **not** create duplicate copies in that same queue for a single publication.

### Safe mental experiment

Suppose an exchange has **no bindings**. You publish `Hello` successfully over an active channel. What happens?

- With default `mandatory=false`, an **unroutable** message can be discarded.
- With `mandatory=true`, the broker returns an unroutable message to the publisher; your client must handle the return/confirmation mechanism appropriately.
- If an alternate exchange is configured, it may provide another routing attempt.
- **Durable exchange + persistent message does not rescue an unroutable publication.**

## 8. Direct exchange: exact routing

A **direct exchange** matches `routingKey` **exactly** against queue binding keys.

**Scenario: support ticket severity**

```text
                     exchange: tickets.direct
                            |
              +-------------+--------------+
              |                            |
        key = critical                key = normal
              |                            |
       tickets.urgent.q                tickets.standard.q
```

Bindings:

| Queue | Binding key |
|---|---|
| `tickets.urgent.q` | `critical` |
| `tickets.standard.q` | `normal` |

Publishes:

| Publish key | Destination |
|---|---|
| `critical` | urgent queue |
| `normal` | standard queue |
| `CRITICAL` | **neither** (case-sensitive) |
| `critical.eu` | **neither** (not a wildcard match) |

**Many-to-many detail:** If `audit.q` also binds with `critical`, a single critical publication reaches **both** `tickets.urgent.q` and `audit.q`. A direct exchange is *not* a guarantee of only one destination.

**Choose direct when:** routing uses a small set of exact categories: `pdf`, `email`, `critical`, `low`, `order-cancelled`.

**UI exercise:** Exchanges → add `tickets.direct`, type **direct**, durable **true**. Queues → add the two durable classic queues. From each queue, bind from `tickets.direct` with its exact key. Publish `critical` and inspect `Ready` counts.

## 9. Fanout exchange: broadcast

A **fanout exchange** ignores the publication routing key and sends one copy to **every bound destination queue**.

**Scenario: an event with multiple independent subscribers**

```text
                  exchange: users.events.fanout
                             |
                 UserRegistered event
                    /        |         \
                   v         v          v
           email-service   analytics    audit
           [email.q]       [stats.q]   [audit.q]
```

```text
Publish routing key = anything / empty / ignored
Bindings             = email.q, stats.q, audit.q
Result               = each queue gets its own copy
```

**Do not confuse these two designs:**

```text
(A) ONE queue, THREE consumers:                  (B) THREE queues, ONE consumer each:
    shared.jobs.q                                   email.q   stats.q   audit.q
      /  |  \                                        |        |         |
     C1  C2  C3                                    Email    Stats     Audit

    Competing consumers: typically                  Independent subscriptions:
    each message delivered to one                   each queue gets a copy.
    consumer, not all three.
```

**Choose fanout when:** every independent subsystem must see every event. Each subsystem gets its **own queue**; adding more workers to one queue increases throughput of that *one* subscription.

## 10. Topic exchange: wildcard routing

A **topic exchange** matches dot-separated routing keys against binding patterns.

- `*` matches **exactly one** word.
- `#` matches **zero or more** words.
- A literal word must match exactly.
- Keys and words are case-sensitive.

**Scenario: retail events**

```text
exchange: commerce.events.topic

Published keys:
  order.created
  order.cancelled
  payment.failed
  order.eu.created
  inventory.eu.low

Bindings:
  notification.q   -> order.*
  audit.q          -> #
  eu.orders.q      -> order.eu.*
  failed.q         -> *.failed
```

**Matching matrix:**

| Publish routing key | `order.*` | `#` | `order.eu.*` | `*.failed` |
|---|:---:|:---:|:---:|:---:|
| `order.created` | ✅ | ✅ | ❌ | ❌ |
| `order.cancelled` | ✅ | ✅ | ❌ | ❌ |
| `order.eu.created` | ❌ | ✅ | ✅ | ❌ |
| `payment.failed` | ❌ | ✅ | ❌ | ✅ |
| `inventory.eu.low` | ❌ | ✅ | ❌ | ❌ |
| `order` | ❌ | ✅ | ❌ | ❌ |

**Subtle but important:** `order.#` matches `order` **and** `order.created` **and** `order.eu.created`. `order.*` does **not** match `order` and does **not** match `order.eu.created`.

Choose a stable naming convention. For example, `domain.entity.action`:

```text
billing.invoice.created
billing.invoice.paid
identity.user.registered
identity.user.disabled
```

**Choose topic when:** subscribers need rich filtering without the publisher knowing consumer identities. Avoid ad hoc key formats that make bindings impossible to maintain.

## 11. Headers exchange: attribute-based routing

A **headers exchange** evaluates values in AMQP message **headers** against binding arguments instead of looking at the routing key.

**Scenario: document processing**

Publisher sends:

```json
{
  "documentId": "doc-204",
  "fileName": "manual.pdf"
}
```

Transport headers (conceptual):

```text
format = pdf
region = eu
importance = high
```

Bindings might be:

| Destination | Binding headers | `x-match` | Meaning |
|---|---|---|---|
| `eu.pdf.q` | `format=pdf`, `region=eu` | `all` | Both must match |
| `priority.q` | `importance=high`, `region=us` | `any` | Either can match |

**`x-match=all`** means every specified matching header must match. **`x-match=any`** means at least one must match. `x-match` is a **binding control argument**, not a normal content header to be matched. Header values have AMQP types—`"10"` (string) and integer `10` are not equivalent.

**Choose headers when:** routing dimensions are naturally independent key/value attributes. Topic keys are usually simpler for hierarchical event names, and header exchanges are often a less common choice.

**UI exercise:** create durable headers exchange `documents.headers`, queue `documents.eu.pdf.q`, bind with arguments `x-match = all`, `format = pdf`, `region = eu` (ensure value types). Publish using message headers `format=pdf`, `region=eu`; then change one value and observe that no longer matches.

## 12. Default, alternate, exchange-to-exchange, and plugin exchanges

### Default exchange (`""`)

The default exchange **always exists**; its publish name is the empty string. It is a special direct exchange with an automatic binding to each queue by **queue name**.

```text
BasicPublishAsync(exchange: "", routingKey: "jobs.q", ...)
                                 |
                                 v
                         default exchange
                                 |
                                 v
                              jobs.q
```

You often hear “publish directly to a queue.” Under AMQP 0-9-1, this is still publish-to-exchange routing. Don't try to declare or manually bind this special exchange.

### Alternate exchange (AE): unroutable fallback

An **alternate exchange** receives a message when the primary exchange cannot route it.

```text
Producer --> orders.direct --[no matching binding]--> orders.unrouted.fanout
                                                               |
                                                               v
                                                       orders.unrouted.q
```

Example exchange declaration arguments for `orders.direct`:

```text
x-alternate-exchange = orders.unrouted.fanout
```

**Important:** An alternate exchange is about **unroutable publishes**, not failed consumer processing. A dead-letter exchange handles eligible **queue-side removal/dead-letter conditions**. They solve different problems.

### Exchange-to-exchange bindings (E2E)

RabbitMQ can bind one exchange to another:

```text
Producer -> all.events.fanout -> billing.events.topic -> billing.audit.q
```

Both source and destination exchange routing types apply. This is routing delegation, not a separate consumer and not necessarily a new application-level publication.

### Specialized / plugin exchange types

RabbitMQ 4.3 documents additional exchange types, such as **local random**, **modulus hash**, and plugin-provided **consistent hashing**, **random**, or **recent history** exchanges. Availability and semantics vary by installed plugins and broker version; **never assume these are always enabled**. Most learning and business topologies begin with direct, fanout, topic, and headers.

### Predeclared exchanges

Every vhost includes built-in exchanges such as `amq.direct`, `amq.fanout`, `amq.topic`, and the unnamed default exchange. They're useful, but using clearly named application-owned exchanges often makes topology ownership and permissions easier to understand.

### Exchange declaration options

| Parameter | Example | Explanation |
|---|---|---|
| `exchange` | `orders.events` | Unique name within vhost |
| `type` | `topic` | Routing algorithm; cannot casually change on an existing exchange |
| `durable` | `true` | Exchange metadata survives broker restart |
| `autoDelete` | `false` | If true, delete after the last binding disappears (after having a binding) |
| `arguments` | `x-alternate-exchange` | Optional exchange-specific configuration; prefer policies when supported |

Declaring the same exchange repeatedly with equivalent properties is generally safe. Declaring it with an **incompatible type or properties** fails with `PRECONDITION_FAILED` and closes the **channel**.

## 13. Exchange comparison and design scenarios

| Type | Routing criterion | One publication to multiple queues? | Typical use case |
|---|---|---:|---|
| **Direct** | Exact binding key | Yes | Explicit priority/category |
| **Fanout** | Every bound destination | Yes | Broadcast an event |
| **Topic** | `*` and `#` matching | Yes | Filter events by domain/action/region |
| **Headers** | Header attributes and `x-match` | Yes | Multi-attribute subscriptions |
| **Default** | Queue name as routing key | Normally one named queue | Simple point-to-point |

### Five architecture choices

1. **Send a receipt email once:** producer → direct/default exchange → `email.jobs` → N workers consuming the **same queue**.
2. **Notify several independent subsystems:** event → fanout → **one queue per subsystem**.
3. **Subscribe to `order.*` only:** topic exchange → subscriber with matching binding.
4. **Send PDFs for EU to specialized processing:** headers exchange with `x-match=all`.
5. **Keep unroutable messages for inspection:** primary exchange → alternate exchange → unrouted queue; monitor and alert.

---
# Part III — Storage and delivery guarantees

## 14. Queue types: classic, quorum, stream, and super stream

“Queue” does **not** imply that all RabbitMQ data structures have the same delivery and storage semantics. Picking a queue type is an architectural decision.

### Classic queue

Classic queues are the original, flexible **non-replicated** queue type in modern RabbitMQ 4.x.

- Good for development, temporary/exclusive queues, and workloads where losing data on a failed node is acceptable or recoverable.
- Support many features such as per-message TTL, priorities, consumer priorities, and queue length limits.
- A *durable* classic queue on one node **survives normal restarts with persistent messages**, but **does not gain replicas**. If that disk or node is permanently lost, durability alone does not save it.
- Classic queue mirroring was **removed** in RabbitMQ 4.0; old high-availability `ha-mode` tutorials are obsolete.

Example arguments:

```text
name      = tutorial.classic
queueType = classic (or vhost default)
durable   = true
exclusive = false
autoDelete= false
```

### Quorum queue

A quorum queue is a replicated, persistent, **Raft-based** queue designed for high-value work that must survive an individual node failure in a healthy multi-node cluster.

```text
                  Quorum queue: orders.critical
                  +--------------------------+
Publisher ------> | leader   [node A]        |
                  | follower [node B]        |
                  | follower [node C]        |
                  +--------------------------+
                      majority = 2 of 3
```

- Must be durable; cannot be exclusive or server-named.
- Messages are persisted; message priority and delayed retry features changed significantly in 4.3.
- Has a **delivery limit** for poison-message protection (default 20 since 4.0).
- Can enable **at-least-once dead-letter transfer** subject to prerequisites.
- A 3-member quorum tolerates one member loss while a majority remains; losing the majority makes the queue unavailable for operations needing quorum.
- **One-node local quorum queues are educational only**—they have no replication-based high availability.

Declaration argument:

```json
{"x-queue-type": "quorum"}
```

### RabbitMQ Stream

A RabbitMQ Stream is an **append-only, durable, replicated log**, not a destructive work queue.

```text
stream: website.events
+--------+--------+--------+--------+--------+
| event0 | event1 | event2 | event3 | event4 | ...
+--------+--------+--------+--------+--------+
   ^                       ^
   |                       |
Consumer A offset 0   Consumer B offset 3
```

- Multiple independent consumers can read the **same stored event** at different offsets.
- Reading doesn't delete an event; **retention** deletes old stream segments by age/size.
- Streams are useful for event replay, high-throughput feeds, event sourcing-like projections, and analytics.
- RabbitMQ Stream protocol and its dedicated client are generally preferred for high-throughput stream applications.
- Streams provide retention and replay semantics but still require careful consumer offset management.

Stream declaration and retention (conceptual AMQP 0-9-1 arguments):

```json
{
  "x-queue-type": "stream",
  "x-max-age": "7D",
  "x-max-length-bytes": 1073741824
}
```

`7D` is an age-retention setting; `1073741824` is a size limit in bytes. Stream retention is **segment-based**, so don't interpret those numbers as exact per-record removal deadlines.

### Super stream

A **super stream** divides a logical stream into multiple partition streams to scale throughput and enable partitioned consumption. A partition key maps events to a partition. Ordering can be reasoned about **within a partition**, not as one universal order across all partitions. Super streams are configured using the Stream ecosystem/tooling rather than simply setting a flag on an ordinary classic queue.

### Queue choice cheat sheet

| Requirement | Start by considering | Why |
|---|---|---|
| Simple local queue demo | Classic durable | Easy to explore |
| Business-critical orders | Quorum | Replication and message safety |
| Temporary exclusive reply | Classic exclusive | Lifecycle tied to owning connection |
| Millions of retained replayable events | Stream / super stream | Log + offsets + retention |
| Every independent consumer must re-read history | Stream | Non-destructive reads |
| Multiple workers share each pending task | Classic/quorum | Competing-consumer work distribution |

> **Do not choose a type by name alone.** Capacity, queue count, availability goals, fanout, latency, retention, and operation semantics all matter. Consult the official feature matrix before migrating types.

## 15. Durable, persistent, transient, exclusive, auto-delete: separate concepts

These flags are among the most important RabbitMQ concepts—and the most frequently confused.

### 15.1 `durable` (topology metadata)

- `durable: true` for a queue means **the queue's metadata** survives a broker restart.
- `durable: true` for an exchange means **exchange metadata** survives a restart.
- A durable classic queue does **not** automatically guarantee that all published messages survive restart; persistent message delivery is a separate setting.

### 15.2 `DeliveryMode = 2` (message persistence, AMQP 0-9-1)

- `DeliveryMode = 1`: transient message.
- `DeliveryMode = 2`: persistent message.
- Publish persistent messages to durable destinations if restart survival matters.
- A persistence flag is necessary but **not sufficient** for a strong end-to-end guarantee; use confirms and understand storage failure modes.
- Quorum queues persist messages by design; classic queues distinguish transient vs. persistent message semantics on recovery.

### 15.3 `exclusive` (connection ownership)

An exclusive queue can only be used by its declaring connection, and its lifecycle is tied to that connection. It is intended for temporary connection-specific needs, not shared business work. Classic queues support exclusive queues; quorum queues do not.

### 15.4 `autoDelete` (lifecycle based on consumers)

A queue configured `autoDelete: true` is eligible to be deleted after its **last consumer** has been cancelled or disconnected, **provided it previously had a consumer**. This is different from simply “delete when empty.” If it has never had a consumer, automatic deletion from that rule may not occur.

For an **exchange**, auto-delete is based on losing the **last binding** after having had a binding, not consumer count.

### 15.5 TTL settings are different again

- **Message TTL:** how long a message may remain eligible for delivery before it expires.
- **Queue TTL / `x-expires`:** how long an unused queue may remain before deletion.
- **Stream retention:** how long/large stream log segments are retained; not ACK-driven removal.

### Survival matrix (classic queues)

| Queue | Message | Broker normal restart | Result expectation |
|---|---|---|---|
| Durable | Persistent | Yes | Queue/message **designed to recover** (subject to confirm, storage integrity, policies) |
| Durable | Transient | Yes | Queue recovers; transient messages **do not** survive recovery |
| Non-durable | Either | Yes | Queue removed on restart; messages lost with it |
| Exclusive temporary | Either | Owner connection closes | Queue is deleted, with its messages |
| Durable | Persistent | **Docker volume deleted** | **Gone**: storage itself was removed |

> **RabbitMQ 4.3 compatibility warning:** **non-durable, non-exclusive classic queue declarations are disabled by default**. Old tutorials using `durable: false, exclusive: false` may now fail. Prefer a durable queue or an exclusive temporary queue. Do not blindly enable deprecated features to make old training code run.

### Scenario: “My message disappeared after Docker restart”

Check, in order:

1. Did you accidentally run `docker compose down -v`?
2. Was the queue durable and not auto-deleted/exclusive?
3. Was a classic-queue message marked persistent?
4. Was the publish routed into that queue before the restart?
5. Did the consumer already ACK or did the message expire/dead-letter/purge?
6. Did the broker actually confirm responsibility for the publish?
7. Was the message moved due to overflow or delivery limit?

**One Boolean flag cannot answer all seven questions.**

### Queue declaration arguments and characteristics

| Field | Example | Effect |
|---|---|---|
| `queue` | `reports.pending` | Queue name (unique within vhost) |
| `durable` | `true` | Survives broker restart as topology |
| `exclusive` | `false` | Not owned by a single connection |
| `autoDelete` | `false` | Does not disappear after last consumer disconnect |
| `arguments.x-queue-type` | `quorum` | Queue implementation type |
| `arguments.x-message-ttl` | `60000` | Queue-level message TTL, in milliseconds |
| `arguments.x-expires` | `3600000` | Queue expiration after being unused; milliseconds |
| `arguments.x-max-length` | `1000` | Approximate allowed ready backlog, subject to queue behavior |
| `arguments.x-dead-letter-exchange` | `jobs.dlx` | Exchange for dead-lettered messages |
| `arguments.x-max-priority` | `5` | Classic queue priority levels; **ignored by quorum** |
| `arguments.x-single-active-consumer` | `true` | Keep one active consumer while others stand by |

Prefer **policies** for settings you may need to change at runtime. Queue **type**, exclusivity, and other structural properties are not simply mutable by re-declaring the same name.

## 16. Message lifecycle: exactly when is a message removed?

The most important question is not simply “Are messages persistent?” It is “**What event causes a message to leave this queue?**”

### Case A: published but not routed

```text
publish -> exchange -> no matching destination
                              |
                       discarded, OR
                       returned (mandatory), OR
                       sent through alternate exchange
```

A durable exchange is **not a mailbox** that holds unmatched messages until a queue appears later.

### Case B: message waiting with no consumers

The message stays **Ready** if the destination queue exists and no TTL/capacity/purge/deletion/failure condition removes it. You can stop the worker, publish three jobs, and start the worker later.

### Case C: `autoAck=true`

```text
Queue -> delivery -> broker considers it handled immediately
                     |
                     +-- consumer crashes before doing work? The job is lost.
```

Automatic acknowledgements are convenient for demonstrations or lossy telemetry, but **not** recommended for important, side-effecting jobs.

### Case D: manual ACK (`autoAck=false`)

```text
Queue --deliver--> Consumer
           |          |
           |          +-- processes successfully -> BasicAck -> removed from queue
           |          +-- process fails -> BasicNack/Reject -> requeue or dead-letter/drop
           |          +-- connection lost before ACK -> broker requeues/redelivers
```

Once the queue receives a valid ACK and considers the delivery settled, the message is no longer available for normal replay from classic/quorum queues. A consumer ACK is **not** the same as persisting the work outcome in your business database.

### Case E: `BasicNack(requeue: true)`

Message is returned for redelivery (potentially to another consumer). If the underlying failure is permanent, immediate requeue can create a **hot infinite loop**. Quorum queues also have poison-message and retry behavior which can bound/modify this outcome.

### Case F: `BasicReject(requeue: false)` or `BasicNack(requeue: false)`

The rejected message is removed from the source queue; if a valid DLX route is configured it can be dead-lettered. Otherwise, it can be discarded.

### Case G: TTL / overflow / delivery limit

A queued message may expire; a queue may overflow; a quorum message may exceed its delivery limit. Depending on policies, it can be **dead-lettered or discarded**. The consumer might never see it.

### Case H: manual purge or deletion

- **Purge queue:** removes queued ready messages; it does not mean that currently unacknowledged deliveries have been processed.
- **Delete queue:** removes the queue and its messages.
- **Delete vhost/volume:** removes much larger sets of broker state. Exercise extreme caution.

### Side-by-side behavior matrix

| Situation | Classic/quorum work queue | RabbitMQ Stream |
|---|---|---|
| Publish successfully | Stored/routed according to queue behavior | Appended to log |
| Receive message | Delivery becomes outstanding | Consumer reads offset |
| ACK successfully | Message can be removed | Does **not** erase event |
| Consumer crashes pre-ACK | Redelivery is possible | Consumer re-reads from tracked offset |
| Replay an ACKed record next week | Not available in normal queue | Possible while data retained |
| Data removed over time | ACK/reject/TTL/limits/purge/delete | Retention age/size (segment-based) |

## 17. Producer safety: publisher confirms, returns, unroutable messages

**There are three different failure questions:**

1. Did the publisher **send** the message over the TCP connection?
2. Did RabbitMQ **accept/confirm** the publish under the queue type's durability guarantees?
3. Did the exchange **route** it to at least one destination?

A normal method return without confirm tracking is insufficient proof of (2), and does not automatically prove (3).

### Publisher confirmations

When enabled on a publishing channel, the broker can **ACK/NACK publishes**. In RabbitMQ.Client 7.x you enable publisher confirms at channel creation using `CreateChannelOptions`. When **confirmation tracking** is enabled, awaiting `BasicPublishAsync` can wait for the corresponding broker confirmation and surface failures.

```text
Publisher ------------------ publish ------------------> Exchange -> Queue
Publisher <------------------ publisher confirm ------- Broker

Consumer <------------------- deliver ----------------- Queue
Consumer -------------------- consumer ACK -----------> Broker
```

**Not equivalent:** confirm says the broker handled responsibility for *publishing*; consumer ACK says a *delivery* was handled. A message can be confirmed by a broker and later fail in business processing.

### Mandatory returns

The **`mandatory` flag** affects unroutable messages:

- `mandatory: false`: unroutable publish may be discarded without being returned.
- `mandatory: true`: broker returns a message that cannot be routed; in the 7.x tracked-publish path, an unroutable return can surface as a publish failure to handle.
- **Alternate exchange** might route otherwise-unroutable messages; interpret return behavior together with alternate exchange routing.

### Publish reliability recipe

1. Create/verify durable exchange and destination queue topology.
2. Bind them with the intended routing rule.
3. Set message persistence if using classic queues and restart survival matters.
4. Use an **appropriate confirm-enabled channel** and await/track confirms.
5. Use `mandatory` and/or alternate exchange for unroutable publishes.
6. Treat *ambiguous network outcomes* as potentially sent and potentially unsent.
7. Retry with a **stable `MessageId`** and ensure consumer-side idempotency.
8. For database write + publish, consider the **Transactional Outbox** pattern.

### Why confirms still cannot give you exactly-once processing

```text
1. Publisher sends message M123.
2. Broker stores M123 and sends confirm.
3. Network fails before publisher sees confirm.
4. Publisher does not know whether M123 arrived.
5. Publisher retries M123.
6. Consumer may process M123 twice.
```

This is normal distributed-system uncertainty. Don't assume `BasicPublishAsync` eliminates duplicates.

## 18. Consumer safety: auto ACK, manual ACK, NACK, reject, requeue

### The four decisions a consumer makes

| Outcome | Action | Broker effect |
|---|---|---|
| Processed successfully | `BasicAckAsync(tag, multiple: false)` | Settle/remove this delivery |
| Transient failure and safe to retry | `BasicNackAsync(tag, multiple: false, requeue: true)` | Requeue (or 4.3 quorum delayed retry if configured) |
| Permanent invalid message | `BasicRejectAsync(tag, requeue: false)` | Dead-letter if configured; otherwise discard |
| Consumer lost before settling | No ACK reaches broker | Broker can return/requeue delivery, with queue-specific behavior |

### `ACK` vs. `NACK` vs. `Reject`

- `ACK`: positive acknowledgment for a **delivery**, not a new publication.
- `NACK`: negative acknowledgment, with `multiple` to reject several outstanding tags at once and `requeue` to choose return vs removal/dead-letter.
- `Reject`: negative acknowledgment for **one** delivery; no `multiple` flag.
- **Delivery tag**: monotonically assigned on a channel; a tag from one channel is not valid on another.

### Why `multiple` is dangerous

```text
Unacked tags on one channel: 10, 11, 12, 13
ACK deliveryTag=13, multiple=true
  -> acknowledges all currently outstanding tags <= 13 on that channel
```

This is efficient for carefully controlled sequential processing but **dangerous** when callback processing completes out of order. Start with `multiple: false` until you explicitly implement safe batching.

### `redelivered` does not mean “this was processed twice”

The redelivery flag indicates the broker has delivered the message before or attempted to; it does not say whether the earlier consumer completed any business side effect. For duplicates, key your idempotency on **application message ID**, not on delivery tags.

### Failure classification

| Failure | Example | Suggested starting response |
|---|---|---|
| Transient | HTTP 503, temporary DB outage | Bounded retry/backoff (prefer broker features where appropriate) |
| Permanent | Invalid schema, impossible value | Reject and route to monitored DLQ |
| Unknown outcome | DB committed but connection lost pre-ACK | Redelivery and idempotency |
| Cancellation/shutdown | Host stopping | Stop taking new work, finish or safely leave work unacked |
| Poison message | Always crashes handler | Delivery limit + DLQ investigation |

**Do not** ACK first and then perform important side effects. With at-least-once delivery a more reasonable order is: **validate → idempotent business work → commit → ACK**.

## 19. Prefetch, concurrency, ordering, fairness, and backpressure

### Prefetch

Prefetch is the maximum number of deliveries RabbitMQ can send to a consumer without receiving acknowledgements (in typical per-consumer mode).

```text
prefetchCount = 3
Consumer receives: M1, M2, M3  -> all unacked
M4 waits until one outstanding delivery is ACKed
```

In RabbitMQ's usual per-consumer interpretation of `basic.qos` with `global: false`, the limit is applied **per consumer**, not pooled across an entire channel. `prefetchCount=0` means **unlimited** from the broker's prefetch perspective—often an unsafe default for slow workers.

Example in modern C#:

```csharp
await channel.BasicQosAsync(
    prefetchSize: 0,
    prefetchCount: 5,
    global: false);
```

| Parameter | Meaning |
|---|---|
| `prefetchSize: 0` | No size-based delivery window constraint; nonzero byte semantics aren't generally supported as a practical tuning knob |
| `prefetchCount: 5` | Up to five outstanding unacknowledged deliveries per consumer (typical RabbitMQ mode) |
| `global: false` | Consumer-scoped limit rather than channel-wide limit; preferred |

### Competing consumers do not guarantee strict fairness

A fast worker can process more messages than a slow worker. Prefetch influences how many messages sit unacked in each process. Round-robin descriptions are helpful for beginners but **not** an exact load-balancing guarantee in a real running system.

### Ordering

Queues are conceptually FIFO, but **end-to-end processing completion order** can change with:

- More than one consumer.
- `prefetchCount > 1` and concurrent handlers.
- Redeliveries and retries.
- Message priorities.
- Multiple publisher channels and different arrival times.

**Scenario:** M1 takes 10 seconds on worker A; M2 takes 1 second on worker B. Even if M1 was delivered first, **M2 completes first**.

If strict serial processing is required for some entity, consider single active consumer, partitioning by entity, or a domain-specific serialization strategy. Beware that single active consumer doesn't automatically make retries/order/side effects trivial.

### Single active consumer (SAC)

Use queue argument:

```json
{"x-single-active-consumer": true}
```

Only one consumer on that queue is active at a time; others wait to take over if needed. Good for workloads requiring controlled consumption, but it can limit throughput. It is **not** the same as an **exclusive** queue.

### Backpressure and bounded work

- Queue backlog = producers outpacing consumers.
- Prefetch bounds deliveries in-flight, **not** how fast producers publish.
- Length limits help bound queue growth, but overflow policies can drop or reject messages.
- Broker memory/disk alarms can block **publishing** connections.
- Worker concurrency must consider downstream database/API capacity, not just CPU cores.

Track `ready`, `unacked`, publish/ack rates, consumer counts, oldest-message age when available, resource alarms, and business-level processing latency. A low queue depth can still hide a stuck consumer if everything is `Unacked`.

## 20. TTL, queue expiration, maximum length, and overflow

### Message TTL (`x-message-ttl`)

Queue-level setting in **milliseconds**:

```json
{"x-message-ttl": 60000}
```

Messages can expire after 60 seconds of queue residence. Expired messages are **not guaranteed to disappear from every visible counter at exactly 60.000 seconds**; expiration and dead-lettering happen according to queue implementation/scanning. TTL is not a precise task scheduler.

Per-message AMQP 0-9-1 property (a **string**, in milliseconds):

```csharp
var props = new BasicProperties
{
    Expiration = "15000" // 15 seconds
};
```

If both queue-level and per-message TTL apply, the **shorter** relevant expiry determines when a message expires. TTL isn't a way to interrupt a handler already processing a delivery.

### Queue TTL / `x-expires`

```json
{"x-expires": 3600000}
```

Deletes an **unused** queue after a period of inactivity. “Unused” has RabbitMQ-specific conditions; it is *not* “delete if empty for one hour.” Use it for temporary queues whose lifecycle should be automatically bounded.

### `x-max-length` / `x-max-length-bytes`

These limit backlog by **count** or **bytes**. Queue-type semantics and in-flight deliveries affect the practical limit. Do not treat maximum length as a substitute for capacity planning.

```json
{
  "x-max-length": 10000,
  "x-overflow": "reject-publish"
}
```

### Overflow actions

| Mode | High-level behavior | Warning |
|---|---|---|
| `drop-head` | Discard/dead-letter older ready messages to make space | Oldest data can disappear |
| `reject-publish` | Reject new incoming publications that cannot be accepted | Publishers need confirms and a failure strategy |
| `reject-publish-dlx` | Classic queue-specific option | Not supported by quorum queues |

These policies have queue-specific trade-offs; re-read the [queue length guide](https://www.rabbitmq.com/docs/maxlength) before using a hard limit in production. For quorum queues, even `reject-publish` may overshoot a strict numerical limit briefly while publishes are in flight.

### When TTL surprises developers

Suppose an email is queued with a one-minute TTL while a worker is offline for ten minutes. The message may expire and go to the DLQ (if configured) without being sent. That can be correct for **time-sensitive OTPs** and completely wrong for **invoices that must always be processed**. TTL is a *business decision* as much as a broker option.

## 21. Dead-letter exchanges, poison messages, and retry patterns

### What is a DLX?

A **Dead Letter Exchange** is an ordinary exchange chosen by queue policy or declaration arguments as the destination for dead-lettered messages.

A **Dead Letter Queue** is a queue bound to that exchange. `DLX` is a routing mechanism; `DLQ` is a destination queue. A queue does not magically become a DLQ just because its name ends with `.dlq`.

```text
                   normal flow
Producer -> jobs.direct -> jobs.q -> Consumer
                          |
                          | reject(false), TTL, length limit,
                          | quorum delivery limit, etc.
                          v
                      jobs.dlx
                          |
                          v
                      jobs.dlq
```

Common dead-letter reasons include `rejected`, `expired`, `maxlen`, and quorum `delivery_limit`.

### DLX policy vs. hardcoded arguments

Two ways to configure source queue `jobs.q`:

**Queue arguments** (easy to learn, harder to change):

```json
{
  "x-dead-letter-exchange": "jobs.dlx",
  "x-dead-letter-routing-key": "jobs.failed"
}
```

**Policy** (recommended for operations):

```powershell
docker exec rabbitmq-local rabbitmqctl set_policy -p learning jobs-dlx '^jobs\.' '{"dead-letter-exchange":"jobs.dlx","dead-letter-routing-key":"jobs.failed"}' --apply-to queues
```

Policy changes can target multiple queues without redeploying code. Note that explicit client `x-arguments` can take precedence over ordinary policies, while operator policies can enforce guardrails. Create the DLX **and bindings** before expecting successful dead-letter routing.

### `x-death` and failure history

RabbitMQ adds an `x-death` header to dead-lettered AMQP 0-9-1 messages. It records compressed history by source queue and reason, with counts and other metadata. This is useful for diagnostics, but **don't assume every retry or every consumer failure produces an `x-death` entry**. A simple `BasicNack(requeue: true)` isn't by itself a dead-letter event.

### Poison messages and bounded retries

A **poison message** consistently fails processing, perhaps because it has invalid data or triggers a bug.

**Bad design:**

```text
Consumer fails -> BasicNack(requeue=true) -> immediately re-deliver -> fail -> repeat forever
```

Consequences: CPU consumption, log floods, repeated API calls, backlog starvation.

**Better design:** bounded attempts, increasing delay where supported, meaningful failure reason, and monitored dead-letter destination.

### Three retry approaches (compare carefully)

**A. Immediate requeue** — simplest, but potentially dangerous:

```text
jobs.q -> consumer fails -> requeue -> jobs.q
```

Use only where retry quickly is rational or broker-level delayed retry is configured.

**B. Retry queue + TTL + DLX** — conventional compatibility approach:

```text
jobs.q -> consumer publishes copy to jobs.retry.q -> TTL expires
                                          |             |
                                          |             v
                                          +-------- jobs.direct -> jobs.q
```

**Critical reliability warning:** If the consumer publishes a retry copy and then ACKs the original, **crash timing can lose or duplicate a job**. Confirm the new publish before ACKing the original; design idempotent consumers. A misconfigured TTL/DLX loop can amplify messages, and TTL is not a precise timer.

**C. RabbitMQ 4.3 quorum native delayed retry** — modern option:

```text
quorum jobs.q -> consumer NACK/REJECT with requeue as appropriate
             -> broker defers message internally
             -> eligible for redelivery after configured delay
```

This avoids the separate retry-queue re-publication hop for supported **quorum queue** return scenarios. Details in section 22.

### Dead-letter transfer reliability

**By default**, dead-letter republishing does **not** guarantee at-least-once transfer to the target queue if its route/storage is unavailable. Quorum queues can enable **at-least-once dead lettering**, with specific policies and overflow constraints. This is a separate guarantee from publisher confirms in application code.

### DLQ operational workflow

1. Alert when `jobs.dlq` grows or `delivery_limit` spikes.
2. Inspect a **copy** of the payload and headers safely (avoid dumping secrets/PII).
3. Find the underlying exception and classify permanent vs. transient.
4. Fix code, external dependency, or bad event schema.
5. Reprocess deliberately through a controlled replay tool with idempotency.
6. Monitor for repeat failures; never blindly auto-requeue the entire DLQ forever.

## 22. New in RabbitMQ 4.3: delayed retry, priority, consumer timeouts

These features are **not accurately taught by many older courses**. They are a major reason this README targets 4.3 specifically.

### 22.1 Quorum native delayed retry (new in 4.3)

Supported on **quorum queues**, configured with **policy keys**:

```text
delayed-retry-type = all
delayed-retry-min  = 1000       # milliseconds
delayed-retry-max  = 30000      # milliseconds
```

Or equivalent queue arguments:

```json
{
  "x-queue-type": "quorum",
  "x-delayed-retry-type": "all",
  "x-delayed-retry-min": 1000,
  "x-delayed-retry-max": 30000
}
```

`delayed-retry-type` options:

| Value | Meaning |
|---|---|
| `disabled` | No native delay (default) |
| `all` | Delay returned messages, whether or not failure count incremented |
| `failed` | Delay returns classified as failures, incrementing delivery count |
| `returned` | Delay returns not counted as failures (e.g., normal NACK return cases) |

**Delay formula (linear backoff):**

```text
delay = min(delayedRetryMin * deliveryCount, delayedRetryMax)
```

**Subtle point:** not every return increments the broker's `delivery-count`, so not every NACK necessarily increases the backoff to the next step. The chosen retry type matters.

**Example policy for queues whose names start with `qq.`:**

```powershell
docker exec rabbitmq-local rabbitmqctl set_policy -p learning qq-retry '^qq\.' '{"delayed-retry-type":"all","delayed-retry-min":1000,"delayed-retry-max":30000}' --apply-to quorum_queues
```

**Scenario:** Worker calls a third-party document conversion API. API returns 503. Instead of NACKing and causing an immediate busy loop, use quorum delayed retry to defer the next delivery. This is *not* a broker-level arbitrary job scheduling system; it's for failed/returned deliveries.

### 22.2 Strict message priorities for quorum queues (new in 4.3)

RabbitMQ 4.3 quorum queues support **32 strict priority levels, `0`–`31`**, enabled automatically. **Higher priority messages are dispatched before lower priority messages** when waiting in the queue, subject to prefetch, previously delivered messages, and returned-message rules.

Important facts:

- `x-max-priority` is a **classic queue** argument and is **ignored by quorum**.
- Quorum messages without explicit priority are treated as **priority 4**; classic default is **0**.
- Quorum priority values outside 0–31 are clamped to the supported range.
- Returned/redelivered messages have special sequencing rules and need not preserve their original priority in redelivery order.
- Priority cannot interrupt a delivery already handed to a consumer.

**Scenario:** a high-priority fraud alert is published while one worker is already processing two low-priority jobs. Priority influences which **queued** message is dispatched next, not the work already running. Start the priority demo with the consumer stopped to make the effect visible.

### 22.3 Quorum consumer acknowledgement timeout changes (new in 4.3)

RabbitMQ 4.3 moved consumer acknowledgement timeout handling to **quorum queues** (not classic queues/streams). On timeout, RabbitMQ returns affected outstanding deliveries and can cancel the timed-out consumer via `basic.cancel` for capable AMQP 0-9-1 clients, instead of always closing the whole channel as in older designs.

Configuration precedence (highest first):

1. `x-consumer-timeout` in consumer argument, when applicable.
2. `x-consumer-timeout` queue argument.
3. `consumer-timeout` queue policy.
4. `consumer_timeout` in `rabbitmq.conf` (default 1,800,000 ms = 30 minutes).

**Scenario:** a worker goes unresponsive for 45 minutes while holding a message. Timeout can make the message available to another worker. But **the first worker may still have performed part of its business action**, so timeout behavior does not remove the need for idempotency.

### 22.4 4.3 transient queue compatibility

Older courses sometimes declare `durable=false, exclusive=false, autoDelete=false`. In RabbitMQ 4.3 this **deprecated, non-exclusive transient classic queue form is disabled by default**. Use a durable queue for teaching shared work; use a non-durable **exclusive** queue for temporary connection-owned replies.

### 22.5 Metadata storage and protocol evolution

RabbitMQ 4.3 uses **Khepri** as its metadata store, changing how internal metadata is maintained. AMQP 1.0 also continues evolving; a tutorial using AMQP 0-9-1 .NET code should not assume every AMQP 1.0 feature exists in that client API. Read the [4.3 release highlights](https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release) for protocol/version-specific features.

## 23. Replication, clustering, failover, and what durability cannot promise

### Single node vs. cluster

```text
Local training                 Production-like HA example
+---------------+             +-------+ +-------+ +-------+
| RabbitMQ A    |             |Node A | |Node B | |Node C |
| single disk   |             +-------+ +-------+ +-------+
+---------------+                   quorum queue replicas
```

A 3-node RabbitMQ cluster is not automatically safe for every queue: **classic queues are non-replicated**; quorum queues are replicated according to membership/configuration; streams have their own replication/retention model. Network partition handling and majority availability matter.

### Publisher confirms and quorum safety

When publishing to a healthy replicated quorum queue, confirms are tied to the message being sufficiently replicated/committed under the queue's safety model. This is much stronger than treating a successful local socket write as proof of durability, but **not** a promise that all copies can withstand any number of simultaneous failures.

### Design failure drills

1. **Stop the only broker node:** consumers disconnect; delivery is unavailable until it recovers.
2. **Restart with the volume intact:** durable topology and persistent data should recover subject to delivery state, policies, and normal storage conditions.
3. **Delete its data volume:** even persistent local data disappears.
4. **Take one node down in a healthy 3-member quorum:** majority remains; availability may continue after election/recovery.
5. **Take two members down:** majority is lost; the quorum queue cannot normally accept/serve operations that require quorum.
6. **Cut publisher network mid-confirm:** outcome is ambiguous; safe retry may create a duplicate.

### “At least once” vs. “at most once” vs. “exactly once”

| Semantics | What you trade | Application requirement |
|---|---|---|
| At most once | Some messages can be lost; no broker redelivery guarantee | Accept drops, e.g., certain telemetry |
| At least once | Unacked messages can be redelivered; duplicates possible | Idempotent handlers |
| Exactly once business effect | Harder than a broker setting | Stable IDs, database uniqueness/transactions, outbox/inbox/business design |

**Do not confuse producer publisher confirms, consumer acknowledgements, and a transaction in another database.** They are three separate boundaries.

---
# Part IV — Modern .NET, real designs, operations, and laboratories

## 24. Modern .NET client 7.x migration and API map

Older RabbitMQ tutorials frequently use `RabbitMQ.Client` 5.x or 6.x. RabbitMQ.Client **7.2.2** has a different, asynchronous-first API. **Do not “fix” old samples by changing only method names without understanding semantics.**

### Old versus modern API

| Older training code | RabbitMQ.Client 7.x approach | Meaning |
|---|---|---|
| `factory.CreateConnection()` | `await factory.CreateConnectionAsync()` | Open AMQP connection asynchronously |
| `connection.CreateModel()` | `await connection.CreateChannelAsync()` | Create logical AMQP channel |
| `IModel` | `IChannel` | Channel abstraction renamed |
| `channel.ExchangeDeclare(...)` | `await channel.ExchangeDeclareAsync(...)` | Declare exchange |
| `channel.QueueDeclare(...)` | `await channel.QueueDeclareAsync(...)` | Declare queue |
| `channel.QueueBind(...)` | `await channel.QueueBindAsync(...)` | Add routing binding |
| `channel.BasicPublish(...)` | `await channel.BasicPublishAsync(...)` | Publish message |
| `channel.BasicConsume(...)` | `await channel.BasicConsumeAsync(...)` | Start subscription |
| `EventingBasicConsumer` with `Received` | `AsyncEventingBasicConsumer` with `ReceivedAsync` | Asynchronous consumer callback |
| `channel.BasicAck(...)` | `await channel.BasicAckAsync(...)` | Settle delivery |
| `channel.BasicNack(...)` | `await channel.BasicNackAsync(...)` | Negative acknowledgment |
| `channel.BasicReject(...)` | `await channel.BasicRejectAsync(...)` | Reject one delivery |
| `channel.BasicQos(...)` | `await channel.BasicQosAsync(...)` | Prefetch |
| `channel.CreateBasicProperties()` | `new BasicProperties()` | Construct message properties |
| `channel.ConfirmSelect()` | `CreateChannelOptions(publisherConfirmationsEnabled: true, ...)` | Enable publisher confirms when channel created |
| `WaitForConfirmsOrDie()` | Await/track confirmed `BasicPublishAsync` as configured | Publisher confirmation handling model |
| `ea.Body` as byte array | `ea.Body` as `ReadOnlyMemory<byte>` | Borrowed memory ownership matters |

### Critical client 7.x memory safety

The delivery body is a `ReadOnlyMemory<byte>` from a pooled buffer. **Copy it before holding it beyond the delivery handler lifetime.** A safe approach for a small UTF-8 message is:

```csharp
var bodyCopy = ea.Body.ToArray();
var json = Encoding.UTF8.GetString(bodyCopy);
```

You can process the memory synchronously/within the callback as allowed by the API, but should not store `ea.Body` in a background queue that outlives the callback unless you copy it. Otherwise you risk later observing invalid or reused memory.

### Why async-first APIs matter

- `CreateConnectionAsync()` avoids blocking a request thread while network setup occurs.
- `BasicPublishAsync()` is an async transport operation. **It only awaits broker confirmation when publisher confirmation/tracking has been enabled appropriately.**
- `ReceivedAsync` accepts an asynchronous handler; `async void` event handlers are inappropriate here.
- Cancellation tokens control relevant local waits/operations; they **cannot retract** a publish already accepted by RabbitMQ.
- Cancellation does not guarantee rollback of any side effects already committed by a consumer.

### Modern `ConnectionFactory` options

```csharp
var factory = new ConnectionFactory
{
    HostName = "localhost",         // Host where broker can be reached
    Port = 5672,                   // Plain AMQP 0-9-1 port for local lab
    UserName = "developer",        // Username created at broker initialization
    Password = "<from-config>",    // Do not hard-code secrets in real projects
    VirtualHost = "learning",      // Must match the configured vhost
    AutomaticRecoveryEnabled = true,
    TopologyRecoveryEnabled = true,
    RequestedHeartbeat = TimeSpan.FromSeconds(30),
    NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
    ClientProvidedName = "rabbitmq-learning-producer"
};
```

| Setting | What it does | Important caution |
|---|---|---|
| `HostName` | Broker DNS name | In another Compose container use `rabbitmq`, not Windows `localhost` |
| `Port` | AMQP listener port | Management UI uses **15672**, not 5672 |
| `UserName`, `Password` | Credentials | Use protected configuration/secret manager in production |
| `VirtualHost` | Logical namespace | User must have permissions on it |
| `AutomaticRecoveryEnabled` | Attempt connection recovery after failures | Does not make publish loss impossible |
| `TopologyRecoveryEnabled` | Recover recorded exchanges/queues/bindings/consumers | Can interact with topology changes; test recovery behavior |
| `RequestedHeartbeat` | Proposed heartbeat interval | Negotiated; detects dead connection, not instantaneous |
| `NetworkRecoveryInterval` | Delay between recovery attempts | Avoid overly aggressive reconnection storms |
| `ClientProvidedName` | Human-friendly connection name in UI | Useful for diagnosing connections |

These are illustrative choices, not universal production defaults.

## 25. A small complete .NET 7.x producer and consumer

This section provides **minimal runnable learning applications**, not a complete microservices template. We intentionally use a durable **classic queue** first because the architecture is easy to inspect. Later replace the topology with a quorum queue **under a different name** to learn the difference.

### 25.1 Create solution and projects

```powershell
dotnet new sln -n RabbitMQLearning
dotnet new console -n RabbitMQ.Producer -f net8.0
dotnet new console -n RabbitMQ.Consumer -f net8.0

dotnet sln RabbitMQLearning.sln add RabbitMQ.Producer/RabbitMQ.Producer.csproj
dotnet sln RabbitMQLearning.sln add RabbitMQ.Consumer/RabbitMQ.Consumer.csproj

dotnet add RabbitMQ.Producer/RabbitMQ.Producer.csproj package RabbitMQ.Client --version 7.2.2
dotnet add RabbitMQ.Consumer/RabbitMQ.Consumer.csproj package RabbitMQ.Client --version 7.2.2
```

You can also target a newer installed .NET SDK. `net8.0` is used here as a broadly compatible baseline for RabbitMQ.Client 7.x; it is not a statement that .NET 8 is the newest SDK.

Set environment variables in the terminal, or load secure development configuration:

```powershell
$env:RABBITMQ_HOST = "localhost"
$env:RABBITMQ_USER = "developer"
$env:RABBITMQ_PASSWORD = "change-this-local-password"
$env:RABBITMQ_VHOST = "learning"
```

**Reminder:** these PowerShell variables must match your `.env` values used to initialize RabbitMQ. Compose's `.env` does *not* automatically populate the PowerShell process that launches a console application.

### 25.2 Producer: complete `Program.cs`

```csharp
using System.Text;
using RabbitMQ.Client;

const string exchangeName = "learning.jobs.direct";
const string queueName = "learning.jobs.q";
const string routingKey = "jobs.generate";

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "developer",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD")
        ?? throw new InvalidOperationException("RABBITMQ_PASSWORD must be set."),
    VirtualHost = Environment.GetEnvironmentVariable("RABBITMQ_VHOST") ?? "learning",
    ClientProvidedName = "learning-producer"
};

await using var connection = await factory.CreateConnectionAsync();

var channelOptions = new CreateChannelOptions(
    publisherConfirmationsEnabled: true,
    publisherConfirmationTrackingEnabled: true);

await using var channel = await connection.CreateChannelAsync(channelOptions);

await channel.ExchangeDeclareAsync(
    exchange: exchangeName,
    type: ExchangeType.Direct,
    durable: true,
    autoDelete: false);

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

await channel.QueueBindAsync(
    queue: queueName,
    exchange: exchangeName,
    routingKey: routingKey);

var messageId = Guid.NewGuid().ToString("N");
var payload = $"{{\"messageId\":\"{messageId}\",\"task\":\"generate-report\"}}";
var body = Encoding.UTF8.GetBytes(payload);

var properties = new BasicProperties
{
    MessageId = messageId,
    ContentType = "application/json",
    ContentEncoding = "utf-8",
    DeliveryMode = 2,
    Type = "ReportRequested"
};

await channel.BasicPublishAsync(
    exchange: exchangeName,
    routingKey: routingKey,
    mandatory: true,
    basicProperties: properties,
    body: body);

Console.WriteLine($"Published and confirmed message {messageId}");
```

**Code walkthrough (do not skip):**

1. `CreateConnectionAsync()` establishes network/authentication/session state.
2. `CreateChannelOptions` enables publisher confirms **and tracking**. With this configuration, awaiting the publish can await confirmation; failures should be caught in real applications.
3. `ExchangeDeclareAsync()` creates/checks a durable **direct exchange**.
4. `QueueDeclareAsync()` creates/checks a durable, shared classic queue (the vhost's default type is typically classic).
5. `QueueBindAsync()` associates routing key `jobs.generate` with the queue.
6. `MessageId` identifies the *logical event* for idempotency and tracing.
7. `ContentType` and `ContentEncoding` describe how to interpret message bytes.
8. `DeliveryMode = 2` requests message persistence for classic-queue recovery.
9. `mandatory: true` surfaces otherwise-unroutable messages through the returns/confirmation path.
10. `BasicPublishAsync(...)` sends the publication to an **exchange** (not straight to a queue).
11. `Console.WriteLine` means broker publish completed under this confirm configuration; it does **not** mean the report was generated.

### 25.3 Consumer: complete `Program.cs`

```csharp
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string exchangeName = "learning.jobs.direct";
const string queueName = "learning.jobs.q";
const string routingKey = "jobs.generate";

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "developer",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD")
        ?? throw new InvalidOperationException("RABBITMQ_PASSWORD must be set."),
    VirtualHost = Environment.GetEnvironmentVariable("RABBITMQ_VHOST") ?? "learning",
    ClientProvidedName = "learning-consumer"
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync(
    exchange: exchangeName,
    type: ExchangeType.Direct,
    durable: true,
    autoDelete: false);

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

await channel.QueueBindAsync(
    queue: queueName,
    exchange: exchangeName,
    routingKey: routingKey);

await channel.BasicQosAsync(
    prefetchSize: 0,
    prefetchCount: 5,
    global: false);

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (_, ea) =>
{
    // RabbitMQ.Client 7.x body buffers are borrowed; copy for ownership safety.
    byte[] body = ea.Body.ToArray();
    string json = Encoding.UTF8.GetString(body);

    try
    {
        Console.WriteLine($"Received (redelivered={ea.Redelivered}): {json}");

        // Demonstration only: stand-in for your actual business operation.
        await Task.Delay(TimeSpan.FromSeconds(1));

        // ACK only after successful processing.
        await channel.BasicAckAsync(
            deliveryTag: ea.DeliveryTag,
            multiple: false);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Processing failed: {ex.Message}");

        // Learning choice: requeue this message. Do NOT do this forever
        // for permanent failures in a real system.
        await channel.BasicNackAsync(
            deliveryTag: ea.DeliveryTag,
            multiple: false,
            requeue: true);
    }
};

string consumerTag = await channel.BasicConsumeAsync(
    queue: queueName,
    autoAck: false,
    consumer: consumer);

Console.WriteLine($"Consumer {consumerTag} running. Press ENTER to stop.");
Console.ReadLine();

await channel.BasicCancelAsync(consumerTag);
```

**Why this consumer is intentionally basic:**

- The connection/channel stay alive until user input.
- The consumer is **push-based**; it doesn't busy-poll the queue.
- With `autoAck: false`, the broker tracks `Unacked` deliveries.
- `BasicQosAsync(... prefetchCount: 5 ...)` bounds broker dispatch to this consumer.
- `ea.Body.ToArray()` avoids keeping borrowed memory across asynchronous code.
- ACK uses `ea.DeliveryTag` on the **receiving channel**.
- For learning, transient errors are requeued. Production needs failure classification, bounded retry, cancellation, idempotency, and a DLQ.
- The sample uses the default sequential consumer dispatch. If you later configure concurrent dispatch, synchronization/ACK ordering require further design.

**Production caveat:** A robust `BackgroundService` should coordinate graceful shutdown and allow in-flight handlers to finish or be redelivered; `Console.ReadLine()` is only for this console exercise. Add exception categorization and avoid requeueing permanent failures forever.

### 25.4 Run the example

In **Terminal A**:

```powershell
dotnet run --project RabbitMQ.Consumer
```

In **Terminal B**:

```powershell
dotnet run --project RabbitMQ.Producer
```

Observe:

- UI → **Exchanges**: `learning.jobs.direct` is a direct exchange.
- UI → **Queues and Streams**: `learning.jobs.q` exists.
- With the consumer stopped, publishes increase `Ready`.
- With the consumer running, messages move through `Unacked` and leave after ACK.
- Publishing to a *non-matching* routing key with `mandatory: true` triggers unroutable handling rather than silently succeeding under the tracked confirm path.

### 25.5 Method/parameter reference

#### `ExchangeDeclareAsync`

```csharp
await channel.ExchangeDeclareAsync(
    exchange: "examples.direct",
    type: ExchangeType.Direct,
    durable: true,
    autoDelete: false,
    arguments: null);
```

| Parameter | Example | Meaning |
|---|---|---|
| `exchange` | `examples.direct` | Exchange name within vhost |
| `type` | `ExchangeType.Direct` | Routing algorithm (direct/fanout/topic/headers) |
| `durable` | `true` | Recover exchange metadata after restart |
| `autoDelete` | `false` | If true, delete after last binding disappears (once bound) |
| `arguments` | `null` | Optional feature args (e.g., alternate exchange) |

#### `QueueDeclareAsync`

```csharp
await channel.QueueDeclareAsync(
    queue: "examples.jobs",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        ["x-queue-type"] = "quorum"
    });
```

| Parameter | Meaning |
|---|---|
| `queue` | Broker-visible name; empty names request server-named queue only where supported |
| `durable` | Persist topology |
| `exclusive` | Single-connection ownership for supported queue types |
| `autoDelete` | Remove after last consumer disappears, if it was ever consumed |
| `arguments` | Queue type, TTL, DLX, SAC, limits, etc. |

Do **not** run that quorum declaration against an existing classic queue of the same name; RabbitMQ will reject incompatible declarations.

#### `QueueBindAsync`

```csharp
await channel.QueueBindAsync(
    queue: "examples.jobs",
    exchange: "examples.direct",
    routingKey: "reports.generate",
    arguments: null);
```

- `queue`: destination queue.
- `exchange`: source exchange.
- `routingKey`: exact string for direct; wildcard pattern for topic; ignored by fanout.
- `arguments`: binding-specific metadata (important for headers exchange).

#### `BasicPublishAsync`

```csharp
await channel.BasicPublishAsync(
    exchange: "examples.direct",
    routingKey: "reports.generate",
    mandatory: true,
    basicProperties: new BasicProperties { DeliveryMode = 2 },
    body: Encoding.UTF8.GetBytes("Hello"));
```

- `exchange`: destination routing entity; `""` selects the default exchange.
- `routingKey`: message's routing key, **not** necessarily a queue name.
- `mandatory`: request return if unroutable.
- `basicProperties`: content type, message ID, headers, persistence, TTL, priority, etc.
- `body`: binary payload (`ReadOnlyMemory<byte>`-compatible input); can encode UTF-8, JSON, Protobuf, etc.
- `CancellationToken` overloads: cancel relevant client wait/operation; a cancelled wait does **not** prove the broker never accepted the message.

#### `BasicConsumeAsync`

```csharp
await channel.BasicConsumeAsync(
    queue: "examples.jobs",
    autoAck: false,
    consumer: consumer);
```

- `queue`: queue to subscribe to.
- `autoAck`: false means application controls acknowledgement.
- `consumer`: callback implementation receiving deliveries.
- Returned string is **consumer tag**, an identifier you can use to cancel the subscription.

#### Acknowledgment APIs

```csharp
await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
await channel.BasicRejectAsync(ea.DeliveryTag, requeue: false);
```

In each call the delivery tag belongs to the **same channel** that delivered it. `requeue: false` without a DLX can **discard the message**.

## 26. C# recipes for routing, properties, acknowledgment, and policies

These recipes **assume an open `IChannel channel`**, appropriate declared destination queues, and `using RabbitMQ.Client; using System.Text;`. They demonstrate a specific concept, not a complete standalone app each time.

### Recipe 1 — Default exchange to queue by name

```csharp
await channel.BasicPublishAsync(
    exchange: string.Empty,
    routingKey: "jobs.q", // the queue name due to the default exchange's automatic binding
    mandatory: true,
    basicProperties: new BasicProperties { DeliveryMode = 2 },
    body: Encoding.UTF8.GetBytes("job-1"));
```

### Recipe 2 — Direct exchange: choose destination category

```csharp
await channel.ExchangeDeclareAsync("alerts.direct", ExchangeType.Direct, durable: true);
await channel.QueueDeclareAsync("alerts.critical.q", durable: true, exclusive: false, autoDelete: false);
await channel.QueueBindAsync("alerts.critical.q", "alerts.direct", "critical");

await channel.BasicPublishAsync(
    exchange: "alerts.direct",
    routingKey: "critical",
    mandatory: true,
    basicProperties: new BasicProperties { DeliveryMode = 2 },
    body: Encoding.UTF8.GetBytes("Check service availability"));
```

The publish reaches every queue bound under **exactly** `critical`, not every queue in the vhost.

### Recipe 3 — Fanout: independent copies

```csharp
await channel.ExchangeDeclareAsync("news.fanout", ExchangeType.Fanout, durable: true);
await channel.QueueDeclareAsync("news.email.q", true, false, false);
await channel.QueueDeclareAsync("news.mobile.q", true, false, false);
await channel.QueueBindAsync("news.email.q", "news.fanout", string.Empty);
await channel.QueueBindAsync("news.mobile.q", "news.fanout", string.Empty);

await channel.BasicPublishAsync(
    exchange: "news.fanout",
    routingKey: "ignored",
    mandatory: true,
    basicProperties: new BasicProperties { DeliveryMode = 2 },
    body: Encoding.UTF8.GetBytes("New feature released"));
```

A single publish produces one ready message in **each** matching queue, assuming no consumers or other removals.

### Recipe 4 — Topic: subscribe to a pattern

```csharp
await channel.ExchangeDeclareAsync("shop.topic", ExchangeType.Topic, durable: true);
await channel.QueueDeclareAsync("shop.orders.q", true, false, false);
await channel.QueueBindAsync("shop.orders.q", "shop.topic", "order.*");

await channel.BasicPublishAsync(
    exchange: "shop.topic",
    routingKey: "order.created",   // matches order.*
    mandatory: true,
    basicProperties: new BasicProperties { DeliveryMode = 2 },
    body: Encoding.UTF8.GetBytes("order 123 created"));
```

Try `order.eu.created` afterwards. It has **three** segments and will not match `order.*`.

### Recipe 5 — Headers exchange and `x-match`

```csharp
await channel.ExchangeDeclareAsync("files.headers", ExchangeType.Headers, durable: true);
await channel.QueueDeclareAsync("files.eu.pdf.q", true, false, false);

await channel.QueueBindAsync(
    queue: "files.eu.pdf.q",
    exchange: "files.headers",
    routingKey: string.Empty,
    arguments: new Dictionary<string, object?>
    {
        ["x-match"] = "all",
        ["format"] = "pdf",
        ["region"] = "eu"
    });

var properties = new BasicProperties
{
    DeliveryMode = 2,
    Headers = new Dictionary<string, object?>
    {
        ["format"] = "pdf",
        ["region"] = "eu"
    }
};

await channel.BasicPublishAsync(
    exchange: "files.headers",
    routingKey: "ignored",
    mandatory: true,
    basicProperties: properties,
    body: Encoding.UTF8.GetBytes("doc-204"));
```

**Important:** When testing headers, broker/client encoding and AMQP value types matter. Keep matching values consistent; use proper AMQP-serializable primitive values.

### Recipe 6 — Quorum queue with dead lettering

Create `events.dlx` and bind `events.failed.q` **first**. Then:

```csharp
await channel.QueueDeclareAsync(
    queue: "events.critical.qq",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        ["x-queue-type"] = "quorum",
        ["x-dead-letter-exchange"] = "events.dlx",
        ["x-dead-letter-routing-key"] = "events.failed"
    });
```

For maintainability, move adjustable dead-letter routing settings to a broker **policy** in a real application.

### Recipe 7 — Message metadata for tracing

```csharp
var properties = new BasicProperties
{
    MessageId = Guid.NewGuid().ToString("N"),
    CorrelationId = "request-42",
    Type = "InvoiceCreated.v1",
    ContentType = "application/json",
    ContentEncoding = "utf-8",
    DeliveryMode = 2,
    Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
    Headers = new Dictionary<string, object?>
    {
        ["source"] = "billing-service",
        ["schema-version"] = 1
    }
};
```

- `MessageId`: stable identity of one logical publication, useful for dedupe.
- `CorrelationId`: related request/workflow identity.
- `Type`: application-level type or schema/version marker.
- `ContentType`: e.g., JSON vs. protobuf.
- `ContentEncoding`: representation encoding; not an agreement about the schema.
- `Timestamp`: producer-provided metadata, **not** guaranteed server receive time.
- `Headers`: supplementary metadata; avoid sensitive details and excessively large header maps.

### Recipe 8 — Classic queue priority

```csharp
await channel.QueueDeclareAsync(
    queue: "classic.priority.q",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        ["x-max-priority"] = 5
    });

var highPriority = new BasicProperties
{
    DeliveryMode = 2,
    Priority = 5
};
```

Classic priority queues need `x-max-priority` at declaration; don't assume this exists on every queue type or that it can reorder messages already delivered.

### Recipe 9 — Quorum priority in 4.3

```csharp
var urgent = new BasicProperties
{
    Priority = 31,       // quorum supports 0..31 automatically
    MessageId = Guid.NewGuid().ToString("N")
};
```

Quorum message priorities are supported without `x-max-priority` in 4.3. Remember the quorum default priority is **4**.

### Recipe 10 — Requeue vs. dead-letter on failure

```csharp
try
{
    await HandleBusinessMessageAsync(payload, cancellationToken);
    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
}
catch (TemporaryDependencyException)
{
    await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
}
catch (InvalidMessageException)
{
    await channel.BasicRejectAsync(ea.DeliveryTag, requeue: false);
}
```

`HandleBusinessMessageAsync`, `TemporaryDependencyException`, and `InvalidMessageException` are **application-defined placeholders**, not built-in RabbitMQ classes. The important teaching point is the **decision**, not these example names.

### Recipe 11 — Queue TTL, per-message TTL, and expiration

```csharp
await channel.QueueDeclareAsync(
    queue: "otp.q",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        ["x-message-ttl"] = 60000 // every message in queue: 60 seconds
    });

var props = new BasicProperties
{
    Expiration = "15000"  // this message: 15 seconds
};
```

An OTP arriving after its business expiry should **also** be rejected in the consumer/application; don't rely on queue expiration as your only security control.

### Recipe 12 — `mandatory` for unroutable messages

```csharp
// With confirm tracking enabled for this publishing channel:
await channel.BasicPublishAsync(
    exchange: "orders.direct",
    routingKey: "routing.key.with.no.binding",
    mandatory: true,
    basicProperties: new BasicProperties(),
    body: Encoding.UTF8.GetBytes("test"));
```

Observe returned/unroutable behavior. **Without the appropriate publisher-confirm/return handling**, `await` should not be interpreted as proof of successful routing. Production publishers should explicitly handle this failure mode.

### Recipe 13 — Topology equivalence (why redeclare can fail)

```csharp
// First run:
await channel.ExchangeDeclareAsync("app.events", ExchangeType.Direct, durable: true);

// Later, a different app incorrectly tries:
// await channel.ExchangeDeclareAsync("app.events", ExchangeType.Fanout, durable: true);
// -> PRECONDITION_FAILED: the existing exchange type is not equivalent.
```

When you hit a `PRECONDITION_FAILED` channel closure, inspect the existing entity in the UI before deleting any production topology. This error protects you from conflicting declarations.

### Recipe 14 — Configure delayed retry through a policy (RabbitMQ 4.3)

```powershell
docker exec rabbitmq-local rabbitmqctl set_policy -p learning qq-delayed '^qq\.' '{"delayed-retry-type":"all","delayed-retry-min":2000,"delayed-retry-max":20000}' --apply-to quorum_queues
```

To demonstrate it, create a **quorum** queue with a name starting `qq.`, publish, and NACK the delivery with `requeue: true`. Observe the delayed queue count/status in the Management UI. A policy does not apply if the queue type/pattern doesn't match.

### Recipe 15 — Safe `ReadOnlyMemory<byte>` usage in a consumer

```csharp
consumer.ReceivedAsync += async (_, ea) =>
{
    // Safe even if processing continues asynchronously:
    byte[] ownedBody = ea.Body.ToArray();

    await ProcessOwnedBytesAsync(ownedBody); // application-defined method
    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};
```

The copy is especially important before storing payloads in another work queue or capturing them for later execution. The `ReadOnlyMemory<byte>` from the callback is not an indefinitely owned `byte[]`.

## 27. Message contracts, versioning, correlation, idempotency, and outbox

### A practical JSON event contract

```json
{
  "messageId": "324884b5-d1bd-4d9f-8a75-b44aa6f7a8bc",
  "eventType": "OrderPlaced",
  "schemaVersion": 1,
  "occurredAtUtc": "2026-10-11T10:00:00Z",
  "correlationId": "req-991",
  "data": {
    "orderId": "ord-5001",
    "customerId": "cust-204",
    "total": 49.90,
    "currency": "USD"
  }
}
```

**Business rules:**

- `messageId`: stable across retries of the same logical event.
- `eventType`: semantic event type; choose descriptive past-tense names for facts.
- `schemaVersion`: enables independent evolution; do not break old consumers without a migration plan.
- `occurredAtUtc`: event time, not necessarily broker receive time.
- `correlationId`: trace a workflow across HTTP, DB, and messaging.
- `data`: smallest useful business payload; do not indiscriminately expose sensitive data.

### Idempotent consumer example (conceptual SQL)

Suppose a consumer receives the same `OrderPlaced` twice:

```sql
CREATE TABLE dbo.ProcessedMessages
(
    ConsumerName NVARCHAR(100) NOT NULL,
    MessageId UNIQUEIDENTIFIER NOT NULL,
    ProcessedAtUtc DATETIME2 NOT NULL,
    CONSTRAINT PK_ProcessedMessages
        PRIMARY KEY (ConsumerName, MessageId)
);
```

Within a **single database transaction**:

```text
BEGIN TRANSACTION
  INSERT ProcessedMessages(ConsumerName, MessageId, ...)
  (uniqueness protects against duplicate processing)
  APPLY BUSINESS UPDATE
COMMIT
ACK RabbitMQ delivery
```

If the insert proves the message has already been processed for this consumer, skip the business update and ACK safely. The key is **atomicity between dedupe record and business change** in the same transaction where possible. Handle concurrency/unique constraint violations deliberately.

### The transactional outbox problem

**Unsafe naive producer:**

```text
1. Save order to SQL Server.      [COMMIT succeeds]
2. Publish OrderPlaced to RabbitMQ [process crashes]
Result: order exists, event may not exist.
```

**Transactional Outbox approach:**

```text
API request
   |
   v
SQL transaction:
  INSERT Orders
  INSERT OutboxMessages(messageId, payload, status=Pending)
COMMIT
   |
   v
Background publisher reads outbox
   |
   v
Publish with publisher confirm
   |
   v
Mark outbox record as sent
```

This prevents the original database transaction from depending on a live broker, but the relay may still publish twice after ambiguous confirms. **Inbox/idempotent consumers remain necessary.**

### Correlation ID vs. message ID vs. delivery tag

| Field | Lifetime | Purpose |
|---|---|---|
| `MessageId` | Logical publication/event | Deduplication |
| `CorrelationId` | Business workflow | End-to-end tracing |
| `DeliveryTag` | Specific channel delivery | ACK/NACK on that channel |
| Consumer tag | Particular consumer subscription | Cancel/query consumer |
| Stream offset | Position in a stream | Replay/progress |

Do not use `DeliveryTag` as a durable business ID—it is **not globally unique**.

### Request/reply (RPC) via RabbitMQ

```text
Requester -> requests.q -> Worker
Requester <- reply-queue <- Worker
```

Typical request properties:

```text
ReplyTo       = callback queue (or Direct Reply-To convention)
CorrelationId = unique request ID
```

Worker publishes response to the reply destination with the same `CorrelationId`. The requester maintains a timeout and removes pending entries. This is valid but can recreate synchronous dependencies inside an asynchronous transport; for normal HTTP-like request/response workflows, consider whether a direct API is simpler.

---
## 28. Architecture scenarios: pick a topology for the requirement

There is rarely one universally best topology. Instead, start from the **business delivery requirement** and choose a topology that makes the requirement explicit.

### Scenario 1 — Background PDF generation

**Requirement:** API returns quickly; worker can take 30–90 seconds; jobs must survive a worker restart.

```text
HTTP API -> reports.direct -> reports.generate.qq -> ReportWorker(s)
                |                  |
                |                  +-- manual ACK after report persisted
                +-- publisher confirms, durable topology
```

Recommendations:

- Durable queue; prefer **quorum** if a clustered production environment and data safety are necessary.
- Publisher confirms and an outbox if saving “request accepted” into SQL and publishing are coupled.
- Return `202 Accepted` plus `jobId`; persist status in application database.
- Manual ACK after successful, idempotent report creation.
- Bounded retries and DLQ when generation persistently fails.
- Prefetch based on worker CPU/memory needs (e.g., 1–3 for heavy PDF work, not an arbitrary 1000).

### Scenario 2 — One event, three independently interested services

**Requirement:** all three services need `CustomerRegistered`.

```text
                              +-> welcome-email.q  -> EmailConsumer
customer.events.fanout -------+-> crm-sync.q       -> CrmConsumer
                              +-> analytics.q      -> AnalyticsConsumer
```

Each service owns its queue and consumer scale independently. Stopping the CRM worker should not prevent the email service from seeing its copy (subject to broker/storage pressure).

### Scenario 3 — Topic filtering by region and event

**Requirement:** EU fulfillment sees only events relevant to the EU, while audit sees all events.

```text
commerce.topic
   | `fulfillment.eu.*`  -> eu-fulfillment.q
   | `fulfillment.us.*`  -> us-fulfillment.q
   | `#`                 -> audit.q
```

Keep a documented routing-key grammar. Changing `fulfillment.eu.created` to `eu.fulfillment.created` silently breaks existing bindings unless consumers/topology are migrated together.

### Scenario 4 — Notifications with priorities

**Requirement:** urgent notification requests should be delivered before ordinary queued work.

- Quorum queue on RabbitMQ 4.3 supports **0–31 strict levels** automatically.
- Don't assume priority can interrupt a message already delivered to a worker.
- Beware starvation: a constant stream of highest-priority work can delay low-priority items.
- For stronger isolation and predictable service levels, use **separate urgent/normal queues** with separately provisioned workers rather than relying only on priority.

### Scenario 5 — External API rate limits

**Requirement:** third-party API intermittently returns `429 Too Many Requests`.

Options:

- RabbitMQ 4.3 quorum **delayed retry** for returned deliveries with configured bounds.
- An application-controlled scheduler for precise per-tenant `Retry-After` handling.
- Compatibility retry queue + TTL + DLX if you cannot use quorum 4.3 features; guard against duplicates and complex loops.

Don't pause all consumers because *one tenant* is rate-limited if the workload can be isolated per tenant.

### Scenario 6 — Image thumbnail processing

```text
uploads.topic -> images.thumbnail.qq -> N thumbnail workers
              -> images.audit.q      -> Audit worker
```

Worker reads object-storage URI, not a gigantic image body embedded in a RabbitMQ message. Keep the message small:

```json
{"imageId":"img-32","storageUri":"blob://bucket/file.jpg","maxWidth":320}
```

Payload URI/credential handling must be secure; use short-lived access mechanisms where needed.

### Scenario 7 — Coupon or OTP expiry

**Requirement:** an OTP is no longer useful after 60 seconds.

- Queue message TTL can discard stale work.
- Consumer **must still validate application-level expiry** before executing.
- Dead-letter expiration can give insight into system slowness, but avoid retaining secret OTP values in dead-letter payloads.

### Scenario 8 — Auditing and replaying events later

**Requirement:** several consumer groups must replay previous events next week.

- Classic/quorum queue after ACK isn't an event-history database.
- Consider **RabbitMQ Streams** with age/size retention and offset tracking (or Kafka for broader log/streaming needs).
- Retention must be long enough for the replay requirement.
- Store consumer checkpoints and handle duplicate replay idempotently.

### Scenario 9 — Strict per-customer ordering

**Requirement:** a given customer's operations must be handled in business order.

- Publishing to one queue doesn't guarantee completion order with several workers.
- Consider partitioning by `customerId`, serial processing per key, or single-active-consumer where appropriate.
- Redelivery and retries complicate even apparently FIFO workflows.
- Ensure application concurrency rules protect the true domain invariant.

### Scenario 10 — Fanout plus heavy analytics

**Requirement:** a lightweight email consumer and a high-volume analytics sink subscribe to the same events.

Give each subscriber its own queue. If the analytics consumer falls behind indefinitely, **its queue can still consume broker memory/disk**. Decoupling does not eliminate resource limits. Monitor per-subscriber backlog; consider a stream for large retained analytics feeds.

### Scenario 11 — Dynamic RPC reply queues

**Requirement:** a service sends a request and waits for an answer.

- A temporary **exclusive classic queue** may be suitable for each requester connection, or consider RabbitMQ Direct Reply-To as documented.
- Set `ReplyTo`, `CorrelationId`, and application timeout.
- If the client disconnects, the reply may have nowhere useful to go.
- Avoid adding RabbitMQ RPC where plain HTTP/gRPC would be simpler.

### Scenario 12 — Business critical “OrderPlaced” and SQL Server

**Requirement:** order creation and event publication must not silently diverge.

- Store order and outbox row in **one SQL transaction**.
- Outbox relay publishes using confirms.
- Consumer uses `MessageId` dedupe with durable, application-owned processing state.
- ACK only after its own database state is committed.
- Alert on outbox lag, unconfirmed publishes, DLQ accumulation, and stuck consumers.

### Scenario 13 — Low latency live feed vs. guaranteed processing

Choose **a lossy, low-latency push design** only where drops are acceptable. For important financial transactions or irreversible side effects, favor durable storage, publisher confirms, manual ACK, quorum replication, observability, and reconciliation—even if it costs latency. State the delivery guarantees in a contract, not in a vague assumption like “RabbitMQ is reliable.”

### Scenario 14 — Broker temporarily unavailable

**Requirement:** the API should remain responsive when RabbitMQ cannot be reached.

- Avoid blocking API request threads on endless reconnection loops.
- Fail fast with a meaningful response or commit a durable local outbox when business semantics allow.
- Use bounded connection/retry policies, jitter, and monitoring.
- Circuit breaking can protect downstream dependencies; it doesn't replace durable message storage.

### Scenario 15 — Versioned domain events

**Requirement:** subscribers deploy independently.

Publish a stable event schema (`OrderPlaced.v1`), prefer additive compatible fields, and define a clear migration plan for breaking changes. A message queue decouples execution time, not schema meaning.

### Topology decision worksheet

Before writing production code, answer:

1. Is the message a **command** or **event**?
2. Is **loss acceptable**? Under which failure modes?
3. Must it survive **broker restart** or **node loss**?
4. Does each independent service need a **separate copy**?
5. Must old events be **replayed**?
6. How important is **ordering**, and at what granularity?
7. What is the expected **peak publish rate**, duration, and backlog?
8. How large is the **message body**?
9. Are duplicates **safe**? If not, what is the idempotency key?
10. What happens when a consumer permanently fails?
11. How will **DLQ** messages be investigated and replayed?
12. How are topology and policies deployed across environments?
13. Who owns the message schema and its evolution?
14. What metrics trigger an alert?
15. Where does a transaction **begin and end**?

## 29. Operations: monitoring, security, alarms, and performance

### 29.1 Monitoring checklist

| Metric/signal | What it may mean | Initial action |
|---|---|---|
| `Ready` growing | Producer faster than consumers | Inspect consumers, downstream latency, capacity |
| `Unacked` stuck | Consumer busy, blocked, or never ACKing | Check prefetch, worker logs, handler timeouts |
| Consumer count drops to 0 | Worker disconnected | Check application health/reconnect |
| Publisher confirm latency rises | Replication/disk load, broker stress | Inspect queue type, node disk, resource alarms |
| Frequent redeliveries | Crashes, retries, ACK loss, poison messages | Inspect error classification and DLQ |
| Dead-letter rate spikes | Bad schema, dependencies, expiry, limits | Investigate `x-death` reasons and app exceptions |
| Memory alarm | Broker under memory pressure | Control ingress/backlog, capacity plan |
| Disk alarm | Free disk below configured threshold | Increase disk/reduce backlog safely |
| High connection/channel churn | App creates/disposes too frequently | Reuse connections, manage channels intentionally |
| Quorum unavailable | Majority lost or membership problems | Restore quorum members, inspect cluster health |

RabbitMQ provides a Management API, metrics, and optional Prometheus support. A dashboard is useful, but **alerts should reflect service-level outcomes** (job age, pending orders, failed events), not merely “broker is running.”

### 29.2 Resource alarms

When RabbitMQ hits memory or disk resource thresholds, it may **block publishers** to protect itself. This can present as slow/stalled publishes even though consumers still process messages. Read the broker logs and Management UI alarms instead of immediately increasing client timeouts.

Useful commands:

```powershell
docker exec rabbitmq-local rabbitmq-diagnostics alarms
docker exec rabbitmq-local rabbitmq-diagnostics memory_breakdown
docker exec rabbitmq-local rabbitmq-diagnostics listeners
docker exec rabbitmq-local rabbitmqctl list_connections name state channels
```

### 29.3 Security basics

**Never expose a lab broker to the public internet.** For real deployments:

- Use TLS for external AMQP traffic and management endpoints.
- Use distinct users per application/system, vhost-specific least-privilege permissions.
- Store secrets in a secret manager/managed configuration—not in committed `.env` or app source files.
- Isolate vhosts/environments; avoid default administrator credentials.
- Restrict management UI access and audit administrative actions.
- Avoid PII/credentials inside message body logs, DLQ inspection output, and headers.
- Apply topology creation rights to the smallest possible scope.
- Use supported broker and Erlang releases; plan patching/upgrades.

### 29.4 Permissions and virtual hosts

RabbitMQ permissions for AMQP 0-9-1 users are often configured with **configure/write/read** regexes on each vhost:

- **configure:** declare/delete topology resources matching the regex.
- **write:** publish/bind operations requiring write rights.
- **read:** consume/get from resources requiring read rights.

The exact checks can depend on operation and resource, so validate using the [access control documentation](https://www.rabbitmq.com/docs/access-control) rather than granting `.*` in production.

Local CLI example (learning purposes; not a hardened production role):

```powershell
docker exec rabbitmq-local rabbitmqctl list_permissions -p learning
```

### 29.5 Policies vs. operator policies vs. arguments

- **Client declaration arguments (`x-*`):** attached to one entity at declaration time; often hard to change without redeclaration.
- **Policies:** apply centrally to matching names/types using a regex; can be updated by operations teams.
- **Operator policies:** enforce upper/lower limits and resource guardrails; may override application preferences with special numeric precedence rules.

**Design habit:** reserve immutable structural facts (such as queue type) for deliberate topology provisioning and keep adjustable retention/DLX/limits in policies when supported.

### 29.6 Typical production topology responsibilities

| Concern | Usually owned by |
|---|---|
| Message schema and routing contract | Application/platform architecture |
| Connection/recovery behavior | Application team |
| Exchange/queue/binding deployment | Application or infrastructure-as-code pipeline |
| Users, TLS, network segmentation | Platform/security |
| Queue policies, quotas, retention guardrails | Platform/operations with app input |
| DLQ processing playbook | Service owner |
| Broker upgrades/cluster backups/replication | Platform/operations |

### 29.7 Performance principles

- **Reuse connections:** avoid one connection per message.
- **Treat channels as concurrency-sensitive:** do not share an `IChannel` for uncontrolled concurrent publishing.
- **Use async I/O:** but don't spawn one unbounded task per delivery.
- **Bound in-flight work:** prefetch, worker concurrency, application queues, and downstream limits should agree.
- **Batch only when needed:** throughput tuning can make failure/retry recovery more complex.
- **Keep payloads modest:** store large objects in object storage; send references and metadata.
- **Choose queue type deliberately:** quorum replication costs resources but improves data safety; streams have different throughput/retention characteristics.
- **Avoid creating enormous numbers of queues/channels unnecessarily.**
- **Measure first:** benchmark with realistic body sizes, number of producers, confirms, consumers, disk throughput, and failure scenarios.

### 29.8 Diagnostics through the HTTP API

The Management UI runs on an HTTP API. For a local, authenticated lab, an example inspection request is:

```powershell
$pair = "${env:RABBITMQ_USER}:${env:RABBITMQ_PASSWORD}"
$token = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes($pair))
Invoke-RestMethod -Uri "http://localhost:15672/api/overview" -Headers @{ Authorization = "Basic $token" }
```

**Do not** put long-lived production credentials on a command line or paste the resulting full diagnostics into a public issue: operational responses can reveal names and environment details. The HTTP API is useful for tools and health dashboards, but message processing should use a real protocol client.

### 29.9 Shutdown and deployment behavior

At application shutdown:

1. Stop registering/accepting more jobs.
2. Allow in-flight processing to finish within a **bounded graceful shutdown** window.
3. ACK successfully completed jobs, ideally after data commit.
4. Leave unfinished/unacked work available for broker redelivery; avoid swallowing it.
5. Close the consumer channel/connection appropriately.

A broker restart while consumers are active is **not** the same as a clean idle restart. Expect duplicate delivery possibilities at recovery boundaries.

## 30. Hands-on labs: 15 controlled experiments

> **Lab rules:** Use the `learning` vhost. Give each lab its own names. Observe `Ready` and `Unacked`. Write down *expected* results before clicking publish/get. Never run purge/delete experiments on a shared or production queue. Prefer the UI for topology and the C# console examples for precise ACK/NACK behavior.

### Lab 1 — Validate local installation

**Goal:** verify broker, plugin, network, and volume.

1. Run `docker compose up -d`.
2. Open <http://localhost:15672> and log in.
3. Check `docker compose ps` and run `rabbitmq-diagnostics -q ping` inside the container.
4. Confirm `learning` exists under virtual hosts.
5. Run `docker volume ls` and identify the Compose-managed data volume.

**Expected:** broker running, UI reachable, the volume exists. **Learn:** AMQP port 5672 and UI port 15672 serve different protocols.

### Lab 2 — Default exchange

**Goal:** publish to a named queue without declaring a custom exchange.

1. Create durable **classic** queue `lab02.jobs` (non-exclusive, no auto-delete).
2. Navigate to **Exchanges** and find the default exchange (often displayed as `(AMQP default)`).
3. Publish body `Hello L2` using routing key `lab02.jobs`.
4. Check `lab02.jobs` → `Ready`.
5. Use **Get messages** on this disposable queue and watch the selected ACK/requeue mode.

**Expected:** `Ready` increases before consumption, then changes with Get behavior. **Learn:** publishing to an empty-named exchange still uses an exchange.

### Lab 3 — Direct exchange and unmatched key

1. Create durable direct exchange `lab03.direct`.
2. Create durable queues `lab03.critical` and `lab03.normal`.
3. Bind respectively with `critical` and `normal`.
4. Publish body A with key `critical`.
5. Publish body B with key `normal`.
6. Publish body C with key `Critical`.

**Expected:** A only in critical, B only in normal; C is unmatched (unless alternate or return behavior is configured). **Learn:** exact, case-sensitive matching.

### Lab 4 — Direct exchange can fan out to multiple queues

1. Reuse `lab03.direct`.
2. Create durable queue `lab04.audit`.
3. Bind `lab04.audit` to `lab03.direct` using key `critical`.
4. Publish body `urgent #2` with key `critical`.

**Expected:** both `lab03.critical` and `lab04.audit` receive a copy. **Learn:** direct means *exact-key* routing, not *single-queue* routing.

### Lab 5 — Fanout broadcast vs. competing consumers

1. Create durable fanout exchange `lab05.fanout`.
2. Create durable queues `lab05.email`, `lab05.analytics`.
3. Bind both (routing key ignored).
4. Publish twice with arbitrary routing keys.
5. Observe two messages in **each** queue.
6. Later, run two consumer processes against `lab05.email` only.

**Expected:** a copy per queue, but a given delivery from `lab05.email` goes to one competing consumer at a time. **Learn:** independence comes from **separate queues**, not separate consumers.

### Lab 6 — Topic wildcard matrix

1. Create topic exchange `lab06.topic`.
2. Create queues `lab06.orders`, `lab06.region`, `lab06.all`.
3. Bind them with `order.*`, `order.eu.*`, and `#` respectively.
4. Publish `order.created`, `order.eu.created`, `payment.failed`, and `order`.
5. Compare queue counts with the matrix in section 10.

**Expected:** `order.*` matches only two-segment order keys; `#` matches all; `order.eu.*` matches one of your publishes. **Learn:** `*` vs. `#`.

### Lab 7 — Headers exchange `all` vs. `any`

1. Create headers exchange `lab07.headers`.
2. Create `lab07.eu.pdf` with header binding `x-match=all`, `format=pdf`, `region=eu`.
3. Create `lab07.high.or.eu` with `x-match=any`, `priority=high`, `region=eu`.
4. Publish a message with `format=pdf`, `region=eu`, `priority=normal`.
5. Publish another with `format=png`, `region=us`, `priority=high`.

**Expected:** first goes to both queues; second only to `lab07.high.or.eu`. **Learn:** transport headers are routing attributes with AMQP types.

### Lab 8 — Durable queue + persistent messages survive normal restart

1. Create durable classic queue `lab08.durable`.
2. Publish persistent messages (`DeliveryMode=2`) using the producer code or UI persistence option.
3. Confirm messages are **Ready** with no consumer.
4. Run `docker compose restart rabbitmq` (do **not** use `down -v`).
5. Reopen UI and inspect queue/messages.

**Expected:** topology and persistent queued data should survive an ordinary restart. **Learn:** durable queue + persistent message + retained volume.

### Lab 9 — Message ACK vs. missing ACK

1. Start the .NET consumer sample with `autoAck=false`.
2. Publish several messages.
3. While worker processes, watch `Unacked`.
4. ACKed deliveries disappear.
5. Adapt demo to simulate a crash **after** processing but **before** ACK; publish and inspect redelivery.

**Expected:** a failed/unsettled delivery can reappear; **business action may have already occurred**. **Learn:** why consumer idempotency matters.

### Lab 10 — Prefetch limits and fairness

1. Change consumer `prefetchCount` from `1` to `5`.
2. Use a long fake work delay (e.g., 10 seconds) to make `Unacked` visible.
3. Start 2 consumers on the same queue.
4. Publish 20 jobs while watching counts.
5. Compare with prefetch=1.

**Expected:** larger prefetch allows more outstanding deliveries and different distribution; not a guarantee of fair business completion order. **Learn:** delivery window vs. task concurrency.

### Lab 11 — TTL expiration + DLQ

1. Create durable fanout exchange `lab11.dlx` and queue `lab11.dead` bound to it.
2. Create durable queue `lab11.expiring` with `x-message-ttl=5000` and `x-dead-letter-exchange=lab11.dlx`.
3. Publish persistent messages to `lab11.expiring` with no consumer.
4. Wait beyond 5 seconds and inspect `lab11.dead`.
5. Look at `x-death` headers on a disposable copy.

**Expected:** messages expire from the source queue and can be dead-lettered to the target. **Learn:** TTL is not permanent preservation; observable expiration may not be exact to the millisecond.

### Lab 12 — Reject vs. requeue

1. Build `lab12.work` with a DLX and `lab12.dead` target.
2. Publish two jobs.
3. Consume job A and call `BasicNackAsync(... requeue:true)`.
4. Consume job B and call `BasicRejectAsync(... requeue:false)`.
5. Observe that A is eligible for redelivery; B is routed to DLQ if configured.

**Expected:** the negative acknowledgment flag changes whether the message stays in circulation or is removed/dead-lettered. **Learn:** not all failures behave alike.

### Lab 13 — Quorum queue in RabbitMQ 4.3

1. Create durable queue `qq.lab13` with `x-queue-type=quorum`.
2. Confirm UI shows queue type **quorum**.
3. Publish persistent messages with a confirm-enabled publisher.
4. Consume/ACK a message.
5. Compare queue details against a classic queue.
6. Remember: this is still a **single-node broker**, so no replica-based fault tolerance can be demonstrated locally.

**Expected:** quorum-specific UI/metrics/features, but **no HA** from one node. **Learn:** replicated queue design vs. single node setup.

### Lab 14 — Quorum delayed retry + delivery limit

1. Reuse quorum `qq.lab13` or create `qq.lab14`.
2. Apply the 4.3 `delayed-retry-type=all`, `min=2000`, `max=10000` policy to the `qq.` prefix.
3. Publish one job.
4. Consumer NACKs `requeue=true` instead of ACKing.
5. Watch deferred/delayed state and count in Management UI.
6. Configure a DLX and explore delivery-limit behavior separately; **NACK-based returns need not increment the failure-based delivery count** in the same way as crashes/rejects.

**Expected:** no immediate hot retry loop when delayed-retry policy applies. **Learn:** 4.3 native broker-side delay, failure counters, and policy scope.

### Lab 15 — Quorum message priority

1. Create `qq.lab15` as durable quorum queue.
2. Stop consumers so messages can accumulate.
3. Publish priorities `1`, `5`, `31`, and one **without** a priority.
4. Start a consumer with prefetch=1; record first deliveries.
5. Compare with classic priority queue using `x-max-priority`.

**Expected:** 4.3 quorum dispatches higher queued priorities before lower ones (assuming no exceptional redelivery state); the unspecified priority behaves as **4**, not classic's **0**. **Learn:** 4.3 strict priority and why consumer state affects ordering.

### Additional safe “what if?” experiments

- Publish to an exchange with **no bindings** using `mandatory=false` then `mandatory=true` and compare behavior.
- Delete a *disposable* queue, then publish again using the default exchange.
- Attempt to redeclare an existing direct exchange as topic; inspect the channel exception.
- Remove a consumer and watch queued backlog grow.
- Restart RabbitMQ during an outstanding unacked delivery and watch redelivery.
- Compare a queue's `Ready` and `Unacked` counts while the worker sleeps.
- Intentionally introduce a routing key typo; see that a healthy exchange doesn't make it route correctly.
- Build an alternate exchange and validate unroutable fallback separately from consumer DLX.
- Compare `x-message-ttl=3000` against `Expiration="1000"` when both apply.
- For a later multi-node cluster: stop a quorum leader and test majority behavior (this cannot be proved by the single-node lab).

### Experiments checklist for a learning journal

For every lab, record:

```text
Lab ID:
Exchange / type:
Binding(s):
Queue type / durable / exclusive / autoDelete:
Message properties (DeliveryMode, TTL, Priority):
Publisher confirm setting:
Consumer autoAck setting:
Prefetch:
Expected Ready / Unacked / DLQ changes:
Observed changes:
What happens after consumer failure?
What happens after broker restart?
One sentence explaining WHY:
```

This process is more valuable than simply getting “Hello World” to print successfully.

## 31. Troubleshooting and common misconceptions

### Error: `PRECONDITION_FAILED` during declaration

**Cause:** the queue/exchange already exists but was declared with incompatible settings (for example classic vs. quorum, or direct vs. fanout). **Fix:** inspect current entity and deployment ownership. For a lab, choose a **new name**, rather than deleting unrelated data.

### Error: `NOT_FOUND - no exchange` or `no queue`

The target does not exist in the selected **virtual host**, or the name is misspelled. Check that both producer and consumer use the same vhost and the expected topology is provisioned.

### Connection refused on port 5672

- Is Docker Desktop running?
- Is the broker container healthy?
- Did you map 5672 on loopback?
- Are you calling `localhost` from a **different container** by mistake?
- Is another service already using port 5672?

### “The management page opens but C# cannot connect”

Port 15672 is an HTTP management listener; it is **not** the AMQP protocol. Check credentials, `VirtualHost`, TCP port 5672, and user permissions.

### “I changed the password in `.env`, but login still uses the old password”

Initialization environment variables are not a dynamic user-management system. Existing broker state in the Docker volume is retained. Update via UI/CLI or reset a **disposable** local volume intentionally.

### “My queue disappeared after a consumer disconnected”

You may have declared an **exclusive** queue or an **auto-delete** queue. Verify flags and lifecycle semantics.

### “My messages disappeared after restart”

Investigate non-durable queues, transient messages, ACKs, TTL, DLX, purging, queue deletion, and whether the data volume was removed. Durable queue alone isn't enough for transient classic messages.

### “I published successfully, but no queue received it”

Check exchange, binding, routing key, and vhost. An unroutable message may be discarded; `mandatory` plus proper confirmation/return handling or an alternate exchange helps make this visible.

### “My consumer processes the same job twice”

Likely causes: unacked delivery redelivered, consumer crash after business commit, ambiguous publish retry, or application bug. Use stable message IDs and idempotent processing, not assumptions of exactly-once delivery.

### “Increasing prefetch didn't speed up my consumer”

Prefetch controls the broker delivery window, **not** the actual concurrency of your processing code. If your consumer dispatch is sequential, a prefetch of 100 may just produce 100 unacked messages and no extra processing parallelism.

### “Messages are processed out of order”

Check worker count, prefetch, concurrent dispatch, redeliveries, priority, and multiple publisher channels. Queue ingress order and business completion order are different notions.

### “Messages are constantly re-delivered”

Inspect hot `requeue=true` loops, transient dependency failures, `x-delivery-count` and delivery limits on quorum queues, and whether 4.3 delayed retry is configured. Move permanent failures to a DLQ instead of indefinite immediate requeue.

### “RabbitMQ hangs after filling a queue”

Inspect disk/memory resource alarms and publisher blocking, queue size policies, and downstream consumer throughput. Raising a client timeout doesn't solve exhausted disk.

### “What about transactions?”

AMQP transactions are a separate mechanism and generally not a substitute for modern publisher confirms, database outbox, and consumer idempotency. There is **no implicit atomic transaction** spanning SQL Server and RabbitMQ merely because both operations happen in one C# method.

### “Can RabbitMQ store a message forever?”

A queue can retain a ready message for a long time if not removed by policy or consumer—but storage is finite and nodes can fail. For history/replay semantics, use streams/retention or durable business/event storage rather than an infinitely growing work queue.

### “Will a durable exchange store unrouteable messages?”

**No.** An exchange routes; it does not indefinitely buffer messages waiting for a matching binding.

### “Does `ACK` mean the business action succeeded?”

Only if your **application logic** makes that true. RabbitMQ knows that a consumer acknowledged a delivery, not that your invoice was paid or database committed.

### “Does `BasicPublishAsync` completion mean the consumer finished?”

**No.** Even with confirms, it is a publisher/broker guarantee boundary, not a consumer processing completion signal.

### “Do RabbitMQ and Kafka have the same consumption model?”

**No.** Standard RabbitMQ classic/quorum work queues remove messages after ACK (subject to queue semantics); Kafka and RabbitMQ Streams retain logs and track consumer positions separately. RabbitMQ is multi-protocol and supports both work queues and streams, so compare **the selected data structure**, not only the product names.

### “Is the `guest` account okay remotely?”

Don't design around it. RabbitMQ's default `guest` account is conventionally restricted to loopback access; create least-privilege users and proper TLS for remote systems. Our Compose file uses an explicit development account instead.

### “Is Docker volume durability the same as cluster HA?”

**No.** A named volume preserves files across routine container replacement, but it does not give you replicas, independent failure domains, or a disaster recovery plan.

### “Can I change a classic queue to quorum by declaring it again?”

**No.** Queue type is a foundational characteristic. Plan a topology/data migration or create a new queue and transition publishers/consumers safely.

### “Can a message be routed to multiple queues and processed several times?”

**Yes, intentionally.** Each queue receives a copy for its own subscriber. That is not the same as accidental duplicate delivery to one consumer. Use separate identifiers/dedupe scopes when necessary.

### “Why doesn't my headers exchange match?”

Inspect the binding's `x-match`, exact header names, AMQP value types, and whether your headers were actually sent in *message properties*, not merely as JSON body fields.

### “Why did my old RabbitMQ 3.x or 4.2 tutorial stop working on 4.3?”

Key checks: deprecated transient non-exclusive classic queues, new quorum priority behavior, quorum-specific consumer timeouts, native delayed retry, and .NET client 7.x breaking API changes. Pin versions in sample projects and explicitly document upgrades.

## 32. Glossary, learning path, and official references

### Glossary (quick reference)

| Term | Definition |
|---|---|
| Broker | Server that receives, routes, stores, and delivers messages |
| Producer / publisher | Application that publishes messages |
| Consumer | Application that receives deliveries |
| Exchange | Routes publications according to type/bindings |
| Queue | Buffers and delivers messages to consumers |
| Binding | Link/routing rule between exchange and destination |
| Routing key | Publisher-provided route string |
| Direct exchange | Exact routing key matching |
| Fanout exchange | Sends to all bound destinations |
| Topic exchange | Matches dot-separated key patterns with `*` and `#` |
| Headers exchange | Routes using transport header conditions |
| Default exchange | Special exchange named `""`; routes by queue name |
| Alternate exchange | Fallback route for unroutable publishes |
| Vhost | Logical broker namespace and authorization boundary |
| Connection | AMQP TCP/session relationship |
| Channel | Logical session within a connection |
| Consumer tag | Server/client label for a subscription |
| Delivery tag | Channel-scoped identifier for ACK/NACK |
| Ready | Queued messages awaiting delivery |
| Unacked | Delivered messages awaiting acknowledgment |
| ACK | Positive consumer delivery acknowledgment |
| NACK | Negative consumer acknowledgment |
| Reject | Negative acknowledgment for one delivery |
| Requeue | Return a delivery for later processing |
| Publisher confirm | Broker confirmation of a publication's handling |
| Mandatory | Requests return handling for unroutable publications |
| Durable | Topology survives restart |
| Persistent | Message marked for disk-based recovery (AMQP 0-9-1) |
| Exclusive | Queue tied to owner connection where supported |
| Auto-delete | Remove entity after lifecycle condition |
| TTL | Time to live / expiration threshold |
| DLX | Dead Letter Exchange receiving queue-dead-lettered messages |
| DLQ | Destination queue for investigating failed/expired messages |
| Poison message | Message consistently failing processing |
| Prefetch | Maximum outstanding deliveries per consumer in common mode |
| SAC | Single Active Consumer: one consumer actively receives at a time |
| Quorum queue | Durable Raft-replicated queue type |
| Classic queue | Non-replicated versatile queue |
| RabbitMQ Stream | Replayable, persistent append-only message log |
| Super stream | Partitioned logical stream |
| Idempotency | Duplicate message doesn't duplicate business effect |
| Outbox | Store business update and outgoing event in one DB transaction |
| Inbox | Durable receiver-side dedupe/processing record |
| Khepri | RabbitMQ metadata store in 4.3 |
| `x-death` | History of dead-lettering events and reasons |
| `x-delivery-count` | Quorum message failure/redelivery count metadata |
| `x-queue-type` | Queue type declaration argument |
| Policy | Centrally applied matching broker configuration |
| Operator policy | Broker guardrail that can restrict client/policy settings |
| Raft | Consensus mechanism for replicated state |

### Suggested study progression (12 sessions)

| Session | Core objective | Hands-on |
|---|---|---|
| 1 | Install RabbitMQ, understand UI and AMQP | Labs 1–2 |
| 2 | Exchanges and routing | Labs 3–7 |
| 3 | Durability and persistence | Lab 8 |
| 4 | Connections, channels, modern .NET API | Producer/consumer |
| 5 | Manual ACK, crash recovery, prefetch | Labs 9–10 |
| 6 | TTL, DLX, NACK/reject | Labs 11–12 |
| 7 | Quorum queues and 4.3 features | Labs 13–15 |
| 8 | Message identity and idempotency | Duplicate delivery test |
| 9 | SQL Server outbox and inbox | DB transaction + relay design |
| 10 | ASP.NET Core + BackgroundService | Report processing system |
| 11 | Monitoring, alarms, security | Failure and backlog drills |
| 12 | Streams, replay, and Kafka comparison | Dedicated Stream client exercises |

### Official source references (read before modifying version-sensitive code)

**Releases and setup**

- [RabbitMQ server releases](https://github.com/rabbitmq/rabbitmq-server/releases)
- [Official Docker image/tags](https://hub.docker.com/_/rabbitmq/tags)
- [RabbitMQ downloads](https://www.rabbitmq.com/docs/download)
- [RabbitMQ 4.3 highlights (April 2026)](https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release)
- [RabbitMQ/Erlang compatibility](https://www.rabbitmq.com/docs/which-erlang)
- [Management plugin](https://www.rabbitmq.com/docs/management)

**RabbitMQ core concepts**

- [Exchanges](https://www.rabbitmq.com/docs/exchanges)
- [Queues](https://www.rabbitmq.com/docs/queues)
- [Classic queues](https://www.rabbitmq.com/docs/classic-queues)
- [Quorum queues, including 4.3 features](https://www.rabbitmq.com/docs/quorum-queues)
- [Streams and super streams](https://www.rabbitmq.com/docs/streams)
- [Consumer acknowledgements and publisher confirms](https://www.rabbitmq.com/docs/confirms)
- [Publishers](https://www.rabbitmq.com/docs/publishers)
- [Consumers](https://www.rabbitmq.com/docs/consumers)
- [Prefetch](https://www.rabbitmq.com/docs/consumer-prefetch)
- [TTL and expiration](https://www.rabbitmq.com/docs/ttl)
- [Dead Letter Exchanges](https://www.rabbitmq.com/docs/dlx)
- [Queue length and overflow](https://www.rabbitmq.com/docs/maxlength)
- [Policies](https://www.rabbitmq.com/docs/policies)
- [Reliability and data safety](https://www.rabbitmq.com/docs/reliability)

**.NET and developer references**

- [RabbitMQ .NET 7.x client API guide](https://www.rabbitmq.com/client-libraries/dotnet-api-guide)
- [RabbitMQ.Client on NuGet](https://www.nuget.org/packages/RabbitMQ.Client/)
- [.NET tutorial: Hello World](https://www.rabbitmq.com/tutorials/tutorial-one-dotnet)
- [.NET tutorial: Work queues](https://www.rabbitmq.com/tutorials/tutorial-two-dotnet)
- [.NET tutorial: Publish/Subscribe](https://www.rabbitmq.com/tutorials/tutorial-three-dotnet)
- [.NET tutorial: Routing](https://www.rabbitmq.com/tutorials/tutorial-four-dotnet)
- [.NET tutorial: Topics](https://www.rabbitmq.com/tutorials/tutorial-five-dotnet)
- [.NET tutorial: Reliable publishing](https://www.rabbitmq.com/tutorials/tutorial-seven-dotnet)
- [RabbitMQ .NET client source code](https://github.com/rabbitmq/rabbitmq-dotnet-client)

**Operations**

- [Access control](https://www.rabbitmq.com/docs/access-control)
- [Memory and disk alarms](https://www.rabbitmq.com/docs/alarms)
- [Monitoring](https://www.rabbitmq.com/docs/monitoring)
- [Cluster formation](https://www.rabbitmq.com/docs/cluster-formation)
- [Production checklist](https://www.rabbitmq.com/docs/production-checklist)

---

## Appendix A — Advanced reliability: at-least-once dead-lettering (quorum queues)

A detail that is easy to miss: **a DLX is not automatically a safe persistent backup queue**. Standard dead-letter republishing may fail if the target queue cannot accept the message. For selected **quorum queues**, RabbitMQ supports an *at-least-once* dead-letter strategy that uses an internal consumer and publisher confirmations.

**Prerequisites (source quorum queue):**

1. Declare source queue as a durable **quorum** queue.
2. Configure a valid **dead-letter exchange** and target binding/queue.
3. Set `dead-letter-strategy = at-least-once`.
4. Set `overflow = reject-publish` (**not** `drop-head`).
5. Ensure the required stream queue feature flag is enabled on versions that need it (the 4.3 broker feature state should be verified; don't toggle cluster feature flags blindly).
6. Prefer reasonable queue length/resource limits, monitoring, and an operational DLQ response process.

An illustrative policy for `safe.` prefixed **quorum source queues**:

```powershell
docker exec rabbitmq-local rabbitmqctl set_policy -p learning safe-dlx '^safe\.' '{"dead-letter-exchange":"safe.events.dlx","dead-letter-routing-key":"safe.failed","dead-letter-strategy":"at-least-once","overflow":"reject-publish","max-length":100000}' --apply-to quorum_queues
```

**Why the overflow setting matters:** When the destination is unreachable, the source retains unconfirmed dead-lettered messages. This can consume capacity and block further publishes. Switching the strategy back to `at-most-once`, even indirectly by switching overflow to `drop-head`, can cause **outstanding dead-letter messages to be deleted**. Changes must be managed carefully.

**Another subtlety:** quorum source queues persist messages even when AMQP delivery mode isn't set to persistent, but **dead-lettered messages preserve their original AMQP delivery mode**. If the DLQ is a classic queue and must survive restart, publish source messages with `DeliveryMode = 2` and use durable targets. See the official [quorum queue dead-letter section](https://www.rabbitmq.com/docs/quorum-queues#dead-lettering).

## Appendix B — Enabling RabbitMQ Streams locally (optional later module)

The original Compose example intentionally exposes **only** AMQP 0-9-1 and the Management UI. If you later want to learn the dedicated Stream protocol:

1. Enable the plugin on the disposable local broker:

   ```powershell
   docker exec rabbitmq-local rabbitmq-plugins enable rabbitmq_stream
   ```

2. Add an **optional** Compose port mapping under `rabbitmq.ports`:

   ```yaml
   - "127.0.0.1:5552:5552"
   ```

3. Recreate the container without removing volumes:

   ```powershell
   docker compose up -d
   ```

4. Verify the stream listener and enabled plugins:

   ```powershell
   docker exec rabbitmq-local rabbitmq-diagnostics listeners
   docker exec rabbitmq-local rabbitmq-plugins list -e
   ```

5. Create streams with appropriate retention and use the official **RabbitMQ Stream .NET client**, not the AMQP 0-9-1 tutorial's `IChannel` abstraction, for high-performance offset-based consumption.

**Container lifecycle caution:** plugin changes made interactively inside a container may not survive image re-creation if plugin state is not declared through a persisted/managed enabled-plugins configuration. For a reproducible Streams module, create a dedicated Docker configuration or image that enables `rabbitmq_stream` at startup. **Do not** expose the Stream listener publicly without authentication/TLS/network controls.

## Appendix C — Small but consequential details most tutorials omit

**1. The same published message can live in several queues simultaneously.** If a topic exchange routes one event into two subscriber queues, its TTL, ACK, dead-letter events, and processing outcomes can differ **per queue**.

**2. `message.Expiration` is a decimal string in milliseconds.** The broker parses `"15000"` differently from JSON body text `{"expiration":15000}`; only transport properties have AMQP expiration meaning.

**3. `x-expires` does not mean delete when a queue becomes empty.** It is based on *unused-queue lease* semantics, including consumer/redeclare/get activity; read its documentation for your queue type.

**4. A routing key does not control a fanout exchange.** Providing `payment.failed` to a fanout exchange is allowed, but the exchange ignores it for delivery selection.

**5. An exchange is not a persistent message backlog.** If you publish before declaring/binding queues, a durable exchange doesn't hold events waiting for future subscribers.

**6. Classic `x-max-priority` is a topology design property.** Quorum's 4.3 0–31 strict priority support has different defaults and rules. Don't assume old tutorial priorities work identically after changing queue type.

**7. Multiple subscribers require multiple queues.** Multiple consumers on *one* queue compete; they do not each receive a copy of all messages.

**8. Connection failure can make successful publish results ambiguous.** Use confirms, stable IDs, and idempotent consumers. Message retries can duplicate business work, even when the broker is behaving correctly.

**9. Logs are part of the security boundary.** DLQ inspection, headers, MessageId, correlation IDs, and trace dumps can reveal sensitive payloads if you print whole messages. Use redaction.

**10. Queue health is not business health.** `Ready=0` might mean work completed, but it may also mean an auto-ack consumer dropped everything. Verify application-level outcomes.

---

## Final checklist: before building your first real service

- [ ] I can explain publisher → exchange → binding → queue → consumer.
- [ ] I understand **one queue/many consumers** vs. **many queues/independent subscribers**.
- [ ] I can choose direct, fanout, topic, or headers for a scenario.
- [ ] I know why an unroutable persistent message may still disappear.
- [ ] I can explain queue durability separately from message persistence.
- [ ] I can explain ACK, NACK, reject, requeue, and `multiple`.
- [ ] I know why “ACK after DB commit” can still result in duplicate business attempts.
- [ ] I can configure prefetch and explain why it is not the same as concurrency.
- [ ] I can route invalid/expired/over-limit messages to a DLQ.
- [ ] I know the differences between classic, quorum, and stream storage.
- [ ] I understand **RabbitMQ 4.3 quorum delayed retry and priority changes**.
- [ ] I know why Docker named volumes do not provide cluster HA.
- [ ] I can write a modern RabbitMQ.Client 7.x publisher/consumer.
- [ ] I know how to monitor `Ready`, `Unacked`, backlog, and resource alarms.
- [ ] I can name a message ID, dedupe key, retry strategy, and failure playbook.

> **The goal isn't to memorize every flag. It is to predict what happens to a message when the producer, broker, network, or consumer fails—and to design the system so the result is intentional.**

**License/attribution:** This is an original educational guide written for the `RabbitMQ` repository. The official sources linked above are the authoritative references for version-dependent behavior. The code is designed as instructional material and should be tested and adapted before use in production.
