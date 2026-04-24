# AMQP Connection Multiplexing Proxy

A transparent AMQP 1.0 proxy that removes the Azure Service Bus Emulator's 10-connection limit, enabling realistic microservice development and integration testing.

## Problem

The Azure Service Bus Emulator enforces a hardcoded 10 concurrent AMQP connection limit. When exceeded, clients receive `ConnectionsQuotaExceeded`. This makes the emulator unusable for architectures with more than ~8 services (the emulator uses ~2 connections internally).

## Solution

This proxy sits between your applications and the emulator, accepting unlimited client connections and multiplexing them over a small pool of backend connections to the emulator.

```
  Your Services          Proxy              Emulator
  ──────────────    ┌─────────────┐    ┌──────────────┐
  Service A ─────── │ listen :5672│    │ AMQP (internal)│
  Service B ─────── │             │───>│               │
  ...               │ pool: <=8   │    │ HTTP :5300    │
  Service N ─────── │ connections │    │ (direct)      │
  ──────────────    └─────────────┘    └──────────────┘
```

**No code changes required.** Same connection string, same SDKs.

## Quick Start (Docker Compose)

From the repository root:

```bash
ACCEPT_EULA=Y SQL_PASSWORD='YourStrong!Pass123' \
  docker compose -f Docker-Compose-Template/docker-compose-with-proxy.yml up -d
```

Connect using the standard emulator connection string:

```
Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;
```

The admin API remains directly accessible on port 5300.

## Quick Start (Standalone)

```bash
cd proxy
npm install
npm run build
BACKEND_HOST=localhost BACKEND_PORT=5672 PROXY_PORT=5671 node dist/index.js
```

Then connect your apps to port 5671 instead of 5672.

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `BACKEND_HOST` | `sb-emulator` | Hostname of the Service Bus Emulator |
| `BACKEND_PORT` | `5672` | AMQP port of the emulator |
| `MAX_BACKEND_CONNECTIONS` | `8` | Maximum backend connection pool size |
| `PROXY_PORT` | `5672` | Port the proxy listens on |
| `MAX_CLIENT_CONNECTIONS` | `100` | Maximum concurrent client connections |
| `MAX_FRAME_SIZE` | `1048576` | Maximum AMQP frame size (bytes) |
| `LOG_LEVEL` | `info` | Log level: fatal, error, warn, info, debug, trace |

## Architecture

The proxy uses the [rhea](https://github.com/amqp/rhea) AMQP 1.0 library (the same library used by Azure Service Bus SDKs).

**Modules:**

- `config.ts` — Zod-validated environment configuration
- `connection-pool.ts` — Backend connection pool with least-loaded routing and reconnection
- `link-relay.ts` — Link mapping, message forwarding, and settlement relay
- `cbs-handler.ts` — Local CBS authentication bypass (emulator ignores auth)
- `index.ts` — Entry point, event wiring, graceful shutdown

**Key design decisions:**

- **Link-level proxy:** Each client sender/receiver maps to a corresponding backend link
- **No message buffering:** 1:1 credit forwarding preserves backpressure semantics
- **End-to-end settlement:** Dispositions are never pre-settled, preserving delivery count and dead-lettering
- **CBS bypass:** The emulator ignores authentication, so `$cbs` token requests are handled locally
- **Stateless:** All message state lives in the emulator's SQL database

## Performance

Tested with 50 concurrent connections, 221K+ messages over 10 seconds, zero errors.

| Metric | Value |
|--------|-------|
| Max concurrent connections | 50+ tested, 100 default cap |
| Connection setup time (50 clients) | ~87ms |
| Message throughput | 22K+ msg/s |
| Backend pool size | 8 connections (configurable) |

## Troubleshooting

**Proxy can't connect to emulator:** The emulator takes 20-30 seconds to start. The proxy retries automatically for up to 2 minutes.

**`ConnectionsQuotaExceeded` still appearing:** You may be connecting directly to the emulator instead of the proxy. Verify your connection is going through the proxy port.

**High memory usage:** Check for connection leaks in your application. The proxy logs connection counts every 30 seconds at `info` level.

## Limitations

- **peekLock settlement**: `completeMessage()` / `abandonMessage()` in peekLock mode is not yet supported through the proxy. Use `receiveAndDelete` mode for consuming messages, or connect receivers directly to the emulator on port 5672 while sending through the proxy.
- CBS authentication is bypassed (development use only)
- Service Bus session affinity not yet implemented
- Transaction support is not yet implemented
- Python SDK may have issues with custom AMQP endpoints (use Node.js or .NET SDKs)
