# NordServer

Reverse-engineered server for the original Nordgame client.
This is a fan-made project and is not affiliated with or endorsed by SkyGoblin.

This repository is intentionally **server-only**:
- Included: Java game server, Python HTTP sidecar, Node admin panel, smoke/soak tooling.
- Not included: client jars/resources/assets.

## Current State

Implemented and actively used in this codebase:
- KryoNet game server (`NordServer.java`) with TCP and optional UDP.
- SQLite persistence layer (`NordDatabase.java`).
- HTTP sidecar (`nord_server.py`) for client compatibility routes, forum API and uploads/share pages.
- Live protocol smoke test and reconnect-heavy checksum soak scripts.
- Optional admin panel for direct SQLite operations (`admin-panel/`).

Important scope note:
- This is not full historical Nord parity.
- Some legacy web routes intentionally return placeholders for client compatibility.

## Repository Layout

- `NordServer.java`: main game server.
- `NordDatabase.java`: SQLite access and schema management for core game data.
- `nord_server.py`: HTTP compatibility sidecar and web APIs.
- `run_server.sh`: launcher that compiles Java (when needed) and starts Java + Python services.
- `LiveProtocolSmoke.java`: protocol smoke client harness.
- `run_live_smoke.sh`: run one smoke pass against a live server.
- `run_checksum_soak.sh`: run repeated smoke passes against one long-lived server.
- `analyze_soak_logs.sh`: scan captured logs for out-of-sync/crash signatures.
- `admin-panel/`: standalone Node.js control panel.

## Requirements

- Linux/macOS shell environment
- Java JDK (`javac` + `java`)
- Python 3
- Client/runtime jars one directory above this repo root (classpath is built from `../*.jar`)

Runtime files expected from outside this repo:
- `../*.jar` (including `sqlite-jdbc.jar` and message model dependencies)
- `randomseed.dat` (provide either `./randomseed.dat` or `NORD_RANDOMSEED_PATH`)

We do absolutely not have any client side things on [NordRevived Discord](https://discord.gg/NkEUkvq6vh)

## Quick Start

From this repository root:

```bash
./run_server.sh
```

Default ports:
- `41210/tcp` game server
- `41211/udp` game server (only when enabled)
- `8080/tcp` primary HTTP sidecar
- `31876/tcp` optional legacy image sidecar (if different from HTTP port)

Logs:
- `server.log` (Java)
- `web.log` (Python)

## Configuration

### Core runtime

- `NORD_DB_PATH`: SQLite path (default `./nord-data.sqlite` when launched from this repo)
- `NORD_ENABLE_UDP`: `1` to enable UDP (`0` default)
- `NORD_RANDOMSEED_PATH`: path to `randomseed.dat` used by Java and HTTP sidecar

### Java gameplay tuning

- `NORD_LEVELUP_REWARD_BASE` / `NORD_LEVELUP_REWARD_STEP`
- `NORD_LEVELUP_REWARD` (legacy flat reward override)
- `NORD_BUILDINGS_CHECKSUM_ENABLED` (`1` enables checksum response messages)
- `NORD_BUY_CREDITS_REQUIRE_PROVIDER_CALLBACK`
- `NORD_CLIENT_VERSION_REQUIRED`
- `NORD_BUY_CREDITS_CODE_MAP`
- `NORD_SERVER_HOST`, `NORD_IMAGE_PORT`, `NORD_SERVER_IMAGE_PORT`, `NORD_IMAGE_PREFIX`

### HTTP sidecar

- `NORD_HTTP_BIND` (default `0.0.0.0`)
- `NORD_HTTP_PORT` (default `8080`)
- `NORD_IMAGE_PORT` (default `31876`)
- `NORD_UPLOAD_DIR` (default: `<dirname(NORD_DB_PATH)>/uploads`, so Docker defaults to `/data/uploads`)
- `NORD_UPLOAD_IMAGE_MAX_BYTES`
- `NORD_UPLOAD_IMAGE_MAX_WIDTH`
- `NORD_UPLOAD_IMAGE_MAX_HEIGHT`
- `NORD_UPLOAD_IMAGE_MAX_PIXELS`
- `NORD_UPLOAD_IMAGE_ALLOWED_TYPES` (`jpeg` and/or `png`)
- `NORD_TRACKING_URLS`
- `NORD_DYNAMIC_EVENTS_XML_PATH`
- `NORD_DYNAMIC_EVENTS_XML_INLINE`
- `NORD_HTTP_TRACE_UNKNOWN`
- `NORD_HTTP_TRACE_BODY_LIMIT`

## HTTP Endpoints (Implemented)

Core compatibility routes:
- `GET /randomseed.version`
- `GET /randomseed.dat`
- `GET /ServerTime.jsp`
- `GET /events/dynamic`
- `GET /clients/tracking.jsp`
- `GET /track/?data=...`
- `POST /forum/`
- `POST /uploadPicture`
- `POST /ul/` and `POST /delete/`
- `GET /share/<id>`
- `GET /uploads/<id>` and `GET /dl/<id>`


Legacy compatibility pages:
- `/`, `/jnlp/`, `/jnlp/<filename>`, `/support.jsp`, `/tutorial.jsp`, `/Referral.jsp`, `/HamsterRedirect.jsp`, `/coins/` and provider launch pages.

## Admin Panel

The admin panel lives in `admin-panel/` and is optional.

Run:

```bash
cd admin-panel
npm install
BASIC_AUTH_USERNAME=admin BASIC_AUTH_PASSWORD=change_me npm start
```

Default URL: `http://localhost:8091`

See `admin-panel/README.md` for available actions.

## Git Policy In This Repo

- This repo should contain server code and tooling only.
- Client assets/binaries are intentionally excluded from version control.
