# NordServer

Server for the online MMO Nordgame that closed down in 2014 (nordgame.com). Made by reverse engineering the client to figure out what messages the server needs to handle to allow the game to run.
This is a fan-made project and is not affiliated with or endorsed by SkyGoblin.


## Feature parity and known issues

- `Account creation`: Yes
- `Login`: Yes if valid. Broken on invalid credentials (client crash)
- `Village Building`: Yes. Coin spending is not accurate but can be configured in `building-costs.properties`.
- `Online list`: Yes
- `Village visiting`: Yes, full support with chat, location and village updates. 
- `Leveling flow`: Partly supported. Buggy growth cycles. Inventory and spin progression resets if you log out and back in but if you complete a level during one session, the new level is saved.
- `Tutorial`: Super buggy, current workaround is to create all new accounts at a higher level to skip the tutorial.
- `Everything else`: Most likely broken, some things look fine client side but are not synced to other players.

## This directory contains

- `NordServer.java`: main KryoNet game server entrypoint.
- `NordDatabase.java`: SQLite-backed persistence for users, villages, and related data.
- `nord_server.py`: small HTTP server used by the client for XML/time endpoints.
- `run_server.sh`: compile-and-run script for both Java + Python servers.

## Requirements

- Java (JDK with `javac` available)
- Python 3
- Client jars in parent directory (including `sqlite-jdbc.jar`)

`run_server.sh` builds classpath from `../*.jar`, so it expects jars one directory above `NordServer/`.

The Nord client needed to connect to the server can not be distributed through Github. But we're a helpful bunch of people in the [NordRevived Discord server](https://discord.gg/NkEUkvq6vh).

## Quick Start

From the project root:

```bash
./NordServer/run_server.sh
```

What the script does:

1. Recompiles `NordDatabase.java` and `NordServer.java` when sources are newer than classes.
2. Truncates `NordServer/server.log` and `NordServer/web.log`.
3. Starts `nord_server.py` (HTTP sidecar).
4. Starts Java `NordServer` and appends output to `server.log`.

Default runtime ports:

- Game server TCP: `41210`
- Game server UDP: `41211` (disabled unless enabled)
- HTTP sidecar: `127.0.0.1:8080`

## Configuration

Environment variables:

- `NORD_DB_PATH`: path to SQLite file. Default: `NordServer/nord-data.sqlite`
- `NORD_ENABLE_UDP`: set to `1` to enable UDP; default is TCP-only
- `NORD_HTTP_BIND`: bind host for Python HTTP server (default `127.0.0.1`)
- `NORD_HTTP_PORT`: bind port for Python HTTP server (default `8080`)

Java properties used by the launcher:

- `nord.db.path` (set by `run_server.sh` from `NORD_DB_PATH`)
- `nord.building.costs.path` (defaults to `NordServer/building-costs.properties` in launcher)

## Data and Logs

- `nord-data.sqlite`: primary persistent database file.
- `server.log`: Java server output.
- `web.log`: Python HTTP server output.

These are listed in `NordServer/.gitignore`.

## Troubleshooting

- `SQLite JDBC driver not found`:
  Ensure `sqlite-jdbc.jar` exists in the parent directory so `run_server.sh` includes it in classpath.

- `Address already in use`:
  Another process is already using one of the server ports. Stop it or change bind settings.

- No UDP traffic:
  Confirm `NORD_ENABLE_UDP=1` before launching.
