# Nord Admin Panel

Small standalone Node.js control panel for editing live NordServer SQLite data.

## Features

- Search players by username, village, user id, or village id
- Edit `users.level`
- Edit `users.slx_credits` (coin balance)
- Edit village theme (`village_heightmaps.type`)
- Reset a player's village to a clean slate (clears village objects + custom heightmap)
- Toggle role badges: `VIP`, `Moderator`, `Admin`
- Configure slash-command permissions (`/setcoins`, `/setlevel`) as `everyone`, `vip`, or `mod`
- Queue a global in-game modal popup for all currently online players
- Upsert and delete rows in `achievements` (used as badges)
- Plain HTML/JavaScript frontend

## Location

- App root: `NordServer/admin-panel`
- Static UI: `NordServer/admin-panel/public`
- API server: `NordServer/admin-panel/server.js`

## Run

From repository root:

```bash
cd NordServer/admin-panel
npm install
npm start
```

Open `http://localhost:8091`.

## Config

- `PORT` (default `8091`)
- `NORD_DB_PATH` (default `../nord-data.sqlite` relative to this folder)
- `BASIC_AUTH_USERNAME` (required)
- `BASIC_AUTH_PASSWORD` (required)

Example:

```bash
PORT=8095 \
NORD_DB_PATH=/home/tim/Documents/Nordgame/NordServer/nord-data.sqlite \
BASIC_AUTH_USERNAME=admin \
BASIC_AUTH_PASSWORD=change_me \
npm start
```

Or copy `.env.example` to `.env` and edit the values.

## Notes

- This tool is protected with HTTP Basic Auth.
- Changes are immediate in the SQLite database.
- Role badge mappings used by the panel:
  - `VIP` => `category=6`, `type=0`
  - `Admin` => `category=6`, `type=1`
  - `Moderator` => `category=6`, `type=2`
