# SignalDeck

A quant trading console for strategy runs, backtests, live state, risk controls, and audit logs.

SignalDeck is a full-stack dashboard I built to make a quant trading workflow easier to operate and inspect. It combines a React control surface with a FastAPI backend that can run strategies, track paper/live trading state, persist operational data, and expose the data through REST and WebSocket endpoints.

This is an engineering project, not a trading signal service. Nothing here should be treated as financial advice.

## What It Does

- Manage strategy records, configs, run state, and compile jobs
- Start, stop, and inspect strategy processes
- Create and review backtest runs, logs, trades, equity curves, and metrics
- Monitor portfolio equity, positions, orders, fills, and market data
- Update and audit risk controls such as drawdown and position limits
- Store strategies, backtests, risk state, audit logs, auth data, and time series data
- Expose health, metrics, REST APIs, and WebSocket live updates

## Stack

- Frontend: React 18, TypeScript, Vite, Ant Design, TanStack Query, Zustand, ECharts, Lightweight Charts
- Backend: FastAPI, Uvicorn, pandas, NumPy, ccxt, psycopg, PostgreSQL, SQLite for local smoke tests
- Testing: Vitest, Testing Library, pytest
- Ops: Nginx-friendly static build, supervisor-compatible backend process, health and metrics endpoints

## Repo Layout

```text
.
├── backend/quant/                 # FastAPI backend and strategy runtime
│   ├── api_server.py              # REST, WebSocket, auth, persistence endpoints
│   ├── main.py                    # Strategy runner entrypoint
│   ├── statarb/                   # Backtest and strategy modules
│   ├── db_store.py                # SQLite repository implementation
│   ├── postgres_store.py          # PostgreSQL repository implementation
│   ├── tests/                     # Backend tests
│   ├── tools/                     # E2E, backup, and maintenance tools
│   └── README.md                  # Backend-specific notes
├── frontweb/www.zlsjtj.tech/      # React frontend
│   ├── src/
│   ├── package.json
│   ├── vite.config.ts
│   └── README.md                  # Frontend-specific notes
└── README.md
```

## Local Setup

### Backend

The deployed setup uses PostgreSQL. For local development, SQLite is the fastest way to start the API without creating a database first.

```bash
cd backend/quant
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt

export API_DB_BACKEND=sqlite
export API_DB_PATH=./logs/dev_quant_api.db
export API_AUTH_REQUIRED=false

uvicorn api_server:app --host 127.0.0.1 --port 8000 --reload
```

Health check:

```bash
curl http://127.0.0.1:8000/api/health
```

For a PostgreSQL-backed run:

```bash
export API_DB_BACKEND=postgres
export API_DB_PATH=/dev/null
export API_DB_POSTGRES_DSN='postgresql://quant_user:password@127.0.0.1:5432/quant_db'
uvicorn api_server:app --host 127.0.0.1 --port 8000
```

### Frontend

```bash
cd frontweb/www.zlsjtj.tech
npm install
cat > .env.local <<'EOF'
VITE_API_BASE_URL=http://127.0.0.1:8000/api
VITE_WS_URL=ws://127.0.0.1:8000/ws
VITE_USE_MOCK=false
VITE_API_PROFILE=quant-api-server
VITE_MARKET_CONFIG_PATH=config_market.yaml
VITE_MARKET_POLL_MS=1000
EOF
npm run dev
```

Open `http://127.0.0.1:5173`.

## Useful Commands

Backend:

```bash
cd backend/quant
source .venv/bin/activate
python -m pytest -q
python -m py_compile api_server.py
```

Frontend:

```bash
cd frontweb/www.zlsjtj.tech
npm run lint
npm run typecheck
npm run test:run
npm run build
```

## API Surface

The frontend talks to the backend through `/api`:

- Auth: `POST /auth/login`, `GET /auth/status`, `POST /auth/logout`
- Health and metrics: `GET /health`, `GET /metrics`
- Strategies: `GET/POST /strategies`, `GET/PUT /strategies/{id}`, `POST /strategies/{id}/start`, `POST /strategies/{id}/stop`
- Backtests: `GET/POST /backtests`, `GET /backtests/{id}`, `GET /backtests/{id}/logs`
- Live state: `GET /portfolio`, `GET /positions`, `GET /orders`, `GET /fills`
- Risk: `GET/PUT /risk`, `GET /risk/history`, `GET /risk/events`
- Audit and logs: `GET /audit/logs`, `GET /logs`
- Market data: `GET /market/ticks`, `GET /market/klines`
- Live stream: `WS /ws`

## Notes For Open Sourcing

- Do not commit `.secrets/`, `.env.local`, database files, logs, generated backtest results, or virtual environments.
- Replace production hostnames, API tokens, and private config values before publishing.
- Trading exchange credentials should only be provided through local environment variables or secret files.
- The sample configs are for development and testing. Review risk settings before connecting anything to a real exchange account.

## Current Test Status

At the time this README was written:

- Backend: `107 passed, 5 skipped`
- Frontend: lint clean, typecheck passing, `8 passed`
- Production build: Vite build passing

## License

No license has been selected yet.
