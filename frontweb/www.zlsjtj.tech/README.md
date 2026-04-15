# SignalDeck Frontend

React frontend for the SignalDeck dashboard. It talks to `backend/quant/api_server.py` and covers the main operator views:

- Strategy list, create/edit flow, start/stop controls
- Backtest creation, details, logs, and charts
- Live/paper account views for equity, positions, orders, and fills
- Risk settings and risk history
- Runtime logs and health status
- REST polling and WebSocket updates from `/ws`

## 目录结构

```text
frontweb/www.zlsjtj.tech/
├─ src/
│  ├─ pages/                 # 页面
│  ├─ components/            # UI 组件
│  ├─ hooks/queries/         # react-query 数据查询
│  ├─ api/                   # API 适配层（standard / quant-api-server）
│  ├─ store/                 # zustand 全局状态
│  └─ utils/env.ts           # 环境变量解析
├─ package.json
├─ vite.config.ts
└─ README.md
```

后端默认联调目录：`backend/quant`

## Development

```bash
cd frontweb/www.zlsjtj.tech
npm install
npm run dev
```

Default dev server: `http://localhost:5173`.

## Backend For Local Testing

```bash
cd backend/quant
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn api_server:app --host 0.0.0.0 --port 8000 --reload
```

Health check: `http://localhost:8000/api/health`.

## Environment

The frontend reads these variables in `src/utils/env.ts`:

- `VITE_API_BASE_URL`：REST 基地址，默认 `/api`
- `VITE_WS_URL`：WS 地址，默认 `${ws|wss}://<当前host>/ws`
- `VITE_USE_MOCK`：是否启用本地 mock（`true/false`）
- `VITE_API_PROFILE`：`standard` 或 `quant-api-server`
- `VITE_MARKET_CONFIG_PATH`：行情接口默认配置文件名（默认 `config_market.yaml`）
- `VITE_MARKET_POLL_MS`：REST 行情轮询间隔（200~10000ms）
- `VITE_API_TOKEN`：optional; prefer backend session login with HttpOnly cookies for browser use

Local example:

```env
VITE_API_BASE_URL=http://localhost:8000/api
VITE_WS_URL=ws://localhost:8000/ws
VITE_USE_MOCK=false
VITE_API_PROFILE=quant-api-server
VITE_MARKET_CONFIG_PATH=config_market.yaml
VITE_MARKET_POLL_MS=1000
```

## Backend Auth

The dashboard supports backend session auth:

```bash
export API_AUTH_REQUIRED=true
export DASHBOARD_LOGIN_USERNAME=admin
export DASHBOARD_LOGIN_PASSWORD='your-strong-password'
export API_SESSION_SECRET='your-long-random-secret'
```

For deployments, file-based secrets are easier to keep out of shell history and process listings:

```bash
export API_AUTH_TOKEN_FILE=/path/to/api_auth_token
export DASHBOARD_AUTH_FILE=/path/to/dashboard_auth   # 格式: username:password
export API_SESSION_SECRET_FILE=/path/to/session_secret
```

The login page calls `/api/auth/login`. On success, the backend issues an HttpOnly cookie that is reused by REST and WebSocket requests.

With `VITE_API_PROFILE=quant-api-server`, strategy and backtest writes go through the backend API and database layer.

## API Calls Used By The Frontend

Main endpoints, all under `/api`:

- 鉴权：`POST /auth/login`、`GET /auth/status`、`POST /auth/logout`
- 健康：`GET /health`
- 策略：`GET /strategies`、`GET /strategies/{id}`、`POST /strategies/{id}/start|stop`、`GET /strategy/diagnostics`
- 回测：`GET /backtests`、`POST /backtests`、`GET /backtests/{id}`、`GET /backtests/{id}/logs`、`POST /backtest/start`
- 账户：`GET /portfolio /positions /orders /fills`
- 兼容回退：`GET /paper/equity`（当 `/portfolio` 不可用时）
- 风控：`GET/PUT /risk`
- 日志：`GET /logs`
- 行情：`GET /market/ticks`、`GET /market/klines`
- WebSocket：`WS /ws`（支持 `config_path`、`strategy_id`、`refresh_ms`）

补充：`standard` 与 `quant-api-server` 适配层都使用 `POST/PUT /strategies...` 与 `POST /backtests`。

## Commands

```bash
npm run dev
npm run build
npm run preview
npm run typecheck
npm run lint
npm run test:run
```

## Troubleshooting

- 页面空白或请求 404：检查 `VITE_API_BASE_URL` 是否包含 `/api` 前缀。
- 前端 401：通常是未登录或会话过期；请先在登录页完成 `/api/auth/login`。
- 实时数据不更新：先检查 `ws://localhost:8000/ws` 是否可连，再看 `VITE_MARKET_POLL_MS` 是否过大。
- 登录失败：检查后端 `DASHBOARD_LOGIN_USERNAME / DASHBOARD_LOGIN_PASSWORD`（或 `DASHBOARD_AUTH_FILE`）配置。
