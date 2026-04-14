# 量化交易前端（Quant FrontWeb）

本项目是一个基于 `React + TypeScript + Vite + Ant Design` 的量化交易控制台，
用于对接 `backend/quant/api_server.py`（FastAPI）并提供以下能力：

- 策略管理：列表、创建、编辑、启动、停止
- 回测管理：创建回测、查看详情与日志
- 实盘/纸交易看板：权益、持仓、订单、成交
- 风控参数查看与更新
- 系统日志与健康状态
- WebSocket 实时推送（`/ws`）+ REST 回退行情

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

## 前端启动

```bash
cd frontweb/www.zlsjtj.tech
npm install
npm run dev
```

默认地址：`http://localhost:5173`

## 后端启动（联调）

```bash
cd backend/quant
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn api_server:app --host 0.0.0.0 --port 8000 --reload
```

健康检查：`http://localhost:8000/api/health`

## 环境变量

前端在 `src/utils/env.ts` 中读取以下变量（无 `.env.example`，请自行创建 `.env`）：

- `VITE_API_BASE_URL`：REST 基地址，默认 `/api`
- `VITE_WS_URL`：WS 地址，默认 `${ws|wss}://<当前host>/ws`
- `VITE_USE_MOCK`：是否启用本地 mock（`true/false`）
- `VITE_API_PROFILE`：`standard` 或 `quant-api-server`
- `VITE_MARKET_CONFIG_PATH`：行情接口默认配置文件名（默认 `config_market.yaml`）
- `VITE_MARKET_POLL_MS`：REST 行情轮询间隔（200~10000ms）
- `VITE_API_TOKEN`：可选，主要用于服务间调试；浏览器场景推荐使用后端会话登录（Cookie）

本地联调推荐：

```env
VITE_API_BASE_URL=http://localhost:8000/api
VITE_WS_URL=ws://localhost:8000/ws
VITE_USE_MOCK=false
VITE_API_PROFILE=quant-api-server
VITE_MARKET_CONFIG_PATH=config_market.yaml
VITE_MARKET_POLL_MS=1000
```

## 后端鉴权说明

当前推荐开启后端鉴权并使用会话 Cookie：

```bash
export API_AUTH_REQUIRED=true
export DASHBOARD_LOGIN_USERNAME=admin
export DASHBOARD_LOGIN_PASSWORD='your-strong-password'
export API_SESSION_SECRET='your-long-random-secret'
```

生产环境更推荐使用文件方式（避免明文出现在进程参数中）：

```bash
export API_AUTH_TOKEN_FILE=/path/to/api_auth_token
export DASHBOARD_AUTH_FILE=/path/to/dashboard_auth   # 格式: username:password
export API_SESSION_SECRET_FILE=/path/to/session_secret
```

前端登录页会调用 `/api/auth/login`，成功后由服务端下发 HttpOnly Cookie，后续 REST/WS 自动复用会话。

说明：当前推荐的 `VITE_API_PROFILE=quant-api-server` 模式下，前端创建/编辑策略、创建回测均走后端业务接口（`/api/strategies`、`/api/backtests`）并进入数据库持久化链路。

## 已对齐的核心接口

前端当前使用的主接口（均以 `/api` 为前缀）：

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

## 常用命令

```bash
npm run dev
npm run build
npm run preview
npm run typecheck
npm run lint
npm run test:run
```

## 常见问题

- 页面空白或请求 404：检查 `VITE_API_BASE_URL` 是否包含 `/api` 前缀。
- 前端 401：通常是未登录或会话过期；请先在登录页完成 `/api/auth/login`。
- 实时数据不更新：先检查 `ws://localhost:8000/ws` 是否可连，再看 `VITE_MARKET_POLL_MS` 是否过大。
- 登录失败：检查后端 `DASHBOARD_LOGIN_USERNAME / DASHBOARD_LOGIN_PASSWORD`（或 `DASHBOARD_AUTH_FILE`）配置。
