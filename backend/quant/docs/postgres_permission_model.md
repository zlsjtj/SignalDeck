# PostgreSQL 权限模型模板（最小权限 + 报表只读 + 凭据轮换）

## 1. 目标

为 `quant` 后端提供可直接落地的 PostgreSQL 权限模型模板，满足：

- 业务写账号最小权限（DML，不授予 DDL/超级权限）
- 报表账号只读权限（SELECT）
- 凭据可轮换（支持双账号平滑切换）

模板文件：

- `ops/postgres/postgres_permission_model_template.sql`

## 2. 角色设计

- `quant_app_rw`：`NOLOGIN` 组角色，承载应用写权限。
- `quant_report_ro`：`NOLOGIN` 组角色，承载报表只读权限。
- `quant_app_login`：`LOGIN` 账号，授予 `quant_app_rw`。
- `quant_report_login`：`LOGIN` 账号，授予 `quant_report_ro`。

这种“组角色 + 登录角色”模式可以把权限边界与凭据解耦，便于轮换与审计。

## 3. 最小权限原则

模板默认只授予：

- `CONNECT`（数据库）
- `USAGE`（schema）
- `SELECT/INSERT/UPDATE/DELETE`（应用写角色）
- `SELECT`（报表只读角色）
- 对 `SEQUENCE` 的必要 `USAGE/SELECT/UPDATE`

模板不授予：

- `SUPERUSER`
- `CREATEDB`
- `CREATEROLE`
- `REPLICATION`

## 4. 凭据轮换流程（推荐）

1. 创建新登录账号（如 `quant_app_login_next`），授予同组角色。
2. 更新应用密钥/连接串切到新账号。
3. 验证写入与报表链路。
4. 回收旧账号授权并删除旧账号。

模板中已包含对应 SQL 示例。

## 5. 使用建议

- 将 `CHANGE_ME_*` 密码占位符替换为密钥管理系统注入值。
- 在变更窗口执行，先在预发库验证后再应用生产。
- 每次 schema 新增对象后，确认 `ALTER DEFAULT PRIVILEGES` 与 owner 一致。
