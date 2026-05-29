# Deployment Guide

本文档给出一套适合当前仓库的生产部署方式：用 Docker 构建后端镜像，再用 `docker compose` 编排 API 和 PostgreSQL。

## 部署内容

- `Dockerfile`: 多阶段构建 Rust 后端镜像，运行时内置与 PostgreSQL major version 对齐的 `pg_dump` / `psql`
- `docker/entrypoint.sh`: 容器启动时等待数据库，并按文件名顺序执行 `migrations/*.sql`
- `docker-compose.prod.yml`: 生产编排文件，包含 `api` 和 `db`
- `compose.prod.env.example`: 生产环境变量示例

## 1. 构建镜像

在仓库根目录执行：

```bash
docker build --pull -t qb_api:latest .
```

如果你要推到镜像仓库，可以直接换成自己的 tag：

```bash
docker build --pull -t registry.example.com/cphos/qb_api:2026-04-05 .
docker push registry.example.com/cphos/qb_api:2026-04-05
```

对应地，把 `compose.prod.env.example` 里的 `QB_IMAGE_NAME` 和 `QB_IMAGE_TAG` 改成你的仓库地址和版本号即可。

如果你把 PostgreSQL major version 从默认的 `16` 改成别的版本，构建镜像时还要同步传入 `PG_MAJOR`：

```bash
docker build --pull --build-arg PG_MAJOR=16 -t qb_api:latest .
```

## 2. 准备环境变量

先复制一份示例文件：

```bash
cp compose.prod.env.example .env
```

至少要修改这些值：

- `POSTGRES_PASSWORD`
- `QB_DATABASE_URL`
- `QB_JWT_SECRET`
- `QB_CORS_ORIGINS`

可选但重要：

- `QB_POSTGRES_MAJOR`：默认为 `16`，需要和 `db` 服务使用的 PostgreSQL major version 保持一致

注意：

- `QB_DATABASE_URL` 必须和 `POSTGRES_DB`、`POSTGRES_USER`、`POSTGRES_PASSWORD` 保持一致
- `QB_POSTGRES_MAJOR` 必须和 `docker-compose.prod.yml` 中 `db` 服务实际运行的 PostgreSQL major version 保持一致，否则 `GET /database/backup` 使用的 `pg_dump` 可能报 `server version mismatch`
- 如果数据库密码里包含 `@`、`:`、`/` 之类特殊字符，需要做 URL 编码再写进 `QB_DATABASE_URL`
- `QB_JWT_SECRET` 请使用长随机字符串，生产环境不要沿用默认值

## 3. 启动生产环境

```bash
docker compose --env-file .env -f docker-compose.prod.yml up -d
```

如果你想跳过单独的 `docker build` 步骤，也可以直接：

```bash
docker compose --env-file .env -f docker-compose.prod.yml up -d --build
```

启动流程如下：

1. `db` 容器启动并通过健康检查
2. `api` 容器启动，等待 PostgreSQL 可连接
3. `api` 容器自动执行 `migrations/*.sql`
4. 后端服务监听 `0.0.0.0:8080`
5. 如果 `users` 表为空，会自动创建初始管理员账号 `admin / changeme`

首次上线后请立即登录并修改默认管理员密码。

如果需要自动化 bot，请在管理员界面创建 bot 账号并保存接口返回的 access token；后端不会再次展示该 token 明文。

## 4. 验证部署结果

查看容器状态：

```bash
docker compose --env-file .env -f docker-compose.prod.yml ps
```

查看后端日志：

```bash
docker compose --env-file .env -f docker-compose.prod.yml logs -f api
```

健康检查：

```bash
curl http://127.0.0.1:${QB_BIND_PORT:-8080}/health
```

正常情况下会返回：

```json
{"status":"ok","service":"qb_api_rust"}
```

## 5. 升级和重启

代码更新后重新部署：

```bash
git pull
docker build -t qb_api:latest .
docker compose --env-file .env -f docker-compose.prod.yml up -d
```

如果镜像来自外部仓库：

```bash
docker compose --env-file .env -f docker-compose.prod.yml pull
docker compose --env-file .env -f docker-compose.prod.yml up -d
```

停止服务：

```bash
docker compose --env-file .env -f docker-compose.prod.yml down
```

如果连数据卷也一起删除：

```bash
docker compose --env-file .env -f docker-compose.prod.yml down -v
```

这会删除 PostgreSQL 数据和导出目录，请谨慎执行。

## 6. 数据持久化

compose 文件里定义了两个命名卷：

- `qb_postgres_data`: PostgreSQL 数据目录
- `qb_exports`: `QB_EXPORT_DIR` 对应的导出目录，保存导出和质量检查输出

题目 zip、试卷 zip 和资源文件本身都保存在 PostgreSQL 的 `objects` 表里，不依赖本地文件系统。

## 7. 备份建议

备份数据库：

```bash
docker compose --env-file .env -f docker-compose.prod.yml exec -T db \
  sh -lc 'pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB"' > qb_backup.sql
```

如果你使用后端提供的 `GET /database/backup` 接口，底层也是调用 `pg_dump`；一旦看到 `server version mismatch`，说明 API 镜像里的 PostgreSQL client major version 和数据库不一致，需要调整 `QB_POSTGRES_MAJOR` 后重新构建并部署 API 镜像。

恢复数据库：

```bash
cat qb_backup.sql | docker compose --env-file .env -f docker-compose.prod.yml exec -T db \
  sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" "$POSTGRES_DB"'
```

注意：上面的备份文件是 `pg_dump` 导出的 plain SQL，恢复目标必须是空库；如果直接导入到已有数据的库里，会出现 "relation already exists" 和主键冲突。

如果你要覆盖当前库，建议按下面顺序操作：

```bash
docker compose --env-file .env -f docker-compose.prod.yml stop api
docker compose --env-file .env -f docker-compose.prod.yml exec -T db \
  sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" "$POSTGRES_DB" -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"'
cat qb_backup.sql | docker compose --env-file .env -f docker-compose.prod.yml exec -T db \
  sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" "$POSTGRES_DB"'
docker compose --env-file .env -f docker-compose.prod.yml start api
```

如果你只是想验证备份可恢复，建议恢复到一个临时数据库，而不是直接覆盖生产库：

```bash
docker compose --env-file .env -f docker-compose.prod.yml exec -T db \
  sh -lc 'createdb -U "$POSTGRES_USER" qb_restore_test'
cat qb_backup.sql | docker compose --env-file .env -f docker-compose.prod.yml exec -T db \
  sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" qb_restore_test'
```

验证完后删除测试库：

```bash
docker compose --env-file .env -f docker-compose.prod.yml exec -T db \
  sh -lc 'dropdb -U "$POSTGRES_USER" qb_restore_test'
```

如果你需要保留导出产物，也要同步备份 `qb_exports` 这个卷。

### 手动备份还原（绕过 API）

如果备份文件较大或你的反向代理有请求体大小限制，可以直接在服务器上操作，不经过 HTTP 接口。

后端 `GET /database/backup` 接口生成的备份是一个 `tar.gz` 压缩包，内部结构为：

```
qb_backup_YYYYMMDD_HHMMSS.tar.gz
├── metadata.sql    # pg_dump 导出的完整 SQL
└── objects/        # 文件对象存储目录的副本
```

#### 手动创建备份

```bash
# 1. 导出数据库 SQL
docker compose --env-file .env -f docker-compose.prod.yml exec -T db \
  sh -lc 'pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB"' > metadata.sql

# 2. 复制 objects 卷内容（如果使用了文件对象存储）
docker compose --env-file .env -f docker-compose.prod.yml cp api:/app/data/objects ./objects

# 3. 打包成 tar.gz
tar czf "qb_backup_$(date +%Y%m%d_%H%M%S).tar.gz" metadata.sql objects/

# 4. 清理临时文件
rm -f metadata.sql && rm -rf objects/
```

#### 手动还原备份

```bash
# 1. 停止 API 服务避免写入冲突
docker compose --env-file .env -f docker-compose.prod.yml stop api

# 2. 解压备份包
mkdir -p /tmp/qb_restore && tar xzf qb_backup_*.tar.gz -C /tmp/qb_restore

# 3. 清空并恢复数据库
docker compose --env-file .env -f docker-compose.prod.yml exec -T db \
  sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" "$POSTGRES_DB" -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"'
cat /tmp/qb_restore/metadata.sql | docker compose --env-file .env -f docker-compose.prod.yml exec -T db \
  sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" "$POSTGRES_DB"'

# 4. 恢复 objects 文件（如果备份中包含）
if [ -d /tmp/qb_restore/objects ]; then
  docker compose --env-file .env -f docker-compose.prod.yml cp /tmp/qb_restore/objects/. api:/app/data/objects/
fi

# 5. 重新启动 API 服务（entrypoint 会自动执行 migrations）
docker compose --env-file .env -f docker-compose.prod.yml start api

# 6. 清理临时文件
rm -rf /tmp/qb_restore
```

#### 还原旧的纯 SQL 备份

如果你的备份文件是早期版本导出的纯 `qb_backup.sql`（非 tar.gz 格式），直接还原即可：

```bash
docker compose --env-file .env -f docker-compose.prod.yml stop api
docker compose --env-file .env -f docker-compose.prod.yml exec -T db \
  sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" "$POSTGRES_DB" -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"'
cat qb_backup.sql | docker compose --env-file .env -f docker-compose.prod.yml exec -T db \
  sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" "$POSTGRES_DB"'
docker compose --env-file .env -f docker-compose.prod.yml start api
```

## 8. 运维说明

- 当前 compose 文件默认只部署单实例 `api`
- 由于容器启动时会自动执行 migration，如果未来要扩成多副本，建议把 migration 拆成独立 Job 或手动步骤
- 对外提供服务时，建议在前面加 Nginx、Traefik 或云负载均衡，统一处理 HTTPS 和域名
- 如果只允许前端域名访问 API，请把 `QB_CORS_ORIGINS` 配成明确的生产域名列表

## 9. 迁移说明

### `0002_role_system.sql` — 5 角色权限系统

该迁移将旧的 3 角色体系（viewer / editor / admin）升级为 5 角色体系（viewer / user / leader / bot / admin）。容器启动时会自动执行 `migrations/*.sql`，无需手动操作。

**迁移内容**：

1. 扩展 `users.role` 约束为 5 个值，现有 `editor` 自动迁移为 `user`
2. 新增 `users.leader_expires_at` 列（Leader 角色过期时间）
3. 新增 `questions.created_by` 和 `papers.created_by` 列（所有权追踪）
4. 新增 `question_difficulties.created_by` / `updated_by` 列（难度标签编辑追踪）
5. 创建 `question_reviews` 表（审阅人分配）

**注意事项**：

- 迁移后，旧的 `editor` 角色用户会自动变为 `user` 角色
- 历史数据的 `created_by` 字段为 `NULL`，不影响功能
- 如果使用备份恢复，确保备份文件是迁移后的版本，否则需重新执行迁移

### `0003_bot_access_tokens.sql` — bot access token 认证

该迁移将 bot 账号从“密码登录”切换为“管理员签发的长期 access token”模式。

**迁移内容**：

1. `users.password_hash` 改为可空，供 bot 账号禁用密码登录
2. 新增 `users.bot_token_hash` 和 `users.bot_token_created_at`
3. 使已有 bot 的密码哈希和 refresh token 失效，避免继续走旧登录链路
