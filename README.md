# CPHOS Question Bank

Rust + Axum + PostgreSQL 题库服务。当前版本的核心流程是：

1. 上传单题 zip 压缩包导入题目
2. 用一组有序题目引用创建试卷
3. 删除走软删除，管理员可恢复记录并回收无引用 binary

## API 文档

- 各模块的 API 文档位于 `src/api/<module>/API.md`
- 合并版文档：`docs/API.md`（由 `python scripts/build_api_doc.py` 自动生成）

## 项目架构

```text
src/
├── api/
│   ├── admin/
│   │   ├── API.md
│   │   ├── handlers.rs
│   │   ├── models.rs
│   │   └── queries.rs
│   ├── mod.rs
│   ├── questions/
│   │   ├── API.md
│   │   ├── bundles.rs
│   │   ├── handlers.rs
│   │   ├── imports.rs
│   │   ├── models.rs
│   │   └── queries.rs
│   ├── papers/
│   │   ├── API.md
│   │   ├── bundles.rs
│   │   ├── handlers.rs
│   │   ├── imports.rs
│   │   ├── models.rs
│   │   └── queries.rs
│   ├── ops/
│   │   ├── API.md
│   │   ├── exports.rs
│   │   ├── handlers.rs
│   │   ├── models.rs
│   │   ├── paper_render.rs
│   │   └── quality.rs
│   ├── system/
│   │   ├── API.md
│   │   └── handlers.rs
│   └── shared/
│       ├── details.rs
│       ├── error.rs
│       ├── multipart.rs
│       ├── mod.rs
│       └── utils.rs
├── config.rs
├── db.rs
├── lib.rs
└── main.rs
```

## 数据模型

- `objects`
  单表保存任意上传文件的元数据与二进制内容。
- `users`
  用户表，含 `role`（viewer / user / leader / bot / admin）、`leader_expires_at` 等字段。
- `questions`
  保存题目固定 metadata，以及软删除字段 `deleted_at` / `deleted_by` 和 `created_by`（创建者）。
- `question_files`
  保存题目的 TeX 文件和资源文件引用。
- `question_tags`
  保存题目标签列表。
- `question_difficulties`
  保存每个 difficulty tag 的 `score` / `notes`，以及 `created_by` / `updated_by`（追踪编辑者）。
- `question_reviews`
  审阅人分配表，记录哪些 user 或 leader 被分配为某题目的审阅人。
- `papers`
  保存试卷固定元数据，以及软删除字段 `deleted_at` / `deleted_by` 和 `created_by`（创建者）。
- `paper_questions`
  保存试卷和题目的有序关联。

## 题目 zip 格式

上传文件大小限制 20 MiB，使用 `multipart/form-data`，字段名是 `file`。zip 总解压体积限制 64 MiB。

zip 会按“逻辑根目录”解析：如果最外层只有一个包裹目录，会先自动剥离一层。逻辑根目录建议如下：

```text
question.zip
├── problem.tex
├── README.txt        # 可选，忽略
└── assets/
    ├── figure1.png
    └── ...
```

其中：

- 逻辑根目录必须恰好有一个 `.tex` 文件
- 可选一个 `assets/` 目录
- 逻辑根目录下其他杂散文件会被忽略，不参与导入
- 除根目录 tex / 杂散文件外，其他被导入的非根目录文件都必须位于 `assets/` 下
- 拒绝路径穿越（`..`）和绝对路径
- 总解压体积不能超过 64 MiB
- 真正写入 `objects` 表的只有 tex 和 `assets/` 下的资源文件
- 上传题目时必须额外提供一个非空的 `description`
- 上传题目时必须额外提供一个 `difficulty` JSON 字符串，且至少包含 `human`
- 上传题目时也可以一次性提供完整 metadata：
  - `category` 可选，取值 `none`、`T`、`E`
  - `tags` 可选，传 JSON 字符串数组
  - `status` 可选，取值 `none`、`reviewed`、`used`
- `description` 支持中文，并可直接参与 `GET /questions?q=...` 的模糊匹配
- `description` 会参与 bundle 目录命名，因此不能包含 `/ \\ : * ? " < > |`，不能是 `.`、`..`，也不能以 `.` 结尾
- 如果未提供可选 metadata，上传时其余字段使用默认值：
  - `category = "none"`
  - `tags = []`
  - `status = "none"`
  - `created_at = NOW()`

## 核心 API

### 上传题目

`POST /questions`

请求示例：

```bash
curl -X POST http://127.0.0.1:8080/questions \
  -F "description=热学标定题" \
  -F "category=T" \
  -F 'tags=["thermal","calibration"]' \
  -F "status=reviewed" \
  -F 'difficulty={"human":{"score":5,"notes":"baseline"}}' \
  -F "file=@question.zip"
```

### 更新题目 metadata

`PATCH /questions/{question_id}`

请求体示例：

```json
{
  "category": "T",
  "description": "demo question",
  "tags": ["optics", "mechanics"],
  "status": "reviewed",
  "difficulty": {
    "human": {
      "score": 7,
      "notes": "sample"
    },
    "algo1": {
      "score": 6
    }
  }
}
```

说明：

- 请求体支持部分更新
- 推荐在 `POST /questions` 时就提交完整 metadata；`PATCH` 主要用于后续修正 metadata
- 服务端会先锁定目标题目的主记录；同一题目的 metadata 更新、文件替换和删除会串行执行，避免并发重建 `tags` / `difficulty` 时出现竞态
- `description` 如果出现在更新请求里，必须是非空字符串，并满足上面的文件名安全限制
- `tags` 传空数组会清空
- `difficulty` 如果出现在更新请求里，会整体替换整组 difficulty tag
- `difficulty` 必须至少包含 `human`
- `difficulty.<tag>.score` 要求在 `1..=10`
- `difficulty.<tag>.notes` 是可选字符串，空串会被规范化为 `null`
- `category` 只能是 `none`、`T`、`E`
- `status` 只能是 `none`、`reviewed`、`used`

### 删除题目

`DELETE /questions/{question_id}`

说明：

- 这是软删除，不会立刻删除 binary
- 如果题目仍被未软删除试卷引用，会返回 `409`

### 批量下载题目包

`POST /questions/bundles`

请求体示例：

```json
{
  "question_ids": [
    "8db0d12e-2968-4ede-86d5-1dc5ff0a5d10",
    "e21ed70d-cd18-45cc-89ab-2785d07f4de7"
  ]
}
```

返回一个 zip，根目录附带 `manifest.json`，并按 `description_uuid前缀/` 目录分组题目文件。

### 创建试卷

`POST /papers`

使用 `multipart/form-data` 上传初始化 metadata 和附加 zip。

示例：

```bash
curl -X POST http://127.0.0.1:8080/papers \
  -F 'description=demo paper' \
  -F 'title=Demo Paper' \
  -F 'subtitle=Initial Draft' \
  -F 'authors=["Alice","Bob"]' \
  -F 'reviewers=["Carol"]' \
  -F 'question_ids=["8db0d12e-2968-4ede-86d5-1dc5ff0a5d10","e21ed70d-cd18-45cc-89ab-2785d07f4de7"]' \
  -F 'file=@paper_appendix.zip;type=application/zip'
```

题目顺序由 `question_ids` 数组顺序决定。

说明：

- `description` 为必填，必须是非空字符串
- `description` 同样会参与 bundle 目录命名，因此也要满足上面的文件名安全限制
- `title`、`subtitle` 为必填非空字符串
- `authors`、`reviewers` 为 JSON 字符串数组
- `POST /papers` 必须带一个合法 zip，服务端会原样存成附加 binary
- `GET /papers` 支持通过 `question_id`、`category`、`tag`、`q`、`limit`、`offset` 做组合查询
- `q` 会模糊匹配试卷的 `description`、`title`、`subtitle`、`authors`、`reviewers`

### 更新试卷

`PATCH /papers/{paper_id}`

更新 metadata，也支持通过 `question_ids` 重排题目列表。支持字段：`description`、`title`、`subtitle`、`authors`、`reviewers`、`question_ids`。

### 删除试卷

`DELETE /papers/{paper_id}`

说明：

- 这是软删除，不会立刻删除 appendix binary

### 批量下载试卷包

`POST /papers/bundles`

请求体示例：

```json
{
  "paper_ids": [
    "79bf1ce3-6b61-4e5f-82ab-29e5c7cb2942",
    "8ff430a0-92aa-463b-bf0f-b244a6697bf4"
  ]
}
```

返回一个 zip，根目录附带 `manifest.json`，并按 `paperDescription_uuid前缀/questionDescription_uuid前缀/` 目录展开题目文件。每个试卷目录还会附带一个重命名后的 `append.zip`，内容就是创建试卷时上传的那个 zip。

### 查询、打包与运维

- `GET /papers`
- `GET /papers/{paper_id}`
- `POST /papers/bundles`
- `GET /questions`
- `GET /questions/tags`
- `GET /questions/difficulty-tags`
- `GET /questions/{question_id}`
- `GET /questions/{question_id}/reviewers`
- `POST /questions/{question_id}/reviewers`
- `DELETE /questions/{question_id}/reviewers/{reviewer_id}`
- `POST /questions/bundles`
- `GET /admin/questions`
- `GET /admin/questions/{question_id}`
- `POST /admin/questions/{question_id}/restore`
- `GET /admin/papers`
- `GET /admin/papers/{paper_id}`
- `POST /admin/papers/{paper_id}/restore`
- `POST /admin/garbage-collections/preview`
- `POST /admin/garbage-collections/run`
- `POST /exports/run`
- `POST /quality-checks/run`

`GET /questions` 额外支持：

- `difficulty_tag`
- `difficulty_min`
- `difficulty_max`
- `created_after` / `created_before`
- `updated_after` / `updated_before`

其中 `difficulty_min` / `difficulty_max` 需要和 `difficulty_tag` 一起使用。日期参数支持 `YYYY-MM-DD` 或 RFC 3339 格式。

`GET /papers` 同样支持 `created_after` / `created_before` / `updated_after` / `updated_before` 日期范围筛选。

`GET /questions/tags` 返回：

- 所有未软删除题目的去重 tag 列表
- 响应格式为 `{"tags": ["..."]}`
- 按字典序升序返回，适合前端做输入联想和快速选择

`GET /questions/difficulty-tags` 返回：

- 所有未软删除题目的去重难度标签（algorithm_tag）列表
- 响应格式为 `{"difficulty_tags": ["..."]}`
- 按字典序升序返回，适合前端做下拉选择；建议默认选中 `human`

说明：

- 普通 `/questions` 和 `/papers` 接口默认只返回未软删除记录
- `POST /questions/bundles` 和 `POST /papers/bundles` 需要 `viewer` 及以上角色
- `POST /exports/run` 和 `POST /quality-checks/run` 仍然只允许 `editor` 及以上角色
- 管理员查询、恢复和垃圾回收见 [Admin API](./src/api/admin/API.md)
- `POST /exports/run` 只导出未软删除题目
- `POST /quality-checks/run` 只检查未软删除题目和试卷

## 角色与权限

系统采用 5 级角色体系，基于能力而非线性层级：

| 角色 | 说明 |
|---|---|
| `viewer` | 只读 + bundle 下载 |
| `user` | 可上传题目，编辑自己创建的题目，可被分配为审阅人 |
| `leader` | 可创建/编辑/删除题目和试卷（非 used），可分配审阅人；有过期时间，过期后降级为 user |
| `bot` | 同 leader 权限，无过期时间，用于自动化程序 |
| `admin` | 全部权限 + ops + 用户管理 + 垃圾回收 |

Leader 角色在创建时必须指定 `leader_expires_at` 过期时间。JWT 中间件会在每次请求时检查过期时间，过期后自动将角色降级为 `user`。

## 启动

```bash
export QB_DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/qb'
export QB_BIND_ADDR='127.0.0.1:8080'
export QB_EXPORT_DIR='./exports'            # 导出文件根目录（默认 ./exports）
# export QB_MAX_DB_CONNECTIONS=10           # 可选，连接池上限
# export QB_CORS_ORIGINS='http://localhost:3000,http://localhost:5173'  # 可选，CORS 白名单
# export QB_JWT_SECRET='qb-dev-secret-change-me-in-production'          # 生产环境必须修改
for file in migrations/*.sql; do
  psql "$QB_DATABASE_URL" -v ON_ERROR_STOP=1 -f "$file"
done
cargo run
```

## Docker 部署

仓库现在提供了完整的生产部署样例：

- [Dockerfile](./Dockerfile)
- [docker-compose.prod.yml](./docker-compose.prod.yml)
- [compose.prod.env.example](./compose.prod.env.example)
- [DEPLOYMENT.md](./docs/DEPLOYMENT.md)

快速启动：

```bash
cp compose.prod.env.example .env
docker build -t qb_api:latest .
docker compose --env-file .env -f docker-compose.prod.yml up -d
```

部署说明、升级方式、备份建议和生产注意事项见 [DEPLOYMENT.md](./docs/DEPLOYMENT.md)。

## 测试

单元与集成测试：

```bash
cargo test
```

端到端测试（基于 pytest，会自动启动 Docker PostgreSQL 和 API 进程）：

```bash
cd scripts && python3 -m pytest tests/ -v
```

测试覆盖：

| 模块 | 覆盖内容 |
|------|----------|
| `test_0_auth` | 登录、token 刷新、权限矩阵、5 角色权限检查、leader 过期降级 |
| `test_1_questions` | 题目 CRUD、标签/难度标签列表、difficulty 筛选、日期范围筛选、文件替换、bundle、审阅人管理 |
| `test_2_papers` | 试卷 CRUD、日期范围筛选、试卷 bundle、题目排序与渲染、所有权校验 |
| `test_3_ops` | 导出、质量检查、数据库备份恢复、权限验证 |
| `test_4_admin` | 软删除、恢复、垃圾回收 |

如果已有运行中的 PostgreSQL 和 API 实例，可跳过自动基础设施启动：

```bash
QB_E2E_SKIP_INFRA=1 API_PORT=8080 POSTGRES_PORT=5432 python3 -m pytest tests/ -v
```

## 数据库格式

表结构定义在 [0001_init_pg.sql](./migrations/0001_init_pg.sql)。5 角色权限系统和所有权追踪的迁移在 [0002_role_system.sql](./migrations/0002_role_system.sql)。
