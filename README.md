# CPHOS Question Bank V1

这是一个面向 CPHOS 内部题目整理、统计、检索与导出的题库源码仓。当前版本采用“服务器文件 staging + SQLite 内容快照 + 内部 API 驱动”的结构：源码仓保存 schema、脚本、API、样例与文档；生产环境中的 LaTeX 试卷、题目文件、题图、xlsx 统计工作簿和运行日志放在服务器目录中，由内部 API 读取服务器路径并写入数据库。

## 整体框架
- 导入单元不是单个字符串，而是一套服务器上的 bundle 目录。bundle 中包含 `manifest.json`、题目 JSON、主试卷 `.tex`、题目 `.tex`、答案 `.tex`、图片和可选 xlsx 工作簿。
- 数据库中同时保存“来源路径”和“内容快照”：
  - `papers` 保存 `paper_latex_path` 和 `paper_latex_source`
  - `questions` 保存 `latex_path`、`latex_source`、`answer_latex_path`、`answer_latex_source`
- 路径用于追溯原始文件和后续回写；LaTeX 源文本用于查询、导出、比对、审计和脱离文件系统后的数据迁移。
- `score_workbooks` 直接保存原始 xlsx 二进制内容，同时保留文件名、sheet 名称和哈希；`question_stats` 保存结构化统计结果，并通过 `source_workbook_id` 回链到原始工作簿。

## 为什么数据库里既存路径也存 LaTeX 文本
只存路径的问题是数据库会过度依赖服务器文件系统：一旦目录调整、文件移动、权限变化或需要跨环境导出，数据库本身并不自足。只存 LaTeX 文本的问题是来源追踪会变差，不方便重新编译、对照原始工程和回写。

当前实现选择双存：
- 路径负责来源定位、回写和定位原始工程文件。
- LaTeX 文本负责数据库层面的完整快照，使 API 查询、内部 JSONL 导出、相似题去重、备份迁移都不依赖外部目录仍能成立。

对于 CPHOS 当前题量，这种设计的存储成本很低，但稳定性会明显更高。

## 仓库结构
- `question_bank/`: 核心 Python 包，包含 schema、导入、统计、导出、难度评分、workbook 存储和查询逻辑。
- `scripts/`: 命令行入口，用于本地或服务器运维脚本化执行。
- `api/`: FastAPI 应用，提供内部导入、查询、导出接口。
- `samples/demo_bundle/`: 样例 bundle，演示 LaTeX 试卷、题目、答案和 xlsx workbook 的入库方式。
- `assets/`: 样例图片或开发环境静态资源。
- `docs/`: 字段字典、录入规范、维护手册、部署说明、FAQ。
- `docs/rust_rearchitecture_proposal.md`: Rust + PostgreSQL + 对象存储的重构建议与迁移路线。
- `tests/`: 围绕样例库的基础测试。
- `rust/`: Rust 重构起步目录（`axum` API + PostgreSQL migration 草案）。

## 服务器工作流
### 1. 在服务器上准备一套 bundle
推荐 bundle 结构如下：

```text
bundle_root/
  manifest.json
  questions/
    *.json
  latex/
    papers/
    questions/
    answers/
  images/
  score_workbooks/
```

其中：
- `manifest.json` 记录试卷级元数据和可选 workbook 清单。
- `questions/*.json` 记录 `question_id`、`paper_index`、题型、LaTeX 路径、答案路径、标签等结构化字段。
- `latex/` 下放真实 `.tex` 文件；导入时服务会读取这些文件内容并写入数据库。

### 2. 先校验，再正式导入 bundle
内部 API 工作流如下：

```bash
curl -X POST http://127.0.0.1:8000/imports/bundle/validate \
  -H "Content-Type: application/json" \
  -d '{"bundle_path": "/srv/cphos/raw/incoming/paper_18_regular"}'

curl -X POST http://127.0.0.1:8000/imports/bundle/commit \
  -H "Content-Type: application/json" \
  -d '{"bundle_path": "/srv/cphos/raw/incoming/paper_18_regular"}'
```

导入完成后，数据库会同时写入：
- 试卷路径与主试卷 LaTeX 文本
- 题目路径与题目 LaTeX 文本
- 答案路径与答案 LaTeX 文本
- 图片索引
- 题目顺序索引
- 导入日志
- manifest 中声明的 xlsx 工作簿

### 3. 如有单独补录的 workbook，可单独导入
```bash
curl -X POST http://127.0.0.1:8000/imports/workbooks/commit \
  -H "Content-Type: application/json" \
  -d '{
    "workbook_path": "/srv/cphos/raw/workbooks/评测试卷列表2025-04-07.xlsx",
    "paper_id": "CPHOS-18-REGULAR",
    "exam_session": "2025-04-07",
    "workbook_kind": "paper_registry",
    "workbook_id": "WB-2025-04-07-PAPER-REGISTRY"
  }'
```

### 4. 导入结构化统计，并关联原始 workbook
```bash
curl -X POST http://127.0.0.1:8000/imports/stats/commit \
  -H "Content-Type: application/json" \
  -d '{
    "csv_path": "/srv/cphos/raw/stats/paper_18_regular_scores.csv",
    "stats_source": "score_pipeline_v1",
    "stats_version": "2026-03-15",
    "source_workbook_id": "WB-2025-04-07-PAPER-REGISTRY"
  }'
```

### 5. 查询数据库中的试卷、题目和工作簿
```bash
curl http://127.0.0.1:8000/papers
curl http://127.0.0.1:8000/papers/CPHOS-18-REGULAR
curl "http://127.0.0.1:8000/questions?paper_id=CPHOS-18-REGULAR&category=theory"
curl http://127.0.0.1:8000/score-workbooks/WB-2025-04-07-PAPER-REGISTRY
```

### 6. 导出数据库内容
内部导出推荐用 JSONL；该格式会保留 LaTeX 源文本快照。CSV 导出更轻量，主要用于索引或半公开分发。

```bash
curl -X POST http://127.0.0.1:8000/exports/run \
  -H "Content-Type: application/json" \
  -d '{"format": "jsonl", "public": false}'

curl -X POST http://127.0.0.1:8000/exports/run \
  -H "Content-Type: application/json" \
  -d '{"format": "csv", "public": true}'
```

### 7. 运行质量检查
```bash
curl -X POST http://127.0.0.1:8000/quality-checks/run \
  -H "Content-Type: application/json" \
  -d '{}'
```

## 配置方式
生产路径不写死在仓库中，通过环境变量注入：
- `QUESTION_BANK_DB_PATH`: 服务器上 SQLite 文件路径。
- `QUESTION_BANK_ASSETS_DIR`: 服务器上题图资产根目录。
- `QUESTION_BANK_RAW_DIR`: 服务器上原始资料和 staging bundle 根目录。
- `QUESTION_BANK_EXPORTS_DIR`: 导出目录。

本地未设置这些变量时，项目会回退到仓库内的样例路径，便于开发和测试。

## CLI 快速开始
命令行脚本和内部 API 对应同一套能力，适合调试、批处理和运维：

```bash
python scripts/init_db.py
python scripts/validate_bundle.py samples/demo_bundle
python scripts/import_bundle.py samples/demo_bundle --commit
python scripts/import_stats.py samples/demo_bundle/stats/raw_scores.csv --stats-source sample_scores --stats-version demo-v1 --source-workbook-id WB-CPHOS-18-DEMO-INDEX
python scripts/calculate_difficulty.py --method-version demo-baseline
python scripts/export_data.py --format jsonl
python scripts/check_data_quality.py
```

## API 启动
```bash
pip install -r requirements.txt
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

可用接口：
- `GET /health`
- `GET /papers`
- `GET /papers/{paper_id}`
- `GET /questions`
- `GET /questions/{question_id}`
- `GET /score-workbooks`
- `GET /score-workbooks/{workbook_id}`
- `GET /score-workbooks/{workbook_id}/download`
- `GET /search`
- `POST /imports/bundle/validate`
- `POST /imports/bundle/commit`
- `POST /imports/workbooks/commit`
- `POST /imports/stats/commit`
- `POST /exports/run`
- `POST /quality-checks/run`

## 样例内容
项目自带 3 道示例题和 1 份示例 xlsx 工作簿：
- 理论题：滑块-斜面动力学
- 实验题：单摆测重力加速度
- 实验题：加热电阻的能量评估
- 统计工作簿：`demo_score_index.xlsx`

这些样例仅用于本地开发验证，不代表生产库位置或生产部署方式。


## Rust 重构（Phase 1 起步）
当前仓库已新增 `rust/` 目录，用于承接“Rust + PostgreSQL + 对象存储”重构第一阶段：
- `rust/qb_api`: Rust 只读查询 API，已迁移 `/health`、`/papers`、`/papers/{paper_id}`、`/questions`、`/questions/{question_id}`、`/score-workbooks`、`/score-workbooks/{workbook_id}`。
- `rust/migrations/0001_init_pg.sql`: PostgreSQL 初始 schema 草案，已补齐对象、题目、题目资产、统计、难度与成绩工作簿等核心查询表，开始从 `*_path + *_source/blob` 迁移到对象引用（`object_id`）。

本地运行（需要本机有 Rust 工具链）：
```bash
cd rust
cargo test
```

启动服务前需要配置环境变量：
- `QB_DATABASE_URL`（例如 `postgres://postgres:postgres@localhost:5432/qb`）
- `QB_BIND_ADDR`（可选，默认 `0.0.0.0:8080`）
