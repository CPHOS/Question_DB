# CPHOS 题库重构方案（Rust + PostgreSQL + 对象存储）

## 1. 现状复盘与主要不合理点
基于当前仓库文档（`README.md`、`docs/field_dictionary.md`、`docs/ingestion_guide.md`、`docs/deployment_guide.md` 等）可以确认：系统采用“服务器目录 + SQLite + API”的混合存储方式，核心问题不是“能不能跑”，而是“数据边界不清、状态分散、扩展受限”。

### 1.1 数据双写导致一致性风险
当前 `papers/questions` 里同时保存 `*_path` 与 `*_source`，`score_workbooks` 里又保存 `file_path` 与 `workbook_blob`，形成“路径 + 内容”双写。该方案虽然提升可追溯性，但会带来：
- 文件变了但 DB 快照没更新（或反过来）的漂移。
- 修复动作难以原子化（文件系统与数据库事务分离）。
- 导入链路要维护两套真相来源，审计复杂度高。

> 你提到的 “tex 文件存了两遍” 本质就是这个问题：同一业务对象既存在文件系统，又存在数据库文本字段。

### 1.2 SQLite 不适合作为长期主库
SQLite 在单机小团队场景非常高效，但该项目已经出现：
- 结构化检索（题目、统计、标签、状态）
- 批量导入与质检
- 后续可能并发写入与多角色访问

此时 SQLite 会在并发写、权限治理、在线迁移、审计和备份恢复上逐步成为瓶颈。

### 1.3 文件系统路径耦合运行环境
文档强调“服务器路径导入”，优点是简单；代价是：
- 路径成为隐式 API（跨环境迁移难）。
- 目录组织变更会影响业务可用性。
- 备份需要分别管理 DB 与目录，恢复流程复杂。

### 1.4 原始资产管理缺少统一对象模型
LaTeX、图片、xlsx 目前按业务表分别落库，缺少统一的“对象（Object）”抽象，导致：
- 同一文件重复入库无法天然去重。
- 版本、生命周期、冷热分层策略难统一。
- 安全策略（访问控制、签名 URL、外链）难集中治理。

### 1.5 导入流程更像脚本集合，缺乏领域边界
当前导入、统计、导出流程可用，但边界更偏“脚本编排”，不是“领域服务”。随着功能增长，维护成本会快速上升。

---

## 2. 重构目标
1. **Rust 重写核心服务**：提升可靠性、可维护性与性能。
2. **PostgreSQL 成为唯一事务主库**：承载结构化元数据、关系约束、审计日志。
3. **对象存储承载所有大文件**：LaTeX、图片、xlsx、导出包统一走对象层。
4. **去除路径耦合**：业务只依赖对象 ID/URI，不依赖服务器绝对路径。
5. **实现可迁移、可审计、可回滚**：导入版本化，数据和对象可追溯。

---

## 3. 目标架构（推荐）

```text
[Ingest CLI / Admin API]
           |
           v
   [Rust Ingestion Service] ----> [Object Storage (S3/MinIO/COS)]
           |
           v
     [PostgreSQL Metadata]
           |
           v
       [Rust Query API]
           |
           v
    [Internal consumers / tools]
```

### 3.1 分层原则
- **对象存储**：只负责“字节内容 + 基础元信息”（content-type、size、etag）。
- **PostgreSQL**：负责“业务语义 + 关系 + 状态 + 索引 + 审计”。
- **Rust 服务**：负责导入校验、事务协调、权限控制、导出拼装。

---

## 4. 对象存储方案设计

## 4.1 对象键（Object Key）命名
建议使用稳定、可读、可分层的键：

```text
qb/{env}/{paper_id}/{asset_type}/{sha256_prefix}/{filename}
```

示例：
- `qb/prod/CPHOS-18-REGULAR/latex-question/ab12/QB-2024-E-02.tex`
- `qb/prod/CPHOS-18-REGULAR/workbook/9f3a/2025-04-07-score.xlsx`

要点：
- 以 `sha256` 做幂等和去重依据。
- `asset_type` 区分类别（paper_tex/question_tex/answer_tex/image/workbook/export）。
- 业务查询不依赖 key 解析，key 仅用于存储布局。

## 4.2 元数据统一表（核心）
在 PostgreSQL 建立统一 `objects` 表：
- `object_id (uuid)`
- `bucket`
- `object_key`
- `sha256`
- `size_bytes`
- `mime_type`
- `storage_class`（hot/warm/cold）
- `created_at`
- `created_by`
- `encryption`

业务表仅引用 `object_id`，不再直接保存大字段文本/BLOB。

## 4.3 去重与版本策略
- **去重**：`unique(sha256, size_bytes)`；同内容只存一份对象。
- **版本**：通过 `paper_versions`、`question_versions` 记录“某次导入绑定了哪个 object_id”，而不是覆盖更新。
- **软删除**：业务删除仅断开引用；对象做“引用计数 + 延迟清理”。

## 4.4 访问与安全
- 内部下载使用短时签名 URL（Presigned URL）。
- 对象桶按环境隔离（dev/stage/prod）。
- 强制 SSE（服务端加密）和最小权限 IAM。

## 4.5 生命周期策略
- 热数据（近一年常访问）保留标准存储。
- 冷数据（历史批次）转低频层。
- 导出产物设置 TTL（如 30 天自动清理）。

---

## 5. PostgreSQL 数据模型（关键调整）

### 5.1 资产引用替代文本双存
把：
- `papers.paper_latex_path + paper_latex_source`
- `questions.latex_path + latex_source + answer_latex_path + answer_latex_source`
- `score_workbooks.file_path + workbook_blob`

统一替换为对象引用：
- `papers.paper_tex_object_id`
- `questions.question_tex_object_id`
- `questions.answer_tex_object_id`
- `score_workbooks.workbook_object_id`

必要时保留 `original_path` 作为“导入时证据字段”（仅审计，不参与主流程）。

### 5.2 推荐新增表
- `objects`：对象元数据
- `imports`：导入批次
- `import_items`：每个文件/题目的导入结果
- `paper_versions`、`question_versions`：版本快照
- `audit_logs`：关键操作审计

### 5.3 检索能力
- `questions.search_text` 使用 PostgreSQL `tsvector` + GIN。
- 标签可选 `jsonb` + GIN 或拆分 `question_tags` 关系表。

---

## 6. Rust 重写建议（服务拆分）

### 6.1 技术栈
- Web/API: `axum`
- DB: `sqlx`（或 `diesel`）
- Migration: `sqlx migrate` / `refinery`
- Object Storage: `aws-sdk-s3`（兼容 MinIO/S3）
- 序列化: `serde`
- 异步运行时: `tokio`

### 6.2 模块边界
- `qb-domain`：实体与规则（paper/question/import）
- `qb-storage`：PostgreSQL 仓储 + S3 适配
- `qb-ingest`：bundle 校验、去重上传、事务提交
- `qb-api`：查询与管理接口
- `qb-export`：导出 JSONL/CSV/打包文件

### 6.3 导入事务（关键）
采用“两阶段语义”：
1. 先上传对象到 S3（可重试，幂等）
2. 再在 PostgreSQL 事务中写入元数据与关联

若第 2 步失败，对象通过“孤儿回收任务”延迟清理。

---

## 7. 迁移路线（从当前系统到新系统）
1. **建新库**：部署 PostgreSQL + 新 schema。
2. **资产回填**：扫描现有路径与 DB 文本/BLOB，生成对象并上传。
3. **建立映射**：生成 `legacy_path -> object_id`、`legacy_record -> new_record`。
4. **双写观察期**：旧系统继续运行，新系统做镜像导入并比对。
5. **切换读流量**：先查询 API 切换，再切导入写入。
6. **下线旧字段**：确认稳定后删除 `*_source`/`*_blob` 冗余字段。

---

## 8. 成本与收益评估（简版）
- 成本：一次性迁移开发 + 对象存储费用 + 运维复杂度上升。
- 收益：
  - 消除路径耦合与双写漂移。
  - 支持并发和长期演进。
  - 统一资产治理（去重、版本、生命周期、权限）。
  - 未来可扩展到全文检索、任务队列、异步编译和多租户隔离。

---

## 9. 最小可落地版本（MVP）建议
第一阶段不要一次做“全微服务”，建议先做：
1. 单体 Rust API（含导入 + 查询）
2. PostgreSQL 主库
3. 单个对象桶（按前缀分层）
4. 保留现有 bundle 格式，先改“落库方式”

这样可以以最低迁移风险先解决核心问题（双写、路径耦合、SQLite 扩展性），后续再拆服务。
