# Papers API

## Endpoints

### `POST /papers`

创建试卷，并按 `question_ids` 的顺序写入题目关联。

请求体：

```json
{
  "edition": "2026",
  "paper_type": "regular",
  "title": "Demo paper",
  "notes": "optional",
  "question_ids": ["uuid-1", "uuid-2"]
}
```

### `GET /papers`

列出试卷摘要，包括题目数量。

### `GET /papers/{paper_id}`

返回试卷详情和按顺序展开后的题目摘要。

### `PATCH /papers/{paper_id}`

部分更新试卷 metadata 和题目列表。

支持字段：

- `edition`
- `paper_type`
- `title`
- `notes`
- `question_ids`

成功时返回更新后的完整试卷详情。

### `DELETE /papers/{paper_id}`

删除试卷。

成功响应：

```json
{
  "paper_id": "uuid",
  "status": "deleted"
}
```

### `POST /papers/bundles`

按给定试卷列表批量打包下载。

请求体：

```json
{
  "paper_ids": ["uuid-1", "uuid-2"]
}
```

返回值：

- 响应体是一个 `application/zip`
- zip 根目录包含 `manifest.json`
- 每个试卷使用自己的 `paper_id/` 目录分组
- 每个试卷目录下再按 `question_id/` 展开题目的 `.tex` 和 `assets/` 文件
