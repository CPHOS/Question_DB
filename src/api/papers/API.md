# Papers API

## Endpoints

### `POST /papers`

创建试卷，并按 `question_ids` 的顺序写入题目关联。

请求体：

```json
{
  "edition": "2026",
  "paper_type": "regular",
  "description": "Demo paper",
  "question_ids": ["uuid-1", "uuid-2"]
}
```

说明：

- `description` 为必填，必须是非空字符串
- `description` 支持中文

### `GET /papers`

按条件分页查询试卷，搜索也统一走这个接口。

支持的 query 参数：

- `question_id`
- `paper_type`
- `category`
- `tag`
- `q`
  关键词搜索，只会匹配 `description`
- `limit`
- `offset`

### `GET /papers/{paper_id}`

返回试卷详情和按顺序展开后的题目摘要。

### `PATCH /papers/{paper_id}`

部分更新试卷 metadata 和题目列表。

支持字段：

- `edition`
- `paper_type`
- `description`
- `question_ids`

其中 `description` 如果出现在更新请求里，必须是非空字符串。

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
