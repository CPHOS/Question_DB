# Question Bank API 文档

> 本文档由 `scripts/build_api_doc.py` 自动生成，请勿手动编辑。
> 源文件位于各模块的 `src/api/<module>/API.md`。

## 全局约定

### Base URL

所有路径相对于服务根，例如 `http://localhost:8080`。

### 分页响应格式

所有列表接口使用统一分页包裹：

```json
{
  "items": [ ... ],
  "total": 42,
  "limit": 20,
  "offset": 0
}
```

- `limit` 默认 `20`，范围 `1..100`
- `offset` 默认 `0`，最小 `0`

### 未知字段策略

`PATCH` / `POST` 的 JSON 请求体启用了 **deny_unknown_fields**，传入未定义字段会返回 `400`。
