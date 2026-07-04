# mock_services/shared/

Utilities dùng chung cho cả 3 mock services.

## Files

| File | Vai trò |
|------|---------|
| `health.py` | Standard health check response: status, service name, record count, uptime |
| `pagination.py` | Pydantic generics `Page[T]`: tính offset/limit, trả `items/total/page/pages` |
| `errors.py` | Standard error response format + `X-Inject-Fault` header handler |

## Error format chuẩn

Tất cả 3 mock service trả lỗi theo cùng 1 format:

```json
{
  "error": "NOT_FOUND",
  "message": "TAC '999999' not found in database",
  "request_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

## Fault injection (`X-Inject-Fault` header)

`errors.py` cung cấp FastAPI middleware đọc header `X-Inject-Fault`
và inject hành vi lỗi vào response — dùng để test pipeline resilience:

| Giá trị header | Hành vi |
|---------------|---------|
| `delay=500` | Trì hoãn response 500ms |
| `status=503` | Trả HTTP 503 bất kể request |
| `error_rate=0.2` | 20% request trả lỗi ngẫu nhiên |
| `timeout` | Không trả response (giả lập timeout) |
