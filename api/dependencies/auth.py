"""
API Key authentication dependency cho FastAPI.

Cơ chế: Bearer token tĩnh qua header X-API-Key.
Giá trị key đọc từ env var API_KEY (xem api/config.py).

Dùng trong router bằng FastAPI Depends:
    from api.dependencies.auth import verify_api_key
    @router.post("/endpoint")
    async def endpoint(api_key: str = Depends(verify_api_key)):
        ...

Lưu ý (Quyết định Lab #10):
    Đây là API Key tĩnh, không phải OAuth2 full flow.
    Production cần tích hợp OAuth2 Authorization Code + PKCE
    theo CAMARA OIDC profile.
"""

import secrets
from typing import Optional

from fastapi import Header, HTTPException, status
from api.config import settings


async def verify_api_key(
    x_api_key: Optional[str] = Header(
        None,
        alias="X-API-Key",
        description="API Key xác thực. Lấy từ env var API_KEY.",
    )
) -> str:
    """
    FastAPI dependency: kiểm tra header X-API-Key.

    Args:
        x_api_key: Giá trị header X-API-Key, FastAPI tự inject.

    Returns:
        Giá trị key nếu hợp lệ (để router có thể log nếu cần).

    Raises:
        HTTPException 401: Nếu header thiếu hoặc key không khớp.
            - Thiếu header: FastAPI tự raise 422, nhưng ta override
              bằng HTTPException 401 để khớp CAMARA error convention.
            - Key sai: raise 401 với WWW-Authenticate header.
    """
    if x_api_key is None or not secrets.compare_digest(x_api_key, settings.api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "error": "UNAUTHENTICATED",
                "message": "Invalid or missing API key.",
            },
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return x_api_key
