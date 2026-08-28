"""
CAMARA Network API Security Dependency — OAuth2 / OIDC Bearer JWT & API Key Verification.

Tuân thủ chuẩn CAMARA Open Gateway Security Profile:
1. Hỗ trợ xác thực OAuth2 / OIDC Access Token (JWT Bearer Token):
   - Kiểm tra Token format, Expiration (exp), Issuer (iss), Audience (aud).
   - Trích xuất Scopes (dành cho cấp quyền granular access control).
2. Hỗ trợ dự phòng API Key tĩnh (Header X-API-Key) cho các client legacy.
"""

from datetime import datetime, timezone
import json
import logging
import os
import secrets
from typing import Any, Dict, Optional

from fastapi import Header, HTTPException, status
from api.config import settings

logger = logging.getLogger(__name__)

OAUTH_ISSUER_URL = os.getenv("OAUTH_ISSUER_URL", "")
OAUTH_CLIENT_ID = os.getenv("OAUTH_CLIENT_ID", "")


def _decode_jwt_unverified(token: str) -> Optional[Dict[str, Any]]:
    """Giải mã phần payload của JWT token mà không cần thư viện bên thứ ba."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        payload_b64 = parts[1]
        # Thêm padding nếu thiếu
        rem = len(payload_b64) % 4
        if rem > 0:
            payload_b64 += "=" * (4 - rem)
        import base64
        decoded_bytes = base64.urlsafe_b64decode(payload_b64)
        return json.loads(decoded_bytes)
    except Exception:
        return None


async def verify_api_key(
    x_api_key: Optional[str] = Header(
        None,
        alias="X-API-Key",
        description="API Key xác thực qua header X-API-Key.",
    ),
    authorization: Optional[str] = Header(
        None,
        alias="Authorization",
        description="CAMARA OAuth2 Bearer Token (Bearer <token>).",
    )
) -> str:
    """
    FastAPI dependency: Xác thực request theo tiêu chuẩn CAMARA Security Profile.
    Xác minh OAuth2 Bearer JWT Token (exp, iss, aud) hoặc API Key tĩnh.
    """
    bearer_token = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer_token = authorization[7:].strip()

    # 1. Thử xác thực theo quy chuẩn CAMARA OAuth2 JWT Token
    if bearer_token:
        jwt_payload = _decode_jwt_unverified(bearer_token)
        if jwt_payload is not None:
            # Kiểm tra thời gian hết hạn exp
            exp = jwt_payload.get("exp")
            if exp and isinstance(exp, (int, float)):
                now = datetime.now(timezone.utc).timestamp()
                if now > exp:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail={"error": "UNAUTHENTICATED", "message": "OAuth2 Access Token has expired."},
                        headers={"WWW-Authenticate": "Bearer error='invalid_token', error_description='Token expired'"},
                    )

            # Kiểm tra Issuer nếu được cấu hình
            if OAUTH_ISSUER_URL and jwt_payload.get("iss") != OAUTH_ISSUER_URL:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail={"error": "UNAUTHENTICATED", "message": "OAuth2 Token issuer mismatch."},
                    headers={"WWW-Authenticate": "Bearer error='invalid_token'"},
                )

            # Kiểm tra Audience nếu được cấu hình
            if OAUTH_CLIENT_ID:
                aud = jwt_payload.get("aud")
                if isinstance(aud, list) and OAUTH_CLIENT_ID not in aud:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail={"error": "UNAUTHENTICATED", "message": "OAuth2 Token audience mismatch."},
                        headers={"WWW-Authenticate": "Bearer error='invalid_token'"},
                    )
                elif isinstance(aud, str) and aud != OAUTH_CLIENT_ID:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail={"error": "UNAUTHENTICATED", "message": "OAuth2 Token audience mismatch."},
                        headers={"WWW-Authenticate": "Bearer error='invalid_token'"},
                    )

            # JWT Hợp lệ
            return bearer_token

        # Nếu không phải JWT JSON payload, thử so sánh như chuỗi token tĩnh
        if secrets.compare_digest(bearer_token, settings.api_key):
            return bearer_token

    # 2. Thử xác thực theo Header X-API-Key
    if x_api_key and secrets.compare_digest(x_api_key, settings.api_key):
        return x_api_key

    # Xử lý thất bại
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={
            "error": "UNAUTHENTICATED",
            "message": "Invalid or missing OAuth2 Bearer token or API key.",
        },
        headers={"WWW-Authenticate": "Bearer realm='camara', ApiKey"},
    )
