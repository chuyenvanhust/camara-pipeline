import pytest
import importlib
from unittest.mock import patch
from fastapi import HTTPException


# Import tường minh trước khi patch — đảm bảo
# module 'api.dependencies.auth' đã nằm trong sys.modules
# và patch() có thể getattr(api.dependencies, 'auth') thành công.
import api.dependencies.auth


@pytest.fixture(autouse=True)
def patch_api_key():
    """
    Patch settings.api_key trong namespace của auth.py.

    Phải patch 'api.dependencies.auth.settings' (nơi auth.py dùng),
    không phải 'api.config.settings' (nơi định nghĩa) — vì
    'from api.config import settings' trong auth.py tạo binding
    riêng trong namespace auth, patch api.config.settings sau đó
    không ảnh hưởng binding đã tạo.

    Import tường minh 'import api.dependencies.auth' ở đầu file
    đảm bảo pkgutil.resolve_name() tìm thấy attribute 'auth'
    trong package 'api.dependencies'.
    """
    with patch("api.dependencies.auth.settings") as mock_settings:
        mock_settings.api_key = "test-secret-key"
        yield mock_settings


@pytest.mark.asyncio
async def test_valid_api_key_passes():
    from api.dependencies.auth import verify_api_key
    result = await verify_api_key(x_api_key="test-secret-key")
    assert result == "test-secret-key"


@pytest.mark.asyncio
async def test_wrong_api_key_raises_401():
    from api.dependencies.auth import verify_api_key
    with pytest.raises(HTTPException) as exc_info:
        await verify_api_key(x_api_key="wrong-key")
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail["error"] == "UNAUTHENTICATED"


@pytest.mark.asyncio
async def test_empty_api_key_raises_401():
    from api.dependencies.auth import verify_api_key
    with pytest.raises(HTTPException) as exc_info:
        await verify_api_key(x_api_key="")
    assert exc_info.value.status_code == 401