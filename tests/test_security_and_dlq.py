# tests/test_security_and_dlq.py
import pytest
from pipeline.dispatcher.ssrf_protection import validate_webhook_url, SSRFValidationError, sign_webhook_payload


def test_ssrf_validation_blocks_loopback_and_metadata():
    """Verify that loopback IPs, private networks, and metadata endpoints are blocked."""
    with pytest.raises(SSRFValidationError):
        validate_webhook_url("http://127.0.0.1:8000/webhook")

    with pytest.raises(SSRFValidationError):
        validate_webhook_url("http://localhost:8000/webhook")

    with pytest.raises(SSRFValidationError):
        validate_webhook_url("http://169.254.169.254/latest/meta-data")


def test_ssrf_validation_allows_valid_https():
    """Verify that valid public HTTPS URLs pass validation and return resolved IP."""
    url = "https://webhook.site/abc-123"
    valid_url, ip = validate_webhook_url(url)
    assert valid_url == url
    assert ip is not None


def test_ssrf_validation_allow_private_mode_for_dev():
    """Verify that local private testing mode works when explicitly allowed."""
    url = "http://127.0.0.1:8000/webhook"
    valid_url, ip = validate_webhook_url(url, allow_private=True)
    assert valid_url == url
    assert ip == "127.0.0.1"


def test_hmac_webhook_signature():
    """Verify that HMAC SHA-256 payload signing generates valid hex signatures."""
    payload = b'{"event": "SIM_SWAP", "phoneNumber": "+84901234567"}'
    signature = sign_webhook_payload(payload, secret="test_secret")
    assert signature.startswith("sha256=")
    assert len(signature) == 7 + 64
