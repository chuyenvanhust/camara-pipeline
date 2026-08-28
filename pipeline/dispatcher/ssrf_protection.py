# pipeline/dispatcher/ssrf_protection.py
from __future__ import annotations

import hmac
import hashlib
import ipaddress
import os
import socket
import urllib.parse
from typing import Tuple

ENVIRONMENT = os.getenv("ENVIRONMENT", "development").lower()
# Production defaults ALLOW_HTTP_WEBHOOKS to False (HTTPS required)
ALLOW_HTTP_WEBHOOKS_DEFAULT = "false" if ENVIRONMENT == "production" else "true"
ALLOW_HTTP_WEBHOOKS = os.getenv("ALLOW_HTTP_WEBHOOKS", ALLOW_HTTP_WEBHOOKS_DEFAULT).lower() in ("true", "1", "yes")

WEBHOOK_SIGNING_SECRET = os.getenv("WEBHOOK_SIGNING_SECRET", "camara_webhook_hmac_secret_key")

METADATA_HOSTS = {
    "localhost",
    "127.0.0.1",
    "::1",
    "169.254.169.254",
    "metadata.google.internal",
    "instance-data",
}

BLOCKED_IP_NETWORKS = [
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.0.0.0/24"),
    ipaddress.ip_network("192.0.2.0/24"),
    ipaddress.ip_network("192.88.99.0/24"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("198.18.0.0/15"),
    ipaddress.ip_network("198.51.100.0/24"),
    ipaddress.ip_network("203.0.113.0/24"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("240.0.0.0/4"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]


class SSRFValidationError(ValueError):
    """Raised when a URL fails SSRF security validation."""
    pass


def validate_webhook_url(url: str, allow_private: bool = False) -> Tuple[str, str]:
    """
    Validates a webhook URL against SSRF attacks and DNS rebinding.
    - Requires HTTPS in production (unless ALLOW_HTTP_WEBHOOKS=true or allow_private=True)
    - Rejects loopback, private IP ranges, cloud metadata IPs, and internal domains.
    - Returns (original_url, resolved_ip) to prevent DNS Rebinding.
    """
    if not url:
        raise SSRFValidationError("URL cannot be empty")

    parsed = urllib.parse.urlparse(url)
    scheme = (parsed.scheme or "").lower()
    hostname = (parsed.hostname or "").lower()

    if scheme not in ("http", "https"):
        raise SSRFValidationError(f"Invalid URL scheme '{scheme}'. Only HTTP and HTTPS are allowed.")

    if not ALLOW_HTTP_WEBHOOKS and scheme != "https" and not allow_private:
        raise SSRFValidationError("In production mode, webhook URLs MUST use HTTPS.")

    if not hostname:
        raise SSRFValidationError("URL must include a valid hostname.")

    if hostname in METADATA_HOSTS and not allow_private:
        raise SSRFValidationError(f"Forbidden host '{hostname}': internal/metadata endpoints are restricted.")

    if allow_private:
        return url, hostname

    # Resolve IP address to prevent DNS rebinding
    try:
        addr_info = socket.getaddrinfo(hostname, None)
    except socket.gaierror as e:
        raise SSRFValidationError(f"Could not resolve hostname '{hostname}': {e}") from e

    resolved_ip = None
    for family, socktype, proto, canonname, sockaddr in addr_info:
        ip_str = sockaddr[0]
        try:
            ip_obj = ipaddress.ip_address(ip_str)
        except ValueError:
            continue

        if ip_obj.is_loopback or ip_obj.is_link_local or ip_obj.is_multicast or ip_obj.is_reserved:
            raise SSRFValidationError(f"Forbidden IP address '{ip_str}' resolved for hostname '{hostname}'.")

        for network in BLOCKED_IP_NETWORKS:
            if ip_obj in network:
                raise SSRFValidationError(
                    f"Forbidden private/internal IP address '{ip_str}' resolved for hostname '{hostname}'."
                )

        if resolved_ip is None:
            resolved_ip = ip_str

    if resolved_ip is None:
        raise SSRFValidationError(f"No valid IP addresses resolved for hostname '{hostname}'.")

    return url, resolved_ip


def sign_webhook_payload(payload_bytes: bytes, secret: str = WEBHOOK_SIGNING_SECRET) -> str:
    """Computes HMAC-SHA256 signature for webhook payload verification."""
    signature = hmac.new(secret.encode("utf-8"), payload_bytes, hashlib.sha256).hexdigest()
    return f"sha256={signature}"
