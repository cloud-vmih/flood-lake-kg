import logging
import re
from typing import Final
from urllib.parse import unquote_plus

from flashflood_data.catalog.models import _is_sensitive_key
from flashflood_data.core.config import EnvironmentSettings

_AUTHORIZATION_FIELD: Final = re.compile(
    r"(?i)(^|[\s;,{])(?P<key_quote>['\"]?)authorization"
    r"(?P=key_quote)\s*[:=]\s*(?P<value_quote>['\"]?)"
)
_BEARER: Final = re.compile(r"(?i)(\bbearer\s+)[^\s;,]+")
_URL_USERINFO: Final = re.compile(r"(?i)(https?://)[^/@\s]+@")
_QUERY_PARAMETER: Final = re.compile(r"([?&])([^=&#\s]+)=([^&#\s]*)")


def redact(value: str, environment: EnvironmentSettings | None = None) -> str:
    """Remove credentials and signed transport values from text."""

    def redact_query(match: re.Match[str]) -> str:
        key = unquote_plus(match.group(2))
        normalized = re.sub(r"[^a-z0-9]", "", key.lower())
        if _is_sensitive_key(key) or normalized == "sig":
            return f"{match.group(1)}{match.group(2)}=[REDACTED]"
        return match.group(0)

    redacted = _QUERY_PARAMETER.sub(redact_query, value)
    redacted = redact_authorization_fields(redacted)
    redacted = _BEARER.sub(r"\1[REDACTED]", redacted)
    redacted = _URL_USERINFO.sub(r"\1[REDACTED]@", redacted)
    if environment is not None:
        for secret in (environment.cdse_username, environment.cdse_password):
            if secret is not None and (plain := secret.get_secret_value()):
                redacted = redacted.replace(plain, "[REDACTED]")
    return redacted


def redact_authorization_fields(value: str) -> str:
    """Redact authorization fields while respecting quoted values."""
    parts: list[str] = []
    cursor = 0
    for match in _AUTHORIZATION_FIELD.finditer(value):
        if match.start() < cursor:
            continue
        end = match.end()
        value_quote = match.group("value_quote")
        if value_quote:
            escaped = False
            while end < len(value):
                character = value[end]
                if escaped:
                    escaped = False
                elif character == "\\":
                    escaped = True
                elif character == value_quote:
                    break
                end += 1
            parts.append(value[cursor : match.end()])
            parts.append("[REDACTED]")
            cursor = end
            continue
        quote: str | None = None
        escaped = False
        while end < len(value):
            character = value[end]
            if quote is not None:
                if escaped:
                    escaped = False
                elif character == "\\":
                    escaped = True
                elif character == quote:
                    quote = None
            elif character in {'"', "'"}:
                quote = character
            elif character in ";\r\n":
                break
            end += 1
        parts.append(value[cursor : match.end()])
        parts.append("[REDACTED]")
        cursor = end
    parts.append(value[cursor:])
    return "".join(parts)


class SecretRedactionFilter(logging.Filter):
    """Redact credentials and signed transport values before log emission."""

    def __init__(self, environment: EnvironmentSettings | None = None) -> None:
        super().__init__()
        self.environment = environment

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage(), self.environment)
        record.args = ()
        record.exc_info = None
        if record.exc_text is not None:
            record.exc_text = redact(record.exc_text, self.environment)
        if record.stack_info is not None:
            record.stack_info = redact(record.stack_info, self.environment)
        return True
