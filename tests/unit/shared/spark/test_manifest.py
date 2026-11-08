"""Spark result manifest uses an exact S3 object key and signed HTTP PUT."""

from datetime import UTC, datetime

import pytest

from flashflood_data.shared.spark.manifest import manifest_key, put_json_manifest


def test_manifest_key_rejects_non_s3_uri() -> None:
    assert manifest_key("s3://staging/silver/weather/run/result.json") == "staging/silver/weather/run/result.json"
    with pytest.raises(ValueError, match="s3"):
        manifest_key("https://example.test/result.json")


def test_put_json_manifest_signs_exact_object() -> None:
    requests = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def opener(request, timeout):
        requests.append((request, timeout))
        return Response()

    put_json_manifest(
        "s3://staging/silver/weather/run/result.json", "{}",
        now=datetime(2026, 1, 1, tzinfo=UTC),
        endpoint="http://minio:9000", access_key="access", secret_key="secret",
        opener=opener,
    )
    request, timeout = requests[0]
    assert request.full_url == "http://minio:9000/staging/silver/weather/run/result.json"
    assert request.get_method() == "PUT"
    assert request.data == b"{}"
    assert request.get_header("Authorization").startswith("AWS4-HMAC-SHA256 ")
    assert timeout == 30
