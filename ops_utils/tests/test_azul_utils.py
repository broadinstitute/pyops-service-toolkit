import json
from unittest.mock import patch

import pytest
import responses

from ops_utils.azul_utils import AzulClient, AzulReindexingError, AzulTokenError, SignedUrl, TOKENINFO_URL
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token

AZUL_URL = "https://azul.test"
SIGNED_URL = "https://bucket.s3.amazonaws.com/manifest.avro?X-Amz-Signature=secret"
CLIENT_ID = "123-abc.apps.googleusercontent.com"


@pytest.fixture
def azul(tmp_path):
    token_file = tmp_path / "azul_token.txt"
    token_file.write_text("azul-token")
    return AzulClient(RunRequest(Token(token_file=str(token_file)), max_retries=1), azul_url=f"{AZUL_URL}/")


class TestSignedUrl:
    def test_redacts_everywhere_but_reveal(self):
        url = SignedUrl(SIGNED_URL)
        for text in (repr(url), str(url), f"{url}", "%s" % url):
            assert "secret" not in text
            assert "https://bucket.s3.amazonaws.com/manifest.avro" in text
        assert url.reveal() == SIGNED_URL
        assert url.host == "bucket.s3.amazonaws.com"
        assert url.location == "https://bucket.s3.amazonaws.com/manifest.avro"

    def test_not_json_serializable(self):
        with pytest.raises(TypeError):
            json.dumps({"url": SignedUrl(SIGNED_URL)})


class TestCheckNotReindexing:
    @responses.activate
    def test_idle(self, azul):
        responses.get(
            f"{AZUL_URL}/health/progress",
            json={"up": True, "progress": {"unindexed_bundles": 0, "unindexed_documents": 0}},
        )
        azul.check_not_reindexing()

    @responses.activate
    def test_backlog(self, azul):
        responses.get(
            f"{AZUL_URL}/health/progress",
            json={"up": True, "progress": {"unindexed_bundles": 3, "unindexed_documents": 0}},
        )
        with pytest.raises(AzulReindexingError, match="unindexed_bundles=3"):
            azul.check_not_reindexing()

    @responses.activate
    def test_not_up(self, azul):
        responses.get(f"{AZUL_URL}/health/progress", json={"up": False, "progress": {}})
        with pytest.raises(AzulReindexingError, match="not healthy"):
            azul.check_not_reindexing()


class TestRequestPfbManifest:
    @responses.activate
    def test_polls_until_ready(self, azul):
        responses.get(f"{AZUL_URL}/index/catalogs", json={"default_catalog": "anvil15"})
        responses.put(
            f"{AZUL_URL}/fetch/manifest/files",
            json={"Status": 301, "Location": f"{AZUL_URL}/fetch/manifest/files/tok", "Retry-After": 0},
        )
        responses.get(
            f"{AZUL_URL}/fetch/manifest/files/tok",
            json={"Status": 301, "Location": f"{AZUL_URL}/fetch/manifest/files/tok", "Retry-After": 0},
        )
        responses.get(f"{AZUL_URL}/fetch/manifest/files/tok", json={"Status": 302, "Location": SIGNED_URL})

        signed_url = azul.request_pfb_manifest("dataset-1")

        assert isinstance(signed_url, SignedUrl)
        assert signed_url.reveal() == SIGNED_URL
        put = next(c.request for c in responses.calls if c.request.method == "PUT")
        assert "catalog=anvil15" in put.url
        assert "format=verbatim.pfb" in put.url
        assert "dataset-1" in put.url

    @responses.activate
    def test_explicit_catalog_skips_lookup(self, azul):
        azul.catalog = "anvil14"
        responses.put(f"{AZUL_URL}/fetch/manifest/files", json={"Status": 302, "Location": SIGNED_URL})
        azul.request_pfb_manifest("dataset-1")
        assert len(responses.calls) == 1

    @responses.activate
    def test_timeout(self, azul):
        azul.catalog = "anvil14"
        responses.put(
            f"{AZUL_URL}/fetch/manifest/files",
            json={"Status": 301, "Location": f"{AZUL_URL}/fetch/manifest/files/tok", "Retry-After": 5},
        )
        with patch("ops_utils.azul_utils.time.sleep"), pytest.raises(TimeoutError):
            azul.request_pfb_manifest("dataset-1", max_wait_seconds=1)

    @responses.activate
    def test_unexpected_status(self, azul):
        azul.catalog = "anvil14"
        responses.put(f"{AZUL_URL}/fetch/manifest/files", json={"Status": 500})
        with pytest.raises(RuntimeError, match="500"):
            azul.request_pfb_manifest("dataset-1")


class TestValidateToken:
    def _mock(self, tokeninfo, status=200):
        responses.get(
            f"{AZUL_URL}/swagger/swagger-initializer.js",
            body=f'window.onload = function() {{\n  const client_id = "{CLIENT_ID}";\n}}',
        )
        responses.get(TOKENINFO_URL, json=tokeninfo, status=status)

    @responses.activate
    def test_valid(self, azul):
        self._mock({"azp": CLIENT_ID, "email": "Me@Broad.org", "expires_in": "3000"})
        info = azul.validate_token("tok", allowed_emails=["me@broad.org"], min_remaining_seconds=1800)
        assert info["email"] == "Me@Broad.org"

    @responses.activate
    def test_wrong_client(self, azul):
        self._mock({"azp": "other.apps.googleusercontent.com", "email": "me@broad.org", "expires_in": "3000"})
        with pytest.raises(AzulTokenError, match="azp"):
            azul.validate_token("tok")

    @responses.activate
    def test_wrong_email(self, azul):
        self._mock({"azp": CLIENT_ID, "email": "x@broad.org", "expires_in": "3000"})
        with pytest.raises(AzulTokenError, match="not an allowed account"):
            azul.validate_token("tok", allowed_emails=["me@broad.org"])

    @responses.activate
    def test_expiring_soon(self, azul):
        self._mock({"azp": CLIENT_ID, "email": "me@broad.org", "expires_in": "60"})
        with pytest.raises(AzulTokenError, match="expires in 60s"):
            azul.validate_token("tok", min_remaining_seconds=1800)

    @responses.activate
    def test_expired(self, azul):
        self._mock({"error": "invalid_token"}, status=400)
        with pytest.raises(AzulTokenError, match="not valid"):
            azul.validate_token("tok")
