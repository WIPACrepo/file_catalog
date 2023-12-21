# test_server.py
"""Test REST Server."""

# fmt:off
# pylint: skip-file

import pytest
import requests
from requests.exceptions import HTTPError
from rest_tools.client import RestClient


def _assert_httperror(exception: Exception, code: int, reason: str) -> None:
    """Assert that this is the expected HTTPError."""
    print(exception)
    assert isinstance(exception, requests.exceptions.HTTPError)
    if exception:
        assert exception.response.status_code == code
        assert exception.response.reason == reason


def test_00_always_succeed() -> None:
    """Succeed with flying colors."""
    assert True


@pytest.mark.asyncio
async def test_01_main(rest: RestClient) -> None:
    """Test that route / returns an HTTPError."""
    with pytest.raises(HTTPError) as cm:
        await rest.request("GET", "/")
    _assert_httperror(cm.value, 404, "Not Found")


@pytest.mark.asyncio
async def test_02_login(rest: RestClient) -> None:
    """Test that route /login returns an HTTPError."""
    with pytest.raises(HTTPError) as cm:
        await rest.request("GET", "/login")
    _assert_httperror(cm.value, 404, "Not Found")


@pytest.mark.asyncio
async def test_03_account(rest: RestClient) -> None:
    """Test that route /account returns an HTTPError."""
    with pytest.raises(HTTPError) as cm:
        await rest.request("GET", "/account")
    _assert_httperror(cm.value, 404, "Not Found")


@pytest.mark.asyncio
async def test_04_HATEOAS(rest: RestClient) -> None:
    """Test that route /api provides a HATEOAS response."""
    res = await rest.request("GET", "/api")
    assert res == {'_links': {'self': {'href': '/api'}}, 'files': {'href': '/api/files'}}

    for method in ["DELETE", "PATCH", "POST", "PUT"]:
        with pytest.raises(HTTPError) as cm:
            await rest.request(method, "/api")
        _assert_httperror(cm.value, 405, "Method Not Allowed")
