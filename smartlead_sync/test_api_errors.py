"""Smartlead errors never carry the key (it rides in the query string).

2 Nov 2026: the PRECISE_LEADS plan expired, every call got a 401, and the
9:00 stats job printed httpx's message, URL and api_key included, to the
Render log each morning.
"""
import asyncio

import httpx
import pytest

from smartlead.api import SmartleadClient, raise_for_status

KEY = "secret-key-123_abc"


def _resp(status: int, body: str = "") -> httpx.Response:
    req = httpx.Request("GET", f"https://server.smartlead.ai/api/v1/campaigns?api_key={KEY}")
    return httpx.Response(status, request=req, text=body)


def test_error_text_has_no_key_but_says_what_went_wrong():
    with pytest.raises(httpx.HTTPStatusError) as err:
        raise_for_status(_resp(401, '{"message": "Plan expired!"}'))
    text = str(err.value)
    assert KEY not in text and "api_key" not in text
    assert "401" in text and "/api/v1/campaigns" in text and "Plan expired!" in text
    assert err.value.response.status_code == 401      # callers still branch on it
    assert err.value.__suppress_context__              # the original message is not chained


def test_success_passes_through():
    raise_for_status(_resp(200, "[]"))


def test_client_get_raises_without_the_key():
    def handler(request):
        return httpx.Response(401, text="Plan expired!")

    async def run():
        async with SmartleadClient(KEY, "PRECISE_LEADS") as slc:
            slc._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            await slc.list_campaigns()

    with pytest.raises(httpx.HTTPStatusError) as err:
        asyncio.run(run())
    assert KEY not in str(err.value) and "Plan expired!" in str(err.value)
