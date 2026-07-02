import asyncio
from smartlead.smart_delivery import SmartDeliveryClient, CreditError

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

class FakeResp:
    def __init__(self, payload, status=200):
        self._p, self.status_code = payload, status
    def json(self): return self._p
    @property
    def text(self):
        import json; return json.dumps(self._p)

class FakeHTTP:
    def __init__(self, mode="ok"): self.mode = mode; self.calls = []
    async def post(self, url, headers=None, json=None):
        self.calls.append(("POST", url, json))
        if "/spam-test/manual" in url:
            if self.mode == "credit":
                return FakeResp({"message": "Insufficient credits, please upgrade"}, 402)
            return FakeResp({"id": 555})
        if "/report/" in url and "/providerwise" in url:
            return FakeResp({"data": [{"inbox": 80, "spam": 20}, {"inbox": 90, "spam": 10}]})
        return FakeResp({})
    async def get(self, url, headers=None):
        self.calls.append(("GET", url, None))
        if "/spam-test/" in url:
            return FakeResp({"status": "COMPLETED", "test_end_date": "2026-07-02T10:00:00Z"})
        return FakeResp({})
    async def aclose(self): pass

async def main():
    c = SmartDeliveryClient("k"); fake = FakeHTTP("ok"); c._client = fake
    tid = await c.create_test(123, 999, ["a@x.com"], "t")
    ok(tid == 555, f"create returns id (got {tid})")
    # default is_warmup True
    ok(fake.calls[-1][2]["is_warmup"] is True, "default is_warmup=True")
    # explicit is_warmup False (warmup-off test mode)
    await c.create_test(123, 999, ["a@x.com"], "t", is_warmup=False)
    ok(fake.calls[-1][2]["is_warmup"] is False, "is_warmup=False passthrough")
    poll = await c.poll_test(555)
    ok(poll["done"] is True and poll["status"] == "COMPLETED", "poll parses done")
    rep = await c.get_report(555)
    ok(abs(rep["inbox_pct"] - 85.0) < 0.01, f"report avg inbox 85% (got {rep['inbox_pct']})")

    cc = SmartDeliveryClient("k"); cc._client = FakeHTTP("credit")
    try:
        await cc.create_test(1, 1, ["a@x.com"], "t"); ok(False, "should raise CreditError")
    except CreditError:
        ok(True, "credit failure -> CreditError")
    print("\nALL PASSED")

asyncio.run(main())
