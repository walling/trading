import aiohttp
import asyncio
import time
from typing import Optional

_HTTP_CONTEXT = None


def request_context():
    global _HTTP_CONTEXT
    if not _HTTP_CONTEXT:
        _HTTP_CONTEXT = HTTPContext()
    return _HTTP_CONTEXT


def request_client(
    throttle_wait: float = 0,
    timeout: float = 30,
    connect_timeout: Optional[float] = None,
    raise_for_status: bool = True,
):
    return HTTPSession(
        throttle_wait=throttle_wait,
        timeout=timeout,
        connect_timeout=connect_timeout or round(timeout / 2),
        raise_for_status=raise_for_status,
    )


class HTTPContext:
    def __init__(self):
        self._sessions = None
        self._ref_count = 0

    def session(self, *args, **kwargs):
        """
        For arguments, see: https://docs.aiohttp.org/en/latest/client_reference.html#aiohttp.ClientSession
        """

        if self._sessions is None:
            raise RuntimeError("HTTPContext: context not initialized")

        session = aiohttp.ClientSession(*args, **kwargs)
        self._sessions.append(session)
        return session

    async def __aenter__(self):
        if self._sessions is None:
            self._sessions = []
        self._ref_count += 1

    async def __aexit__(self, exc_type, exc, tb):
        self._ref_count -= 1
        if self._ref_count > 0:
            return

        sessions = self._sessions or []
        self._sessions = None
        await asyncio.gather(*[s.close() for s in sessions], return_exceptions=True)
        await asyncio.sleep(0.250)


class HTTPSession:
    def __init__(
        self,
        throttle_wait: float,
        timeout: float,
        connect_timeout: float,
        raise_for_status: bool,
    ):
        self.throttle_wait = throttle_wait
        self._throttle_last = 0

        self._session = request_context().session(
            timeout=aiohttp.ClientTimeout(
                total=timeout,  # type: ignore
                connect=connect_timeout,
            ),
            raise_for_status=raise_for_status,
        )

    async def throttle(self):
        if self.throttle_wait <= 0:
            return

        now = time.time()
        waited_already = now - self._throttle_last
        remaining_time = max(0.0, self.throttle_wait - waited_already)

        self._throttle_last = now + remaining_time
        await asyncio.sleep(remaining_time)

    async def get(self, url: str, *args, **kwargs):
        await self.throttle()
        async with self._session.get(url, *args, **kwargs) as response:
            return await response.json()

    async def post(self, url: str, *args, **kwargs):
        await self.throttle()
        async with self._session.post(url, *args, **kwargs) as response:
            return await response.json()


if __name__ == "__main__":

    async def test():
        async with request_context():
            client = request_client()
            result1, result2 = await asyncio.gather(
                client.get("https://httpbin.org/json"),
                client.get("https://httpbin.org/get"),
            )
            print(result1)
            print(result2)

    asyncio.run(test())
