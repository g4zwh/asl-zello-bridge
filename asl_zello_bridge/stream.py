import asyncio

try:
    _asyncio_timeout = asyncio.timeout  # Python 3.11+
except AttributeError:  # pragma: no cover - older Python
    _asyncio_timeout = None


class AsyncByteStream:
    """A bounded, single-consumer async byte FIFO.

    Differences from the original implementation:

    * The buffer is bounded (``max_bytes``, default 1 s of 8 kHz/16-bit mono).
      When a writer outruns the reader the *oldest* data is discarded, so
      latency can never grow without limit.
    * ``write_nowait()`` never awaits, so it is safe to call straight from a
      datagram callback (no task-per-packet).
    * ``read(n, timeout=...)`` has a built-in timeout. The fast path (data
      already buffered) does no task creation at all, unlike wrapping every
      read in ``asyncio.wait_for``. Raises ``asyncio.TimeoutError``.
    * ``clear()`` discards buffered audio (used on PTT edges / when TX cannot
      proceed, so stale audio is never replayed later).

    No lock is needed: nothing awaits while the buffer is being mutated.
    """

    def __init__(self, max_bytes: int = 16000):
        if max_bytes < 2:
            raise ValueError('max_bytes must be >= 2')
        self._buf = bytearray()
        self._max = max_bytes & ~1  # keep 16-bit sample alignment
        self._event = asyncio.Event()
        self.dropped = 0  # total bytes discarded because the buffer was full

    @property
    def buffered(self) -> int:
        return len(self._buf)

    def write_nowait(self, data) -> None:
        if not data:
            return
        self._buf += data
        excess = len(self._buf) - self._max
        if excess > 0:
            excess += excess & 1  # drop whole samples only
            del self._buf[:excess]
            self.dropped += excess
        self._event.set()

    async def write(self, data) -> None:
        # Kept async for API compatibility with the original class.
        self.write_nowait(data)

    def clear(self) -> None:
        self._buf.clear()
        self._event.clear()

    async def _wait_for_data(self, timeout):
        if timeout is None:
            while not self._buf:
                await self._event.wait()
                self._event.clear()
            return

        if _asyncio_timeout is not None:
            async with _asyncio_timeout(timeout):
                while not self._buf:
                    await self._event.wait()
                    self._event.clear()
        else:  # pragma: no cover - older Python
            async def _wait():
                while not self._buf:
                    await self._event.wait()
                    self._event.clear()
            await asyncio.wait_for(_wait(), timeout)

    async def read(self, n: int = -1, timeout: float = None) -> bytes:
        """Return between 1 and ``n`` bytes (all buffered bytes if n < 0).

        Waits for data if the buffer is empty; raises ``asyncio.TimeoutError``
        if ``timeout`` (seconds) elapses first.
        """
        if not self._buf:
            await self._wait_for_data(timeout)
        size = len(self._buf) if (n is None or n < 0) else min(n, len(self._buf))
        data = bytes(self._buf[:size])
        del self._buf[:size]
        return data