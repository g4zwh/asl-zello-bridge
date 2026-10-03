import asyncio

try:
    _asyncio_timeout = asyncio.timeout  # Python 3.11+
except AttributeError:  # pragma: no cover - older Python
    _asyncio_timeout = None


class AsyncByteStream:
    """A bounded, single-consumer async byte FIFO.

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
    * ``dropped`` is a running total of discarded bytes; call
      ``dropped_since_last_check()`` from a monitor task to log overruns
      without losing the lifetime counter.

    Wakeup mechanism:
      Readers re-check ``_buf`` after every wakeup, and nothing awaits
      between the emptiness check and ``event.clear()``, so on a single
      event loop thread no wakeup can be lost.

    No lock is needed: nothing awaits while the buffer is being mutated,
    and ``write_nowait`` is only ever called from the event loop thread.
    """

    def __init__(self, max_bytes: int = 16000):
        if max_bytes < 2:
            raise ValueError('max_bytes must be >= 2')
        self._buf = bytearray()
        self._max = max_bytes & ~1  # keep 16-bit sample alignment
        self._event = asyncio.Event()
        self.dropped = 0  # total bytes discarded because the buffer was full
        self._dropped_seen = 0

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
        self._event.set()   # a parked reader wakes, sees empty, re-parks

    def dropped_since_last_check(self) -> int:
        """Bytes dropped since the previous call (or since construction)."""
        n = self.dropped - self._dropped_seen
        self._dropped_seen = self.dropped
        return n

    async def _wait_for_data(self, timeout):
        # The event is only ever set by write_nowait()/clear(), both of
        # which are called from the event loop thread with no await in
        # between, so there is no window in which a wakeup can be lost.
        if self._buf:
            return

        if timeout is None:
            while not self._buf:
                self._event.clear()
                await self._event.wait()
                # Only a generation bump that actually produced data (or a
                # buffer that is now non-empty) ends the wait. A bare
                # clear() bump means "keep waiting"; returning here would
                # let read() hand back an empty bytes object.
                if self._buf:
                    return
            return

        if _asyncio_timeout is not None:
            async with _asyncio_timeout(timeout):
                while not self._buf:
                    self._event.clear()
                    await self._event.wait()
                    if self._buf:
                        return
        else:  # pragma: no cover - older Python
            async def _wait():
                while not self._buf:
                    self._event.clear()
                    await self._event.wait()
                    if self._buf:
                        return
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