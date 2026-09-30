import asyncio
import logging
import math
import os
import socket
import struct
import time
from array import array

from .stream import AsyncByteStream

USRP_FRAME_TIME = 0.02
USRP_FRAME_SIZE = 352
USRP_HEADER_SIZE = 32
USRP_VOICE_SIZE = USRP_FRAME_SIZE - USRP_HEADER_SIZE

USRP_TYPE_VOICE = 0


def _env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


USRP_GAIN_RX_DB = _env_float('USRP_GAIN_RX_DB', 0)
USRP_GAIN_TX_DB = _env_float('USRP_GAIN_TX_DB', 0)

# If no valid PTT frame arrives from the node for this long, assume the
# un-key frame was lost and release the radio-side PTT.
USRP_RX_PTT_TIMEOUT_SEC = _env_float('USRP_RX_PTT_TIMEOUT_MS', 500) / 1000.0

# TX (Zello -> radio) timing.
USRP_GAP_TIMEOUT_SEC = 0.06     # audio gap tolerated while Zello is still sending
USRP_HANG_SEC = 0.15            # idle time after Zello stops before un-keying
USRP_MAX_GAP_FILL_SEC = _env_float('USRP_MAX_GAP_FILL_MS', 2000) / 1000.0


def db_to_linear(db):
    # Amplitude (sample) gain: 20*log10.
    return math.pow(10, db / 20)


def apply_gain(buf, gain):
    # Fast path: no-op gain
    if gain == 1.0:
        return buf

    n = len(buf) // 2
    if n == 0:
        return b''

    # In-place scaling without multi-pass list allocations
    samples = array('h')
    samples.frombytes(bytes(buf[:n * 2]))
    for i in range(len(samples)):
        s = int(gain * samples[i])
        samples[i] = -32768 if s < -32768 else (32767 if s > 32767 else s)
    return samples.tobytes()


class USRPController(asyncio.DatagramProtocol):
    def __init__(self,
                 stream_in: AsyncByteStream,
                 stream_out: AsyncByteStream,
                 usrp_ptt: asyncio.Event,
                 zello_ptt: asyncio.Event):

        self._logger = logging.getLogger('USRPController')

        self._stream_in = stream_in
        self._stream_out = stream_out

        host = os.environ.get('USRP_HOST')
        if not host:
            raise RuntimeError('USRP_HOST must be set')

        self._tx_seq = 0
        self._tx_host = host
        self._tx_port = int(os.environ.get('USRP_TXPORT', 7070))
        self._tx_addr = None  # resolved once in run()
        self._transport = None

        # Optional: only accept datagrams from the resolved USRP_HOST address.
        self._strict_source = os.environ.get(
            'USRP_STRICT_SOURCE', '').strip().lower() in ('1', 'true', 'yes', 'on')
        self._allowed_sources = set()

        self._usrp_ptt = usrp_ptt
        self._zello_ptt = zello_ptt
        self._ptt_timer = None

        self._usrp_gain_rx = db_to_linear(USRP_GAIN_RX_DB)
        self._usrp_gain_tx = db_to_linear(USRP_GAIN_TX_DB)

        self._logger.info(f'USRP RX gain: {USRP_GAIN_RX_DB}dB = {self._usrp_gain_rx}')
        self._logger.info(f'USRP TX gain: {USRP_GAIN_TX_DB}dB = {self._usrp_gain_tx}')

    # ------------------------------------------------------------------
    # Datagram (radio -> Zello) side
    # ------------------------------------------------------------------
    def connection_made(self, transport):
        self._transport = transport

    def connection_lost(self, exc):
        if self._ptt_timer is not None:
            self._ptt_timer.cancel()
            self._ptt_timer = None

    def _ptt_expired(self):
        self._ptt_timer = None
        if self._usrp_ptt.is_set():
            self._logger.warning('No USRP frames received; releasing PTT (lost un-key frame?)')
            self._usrp_ptt.clear()

    def datagram_received(self, data, addr):
        if len(data) < USRP_HEADER_SIZE or data[:4] != b'USRP':
            return
        if self._strict_source and addr[0] not in self._allowed_sources:
            return

        try:
            _, _, ptt, _, frame_type, _, _ = self._rx_decode_state(data)
        except struct.error:
            return
        if frame_type != USRP_TYPE_VOICE:
            return

        if self._ptt_timer is not None:
            self._ptt_timer.cancel()
            self._ptt_timer = None

        if ptt != 1:
            self._usrp_ptt.clear()
            return

        self._usrp_ptt.set()
        self._ptt_timer = asyncio.get_running_loop().call_later(
            USRP_RX_PTT_TIMEOUT_SEC, self._ptt_expired)

        frame = data[USRP_HEADER_SIZE:]
        frame = frame[:len(frame) & ~1]  # whole 16-bit samples only
        if not frame:
            return

        if self._usrp_gain_rx != 1.0:
            frame = apply_gain(frame, self._usrp_gain_rx)

        self._stream_out.write_nowait(frame)

    # ------------------------------------------------------------------
    # Framing helpers
    # ------------------------------------------------------------------
    def _tx_encode_state(self, ptt=True):
        seq = self._get_seq()
        return 'USRP'.encode('ascii') \
            + struct.pack('>iiiiiii',
                          seq, 0,
                          ptt, 0,
                          USRP_TYPE_VOICE, 0, 0)

    def _rx_decode_state(self, frame):
        header = frame[4:USRP_HEADER_SIZE]
        seq, mem, ptt, tg, type, mpx, res = struct.unpack('>iiiiiii', header)
        return (seq, mem, ptt, tg, type, mpx, res)

    def _frame_ptt_state(self, frame):
        state = self._rx_decode_state(frame)
        return state[2] == 1

    def _get_seq(self):
        self._tx_seq = (self._tx_seq + 1) & 0x7FFFFFFF
        return self._tx_seq

    def _tx_frame(self, pcm):
        header = self._tx_encode_state(ptt=True)

        if self._usrp_gain_tx != 1.0:
            pcm = apply_gain(pcm, self._usrp_gain_tx)

        self._tx(header + pcm)

    def _tx_off(self):
        self._tx(self._tx_encode_state(ptt=False))

    def _tx(self, frame):
        if self._transport is not None and self._tx_addr is not None:
            self._transport.sendto(frame, self._tx_addr)

    def _clear_input(self):
        clear = getattr(self._stream_in, 'clear', None)
        if clear is not None:
            clear()

    async def _resolve_tx_address(self):
        """Resolve USRP_HOST once (retrying) instead of on every sendto()."""
        loop = asyncio.get_running_loop()
        delay = 1.0
        while True:
            family = socket.AF_INET
            sock = self._transport.get_extra_info('socket') if self._transport else None
            if sock is not None:
                family = sock.family
            try:
                infos = await loop.getaddrinfo(
                    self._tx_host, self._tx_port,
                    family=family, type=socket.SOCK_DGRAM)
                self._tx_addr = infos[0][4]
                self._allowed_sources = {info[4][0] for info in infos}
                self._logger.info(f'USRP TX target: {self._tx_addr}')
                return
            except OSError as e:
                self._logger.warning(
                    f'Cannot resolve USRP_HOST {self._tx_host!r}: {e}; retrying in {delay:.0f}s')
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    # ------------------------------------------------------------------
    # Zello -> radio side
    # ------------------------------------------------------------------
    async def run(self):
        # rx is handled by DatagramProtocol parent class
        await self._resolve_tx_address()
        await self.run_tx()

    async def run_tx(self):
        while True:
            try:
                await self._tx_loop()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._logger.exception('USRP TX loop failed; restarting')
                await asyncio.sleep(0.5)

    @staticmethod
    async def _pace(next_tx):
        next_tx += USRP_FRAME_TIME
        now = time.monotonic()
        delay = next_tx - now
        if delay > 0:
            await asyncio.sleep(delay)
            return next_tx
        return now

    async def _tx_loop(self):
        next_tx = time.monotonic()
        buf = b''
        keyed = False
        gap_since = None
        silence = bytes(USRP_VOICE_SIZE)

        try:
            while True:
                if not keyed and not buf and not self._zello_ptt.is_set():
                    await self._zello_ptt.wait()
                    next_tx = time.monotonic()
                    gap_since = None

                zello_active = self._zello_ptt.is_set()
                if zello_active and keyed and gap_since is not None:
                    # Bridging a gap: wake exactly when the next 20 ms frame is
                    # due so silence goes out in real time.
                    timeout = max(0.001, next_tx - time.monotonic())
                else:
                    timeout = USRP_GAP_TIMEOUT_SEC if zello_active else USRP_HANG_SEC

                try:
                    chunk = await self._stream_in.read(
                        USRP_VOICE_SIZE - len(buf), timeout=timeout)
                except asyncio.TimeoutError:
                    if buf:
                        self._tx_frame(buf.ljust(USRP_VOICE_SIZE, b'\x00'))
                        buf = b''
                        keyed = True
                        next_tx = await self._pace(next_tx)

                    if zello_active and keyed:
                        # Zello is still sending but audio is late/missing
                        # (network jitter). Hold the radio keyed with silence
                        # rather than un-keying and re-keying mid-transmission.
                        now = time.monotonic()
                        if gap_since is None:
                            gap_since = now
                        if now - gap_since < USRP_MAX_GAP_FILL_SEC:
                            self._tx_frame(silence)
                            # The read timeout already waited until next_tx.
                            next_tx += USRP_FRAME_TIME
                            if next_tx <= now:
                                next_tx = now + USRP_FRAME_TIME
                            continue

                    # End of transmission.
                    if keyed:
                        self._tx_off()
                        keyed = False
                    gap_since = None
                    self._clear_input()
                    next_tx = time.monotonic()
                    continue

                if not chunk:
                    continue
                buf += chunk
                gap_since = None

                if len(buf) < USRP_VOICE_SIZE:
                    continue

                pcm, buf = buf[:USRP_VOICE_SIZE], buf[USRP_VOICE_SIZE:]
                self._tx_frame(pcm)
                keyed = True
                next_tx = await self._pace(next_tx)
        finally:
            if keyed:
                try:
                    self._tx_off()
                except Exception:
                    pass