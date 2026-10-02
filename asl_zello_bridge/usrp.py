import asyncio
import logging
import math
import os
import socket
import struct
import time

from .stream import AsyncByteStream

USRP_FRAME_TIME = 0.02
USRP_FRAME_SIZE = 352
USRP_HEADER_SIZE = 32
USRP_VOICE_SIZE = USRP_FRAME_SIZE - USRP_HEADER_SIZE

USRP_TYPE_VOICE = 0
USRP_MAGIC = b'USRP'

# chan_usrp is local UDP; three back-to-back unkeys survive a single drop.
USRP_UNKEY_FRAMES = 3
USRP_SOCK_BUF = 256 * 1024


def _env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


def _env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return int(default)


def _env_bool(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ('1', 'true', 'yes', 'on')


USRP_GAIN_RX_DB = _env_float('USRP_GAIN_RX_DB', 0)
USRP_GAIN_TX_DB = _env_float('USRP_GAIN_TX_DB', 0)

# If no valid PTT frame arrives from the node for this long, assume the
# un-key frame was lost and release the radio-side PTT.
USRP_RX_PTT_TIMEOUT_SEC = _env_float('USRP_RX_PTT_TIMEOUT_MS', 500) / 1000.0

# TX (Zello -> radio) timing.
USRP_GAP_TIMEOUT_SEC = 0.06     # audio gap tolerated while Zello is still sending
USRP_HANG_SEC = 0.15            # idle time after Zello stops before un-keying
USRP_MAX_GAP_FILL_SEC = _env_float('USRP_MAX_GAP_FILL_MS', 2000) / 1000.0

# Drop radio->Zello audio while Zello is talking (ASL duplex=0). Set
# USRP_HALF_DUPLEX=0 to pass both directions at once.
USRP_HALF_DUPLEX = _env_bool('USRP_HALF_DUPLEX', True)


def db_to_linear(db):
    # Amplitude (sample) gain: 20*log10. The original used 10*log10, which is
    # a *power* ratio and applied double the configured dB to the samples.
    return math.pow(10, db / 20)


def clamp_short(sh):
    return int(max(-32768, min(32767, sh)))


def apply_gain(buf, gain):
    """Scale little-endian s16 PCM. Host endianness is ignored on purpose."""
    n = len(buf) // 2
    if n == 0:
        return b''
    samples = struct.unpack_from('<%dh' % n, buf)
    return struct.pack('<%dh' % n, *[clamp_short(gain * s) for s in samples])


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
        # README / ASL default is 32001 (not the historical 7070).
        self._tx_port = _env_int('USRP_TXPORT', 32001)
        self._tx_addr = None  # resolved once in run()
        self._transport = None
        self._shutdown = False
        self._unkey_frames = max(1, _env_int('USRP_UNKEY_FRAMES', USRP_UNKEY_FRAMES))

        # Optional: only accept datagrams from the resolved USRP_HOST address.
        self._strict_source = _env_bool('USRP_STRICT_SOURCE', False)
        self._allowed_sources = set()
        self._half_duplex = USRP_HALF_DUPLEX

        self._usrp_ptt = usrp_ptt
        self._zello_ptt = zello_ptt
        self._ptt_timer = None

        self._usrp_gain_rx = db_to_linear(USRP_GAIN_RX_DB)
        self._usrp_gain_tx = db_to_linear(USRP_GAIN_TX_DB)

        self._logger.info(f'USRP RX gain: {USRP_GAIN_RX_DB}dB = {self._usrp_gain_rx}')
        self._logger.info(f'USRP TX gain: {USRP_GAIN_TX_DB}dB = {self._usrp_gain_tx}')
        if self._half_duplex:
            self._logger.info('USRP half-duplex: radio TX ignored while Zello is talking')

    def health(self, now=None):
        addr = self._tx_addr
        return {
            'socket': self._transport is not None,
            'tx_target': None if addr is None else f'{addr[0]}:{addr[1]}',
            'radio_keyed': self._usrp_ptt.is_set(),
            'zello_keyed': self._zello_ptt.is_set(),
            'buffered': getattr(self._stream_in, 'buffered', None),
            'dropped': getattr(self._stream_in, 'dropped', 0),
        }

    # ------------------------------------------------------------------
    # Datagram (radio -> Zello) side
    # ------------------------------------------------------------------
    def connection_made(self, transport):
        self._transport = transport
        sock = transport.get_extra_info('socket')
        if sock is None:
            return
        for opt, val in (
                (socket.SO_RCVBUF, USRP_SOCK_BUF),
                (socket.SO_SNDBUF, USRP_SOCK_BUF)):
            try:
                sock.setsockopt(socket.SOL_SOCKET, opt, val)
            except OSError:
                pass

    def connection_lost(self, exc):
        if self._ptt_timer is not None:
            self._ptt_timer.cancel()
            self._ptt_timer = None
        if self._usrp_ptt.is_set():
            self._usrp_ptt.clear()
        # Drop the closed transport so _tx() cannot sendto() it.
        self._transport = None
        if exc:
            self._logger.warning('USRP socket lost: %s', exc)
        else:
            self._logger.warning('USRP socket closed')

    def _ptt_expired(self):
        self._ptt_timer = None
        if self._usrp_ptt.is_set():
            self._logger.warning('No USRP frames received; releasing PTT (lost un-key frame?)')
            self._usrp_ptt.clear()

    def datagram_received(self, data, addr):
        if len(data) < USRP_HEADER_SIZE or data[:4] != USRP_MAGIC:
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

        # Half-duplex: do not key the Zello TX path while Zello audio is
        # already going out to the radio (prevents feedback / double-key).
        if self._half_duplex and self._zello_ptt.is_set():
            return

        self._usrp_ptt.set()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not None:
            self._ptt_timer = loop.call_later(
                USRP_RX_PTT_TIMEOUT_SEC, self._ptt_expired)

        # One 20 ms voice payload; extra bytes would burst into Zello.
        frame = data[USRP_HEADER_SIZE:USRP_HEADER_SIZE + USRP_VOICE_SIZE]
        frame = frame[:len(frame) & ~1]
        if not frame:
            return

        if self._usrp_gain_rx != 1:
            frame = apply_gain(frame, self._usrp_gain_rx)

        self._stream_out.write_nowait(frame)

    # ------------------------------------------------------------------
    # Framing helpers
    # ------------------------------------------------------------------
    def _tx_encode_state(self, ptt=True):
        seq = self._get_seq()
        return USRP_MAGIC + struct.pack('>iiiiiii',
                                        seq, 0,
                                        1 if ptt else 0, 0,
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

        if self._usrp_gain_tx != 1:
            pcm = apply_gain(pcm, self._usrp_gain_tx)

        if len(pcm) < USRP_VOICE_SIZE:
            pcm = pcm.ljust(USRP_VOICE_SIZE, b'\x00')
        elif len(pcm) > USRP_VOICE_SIZE:
            pcm = pcm[:USRP_VOICE_SIZE]

        self._tx(header + pcm)

    def _tx_off(self):
        # Repeat so a single lost UDP packet cannot leave the radio keyed.
        for _ in range(self._unkey_frames):
            self._tx(self._tx_encode_state(ptt=False))

    def _tx(self, frame):
        transport = self._transport
        addr = self._tx_addr
        if transport is None or addr is None:
            return
        try:
            transport.sendto(frame, addr)
        except (OSError, AttributeError) as e:
            self._logger.warning('USRP send failed: %s', e)

    def _clear_input(self):
        clear = getattr(self._stream_in, 'clear', None)
        if clear is not None:
            clear()

    async def _resolve_tx_address(self):
        """Resolve USRP_HOST once (retrying) instead of on every sendto()."""
        loop = asyncio.get_running_loop()
        delay = 1.0
        while not self._shutdown:
            family = socket.AF_INET
            sock = self._transport.get_extra_info('socket') if self._transport else None
            if sock is not None:
                family = sock.family
            try:
                infos = await loop.getaddrinfo(
                    self._tx_host, self._tx_port,
                    family=family, type=socket.SOCK_DGRAM)
                if not infos:
                    raise OSError('getaddrinfo returned no results')
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
    async def shutdown(self):
        self._shutdown = True
        try:
            self._tx_off()
        except Exception:
            pass
        if self._ptt_timer is not None:
            self._ptt_timer.cancel()
            self._ptt_timer = None
        if self._usrp_ptt.is_set():
            self._usrp_ptt.clear()

    async def run(self):
        # rx is handled by DatagramProtocol parent class
        try:
            await self._resolve_tx_address()
        except asyncio.CancelledError:
            raise
        except Exception:
            self._logger.exception(
                'USRP address resolution failed unexpectedly; TX will not start')
            return
        if self._shutdown or self._tx_addr is None:
            return
        try:
            await self.run_tx()
        except asyncio.CancelledError:
            raise
        except Exception:
            self._logger.exception('USRP run_tx crashed out')

    async def run_tx(self):
        while not self._shutdown:
            try:
                await self._tx_loop()
            except asyncio.CancelledError:
                try:
                    self._tx_off()
                except Exception:
                    pass
                raise
            except Exception:
                self._logger.exception('USRP TX loop failed; restarting')
                await asyncio.sleep(0.5)

    @staticmethod
    async def _pace(next_tx):
        # PACING FIX: keep the absolute schedule when late; snap forward only
        # if more than one frame behind (avoids a catch-up burst).
        next_tx += USRP_FRAME_TIME
        now = time.monotonic()
        delay = next_tx - now
        if delay > 0:
            await asyncio.sleep(delay)
            return next_tx
        if now - next_tx > USRP_FRAME_TIME:
            next_tx = now
        return next_tx

    async def _tx_loop(self):
        next_tx = time.monotonic()
        buf = b''
        keyed = False
        gap_since = None
        silence = bytes(USRP_VOICE_SIZE)

        try:
            while not self._shutdown:
                if not keyed and not buf and not self._zello_ptt.is_set():
                    await self._zello_ptt.wait()
                    if self._shutdown:
                        return
                    next_tx = time.monotonic()
                    gap_since = None

                zello_active = self._zello_ptt.is_set()
                if keyed and zello_active:
                    # Wake when the next 20 ms frame is due so the first
                    # missing packet is filled with silence immediately
                    # (a 60 ms first-gap wait punched a hole in chan_usrp).
                    timeout = max(0.001, next_tx - time.monotonic())
                elif keyed and not zello_active:
                    timeout = USRP_HANG_SEC
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
