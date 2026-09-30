import aiohttp
import asyncio
import base64
import json
import logging
import os
import random
import socket
import struct
import jwt
import time

from datetime import datetime, timedelta, timezone

from pyogg.opus_decoder import OpusDecoder
from pyogg.opus_encoder import OpusEncoder

from .stream import AsyncByteStream


def _env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


AUTH_TOKEN_EXPIRY = 3600
AUTH_TOKEN_EXPIRY_THRESHOLD = 600
REAUTH_MIN_INTERVAL_SEC = 60.0

POST_LOGIN_COOLDOWN_SEC = 0.8
CHANNEL_NOT_READY_BACKOFF_SEC = 0.5
AUTH_WATCHDOG_SEC = 8.0
START_STREAM_TIMEOUT_SEC = 2.0

# Reconnect policy: exponential backoff with jitter, reset once a connection
# has stayed up (and logged in) for RECONNECT_STABLE_SEC.
RECONNECT_BASE_SEC = 5.0
RECONNECT_MAX_SEC = 60.0
RECONNECT_STABLE_SEC = 60.0
KICK_BACKOFF_SEC = 30.0   # minimum wait after 'kicked' (same account elsewhere?)

# If a Zello stream stops sending audio but never sends on_stream_stop, release
# the RX-in-progress flag after this long so TX is not blocked forever.
RX_IDLE_TIMEOUT_SEC = _env_float('ZELLO_RX_IDLE_TIMEOUT_SEC', 2.0)

# Keep the Zello stream open this long after USRP PTT drops (and resume it if
# PTT returns). Prevents squelch flutter causing rapid start/stop cycles that
# trigger 'woodpecker prohibited'.
TX_HANG_SEC = _env_float('ZELLO_TX_HANG_MS', 400) / 1000.0
TX_HOLDOFF_SEC = 0.15     # debounce before opening a new stream
TX_READ_TIMEOUT_SEC = 1.0
BACKOFF_RESET_SEC = 30.0
MAX_UNKNOWN_ERRORS = 5

PTT_IDLE_SLEEP_SEC = 0.003
MAIN_LOOP_YIELD_SEC = 0.001

PCM_FRAME_BYTES = 320     # 20 ms @ 8 kHz, mono, 16-bit


def socket_setup_keepalive(sock):
    # Each option is best-effort: some are Linux-only and none are worth
    # failing the connection over (aiohttp's heartbeat covers dead links).
    for level, name, value in (
            (socket.SOL_SOCKET, 'SO_KEEPALIVE', 1),
            (socket.IPPROTO_TCP, 'TCP_KEEPIDLE', 60),
            (socket.IPPROTO_TCP, 'TCP_KEEPINTVL', 10),
            (socket.IPPROTO_TCP, 'TCP_KEEPCNT', 3),
            (socket.IPPROTO_TCP, 'TCP_NODELAY', 1)):
        opt = getattr(socket, name, None)
        if opt is None:
            continue
        try:
            sock.setsockopt(level, opt, value)
        except Exception:
            pass


class ZelloController:

    def __init__(self,
                 stream_in: AsyncByteStream,
                 stream_out: AsyncByteStream,
                 usrp_ptt: asyncio.Event,
                 zello_ptt: asyncio.Event):
        self._logger = logging.getLogger('ZelloController')
        self._stream_out = stream_out
        self._stream_in = stream_in
        self._stream_id = None
        self._seq = 0

        # Cache environment variables to avoid repeated lookups
        self._zello_username = os.environ.get('ZELLO_USERNAME')
        self._zello_password = os.environ.get('ZELLO_PASSWORD')
        self._zello_channel = os.environ.get('ZELLO_CHANNEL')
        self._zello_issuer = os.environ.get('ZELLO_ISSUER', '')
        self._zello_ws_endpoint = os.environ.get('ZELLO_WS_ENDPOINT')
        self._private_key_path = os.environ.get('ZELLO_PRIVATE_KEY')
        self._ssl_verify = os.environ.get(
            'ZELLO_SSL_VERIFY', '1').strip().lower() not in ('0', 'false', 'no', 'off')
        self._validate_config()

        self._token_expiry = None      # wall-clock datetime (compared with JWT exp)
        self._refresh_token = None
        self._logged_in = False

        self._usrp_ptt = usrp_ptt
        self._zello_ptt = zello_ptt

        self._ws = None
        self._txing = False

        self._auth_lock = asyncio.Lock()

        self._talk_user = None
        self._talk_start = None        # monotonic

        self._usrp_tx_start = None     # monotonic

        self._tasks = []
        self._bg_tasks = set()
        self._shutdown = False

        self._pkt_id = 0
        self._private_key = None
        self.load_private_key()

        self._encoder = self._make_encoder()

        # All timers below are time.monotonic() values (immune to clock steps).
        self._woodpecker_until = None
        self._empty_msg_backoff_until = None
        self._ptt_down_at = None
        self._backoff_state = {}       # kind -> (consecutive count, last time)

        self._in_woodpecker_backoff = False
        self._in_empty_backoff = False

        self._last_skip_key = None
        self._last_skip_reason_at = None

        self._frame_window_start = None
        self._frame_count = 0
        self._frame_bytes = 0

        self._channel_ready = False
        self._auth_in_progress = False
        self._last_login_at = None
        self._channel_backoff_until = None
        self._start_retry_after = None
        self._auth_started_at = None
        self._auth_seq = None
        self._last_reauth_attempt = None

        self._pending = {}             # seq -> Future awaiting that command's reply
        self._kicked = False
        self._session_authed = False
        self._unknown_errors = 0
        self._last_rx_audio = None

        self._stat_start_attempts = 0
        self._stat_start_ok = 0
        self._stat_channel_not_ready = 0
        self._stat_read_timeouts = 0

        self._codec_header_b64 = base64.b64encode(
            struct.pack('<hbb', 8000, 1, 20)).decode('utf8')

    # ------------------------------------------------------------------
    # Setup / auth helpers
    # ------------------------------------------------------------------
    def _validate_config(self):
        missing = [name for name, val in (
            ('ZELLO_WS_ENDPOINT', self._zello_ws_endpoint),
            ('ZELLO_CHANNEL', self._zello_channel),
            ('ZELLO_USERNAME', self._zello_username),
            ('ZELLO_PASSWORD', self._zello_password)) if not val]
        if missing:
            raise RuntimeError(
                'Missing required environment variable(s): ' + ', '.join(missing))
        if not self._ssl_verify:
            self._logger.warning(
                'ZELLO_SSL_VERIFY is disabled: TLS certificates will NOT be verified')

    def get_seq(self):
        seq = self._seq
        self._seq = seq + 1
        return seq

    async def get_token(self):
        if self._private_key:
            self._logger.info('Private key detected, getting Zello Free token')
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self.get_token_free)
        return None

    def load_private_key(self):
        if self._private_key is None and self._private_key_path:
            try:
                with open(self._private_key_path, 'rb') as f:
                    self._private_key = f.read()
            except Exception as e:
                self._logger.error(f'Failed to load private key file: {e}')
        return self._private_key

    def get_token_free(self):
        expiry = datetime.now(timezone.utc) + \
            timedelta(seconds=AUTH_TOKEN_EXPIRY)
        key = self.load_private_key()
        token = jwt.encode({
            'iss': self._zello_issuer,
            'exp': int(expiry.timestamp())
        }, key, algorithm='RS256')
        self._token_expiry = expiry
        return token

    @staticmethod
    def _redact(obj):
        try:
            data = dict(obj)
        except Exception:
            return obj
        for k in ('password', 'auth_token', 'refresh_token'):
            if k in data and data[k] is not None:
                val = str(data[k])
                if k == 'password':
                    data[k] = '<redacted>'
                else:
                    data[k] = val[:12] + '…<redacted>'
        return data

    async def authenticate(self, ws):
        payload = {
            'command': 'logon',
            'seq': self.get_seq(),
            'username': self._zello_username,
            'password': self._zello_password,
            'channels': [self._zello_channel]
        }
        used_refresh = False
        if self._refresh_token is not None:
            self._logger.info('Authenticating with refresh token')
            payload['refresh_token'] = self._refresh_token
            self._refresh_token = None
            used_refresh = True
        else:
            token = await self.get_token()
            if token is not None:
                self._logger.info('Authenticating with new token')
                payload['auth_token'] = token
            else:
                self._logger.info('Authenticating with username/password (Zello Work)')

        self._auth_in_progress = True
        self._auth_started_at = time.monotonic()
        self._auth_seq = payload['seq']

        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(
                f"Sending logon payload: {json.dumps(self._redact(payload))}")
            auth_method = "refresh_token" if used_refresh else ("auth_token" if payload.get("auth_token") else "username/password")
            self._logger.debug(f"Auth method: {auth_method}")
        self._logger.info('Logging in...')
        await ws.send_str(json.dumps(payload))

    # ------------------------------------------------------------------
    # Small utilities
    # ------------------------------------------------------------------
    def _debug_skip(self, reason):
        key = reason.split(' (')[0]
        now = time.monotonic()
        if (self._last_skip_key != key or self._last_skip_reason_at is None
                or now - self._last_skip_reason_at >= 2):
            self._logger.debug("TX skip: %s", reason)
            self._last_skip_key = key
            self._last_skip_reason_at = now

    def _frame_summary_maybe_emit(self):
        if not self._txing:
            self._frame_window_start = None
            self._frame_count = 0
            self._frame_bytes = 0
            return
        if self._frame_window_start is None:
            self._frame_window_start = time.monotonic()
            return
        elapsed = time.monotonic() - self._frame_window_start
        if elapsed >= 1.0 and self._frame_count > 0:
            if self._logger.isEnabledFor(logging.DEBUG):
                pps = self._frame_count / elapsed
                avg = self._frame_bytes / self._frame_count
                kb = self._frame_bytes / 1024.0
                self._logger.debug(
                    f"TX summary: stream_id={self._stream_id} frames={self._frame_count} avg_size={avg:.1f}B pps={pps:.1f} bytes={kb:.2f}KB")
            self._frame_window_start = time.monotonic()
            self._frame_count = 0
            self._frame_bytes = 0

    def _next_backoff(self, kind, base, cap):
        """Consecutive-failure exponential backoff, forgotten after a quiet spell."""
        now = time.monotonic()
        count, last = self._backoff_state.get(kind, (0, None))
        if last is None or now - last > BACKOFF_RESET_SEC:
            count = 0
        count += 1
        self._backoff_state[kind] = (count, now)
        return min(cap, base * (2 ** (count - 1)))

    def _drain_input(self):
        clear = getattr(self._stream_in, 'clear', None)
        if clear is not None:
            clear()

    def _resolve_pending(self, seq, data):
        if seq is None:
            return False
        fut = self._pending.pop(seq, None)
        if fut is None:
            return False
        if not fut.done():
            fut.set_result(data)
        return True

    def _fail_pending(self, reason):
        pending, self._pending = self._pending, {}
        for fut in pending.values():
            if not fut.done():
                fut.set_result({'success': False, 'error': reason})

    async def _send_command(self, payload, timeout):
        """Send a command and await the server reply carrying the same seq."""
        seq = payload['seq']
        fut = asyncio.get_running_loop().create_future()
        self._pending[seq] = fut
        try:
            await self._ws.send_str(json.dumps(payload))
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._pending.pop(seq, None)

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self._bg_tasks.add(task)

        def _on_done(t):
            self._bg_tasks.discard(t)
            if not t.cancelled() and t.exception():
                self._logger.error(f"Background task failed: {t.exception()}")

        task.add_done_callback(_on_done)

    def _reset_connection_state(self):
        self._ws = None
        self._logged_in = False
        self._stream_id = None
        self._txing = False
        self._talk_user = None
        self._talk_start = None
        self._usrp_tx_start = None
        self._channel_ready = False
        self._auth_in_progress = False
        self._channel_backoff_until = None
        self._start_retry_after = None
        self._auth_started_at = None
        self._auth_seq = None
        self._last_rx_audio = None
        self._unknown_errors = 0
        self._fail_pending('connection closed')
        if self._zello_ptt.is_set():
            self._zello_ptt.clear()

    def _check_auth_watchdog(self, now):
        """Clear a stuck auth attempt. Returns True if it tripped before login."""
        if (self._auth_in_progress and self._auth_started_at is not None
                and now - self._auth_started_at > AUTH_WATCHDOG_SEC):
            self._logger.warning("Auth watchdog tripped; clearing auth_in_progress")
            self._auth_in_progress = False
            self._auth_started_at = None
            self._auth_seq = None
            return not self._logged_in
        return False

    def _check_rx_idle(self, now):
        if not self._zello_ptt.is_set() or self._last_rx_audio is None:
            return
        idle = now - self._last_rx_audio
        if idle > RX_IDLE_TIMEOUT_SEC:
            self._logger.warning(
                f'UnKeyed:{self._talk_user or "Unknown"} (no audio for {idle:.1f}s, '
                'no stop message; releasing RX)')
            self._zello_ptt.clear()
            self._talk_user = None
            self._talk_start = None
            self._last_rx_audio = None

    async def _maybe_reauth(self, now):
        ws = self._ws
        if ws is None or ws.closed or self._token_expiry is None or not self._logged_in:
            return
        if self._auth_in_progress or self._txing or self._usrp_ptt.is_set():
            return
        remaining = (self._token_expiry - datetime.now(timezone.utc)).total_seconds()
        if remaining > AUTH_TOKEN_EXPIRY_THRESHOLD:
            return
        if (self._last_reauth_attempt is not None
                and now - self._last_reauth_attempt < REAUTH_MIN_INTERVAL_SEC):
            return
        self._last_reauth_attempt = now
        self._logger.info(f'Access token will expire in {remaining:.0f}s, reauthenticating')
        async with self._auth_lock:
            if self._ws is not None and not self._ws.closed:
                try:
                    await self.authenticate(self._ws)
                except Exception as e:
                    self._logger.error(f'Reauthentication failed: {e}')

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def shutdown(self):
        self._shutdown = True
        for task in self._tasks:
            if not task.done():
                task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        for task in list(self._bg_tasks):
            task.cancel()
        self._fail_pending('shutdown')
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._logger.info(
            f"Stats: start_attempts={self._stat_start_attempts} start_ok={self._stat_start_ok} "
            f"chn_not_ready={self._stat_channel_not_ready} read_timeouts={self._stat_read_timeouts}"
        )

    async def _supervise(self, name, fn):
        """Restart a task if it dies, with backoff, so one bug can't silently
        disable half of the bridge."""
        delay = 1.0
        while not self._shutdown:
            started = time.monotonic()
            try:
                await fn()
                if self._shutdown:
                    return
                self._logger.warning(f'{name} task exited unexpectedly; restarting')
            except asyncio.CancelledError:
                raise
            except Exception:
                self._logger.exception(f'{name} task crashed; restarting')
            if time.monotonic() - started > 60:
                delay = 1.0
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)

    async def run(self):
        try:
            self._tasks = [
                asyncio.create_task(self._supervise('rx', self.run_rx)),
                asyncio.create_task(self._supervise('monitor', self.monitor)),
                asyncio.create_task(self._supervise('tx', self.run_tx))
            ]
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._logger.error(f"Run error: {e}")
        finally:
            await self.shutdown()

    async def monitor(self):
        self._logger.info('Monitor task starting')
        while not self._shutdown:
            now = time.monotonic()
            if self._check_auth_watchdog(now):
                ws = self._ws
                if ws is not None and not ws.closed:
                    self._logger.warning('No logon response; closing connection to retry')
                    await ws.close()
            self._check_rx_idle(now)
            await self._maybe_reauth(now)
            await asyncio.sleep(0.5)

    # ------------------------------------------------------------------
    # TX (radio -> Zello)
    # ------------------------------------------------------------------
    def _tx_blocked_reason(self, now):
        if not self._logged_in:
            return 'not logged in'
        if self._auth_in_progress:
            return 'auth in progress'
        if not self._channel_ready:
            until = self._channel_backoff_until
            if until is not None and now < until:
                return f'channel not ready ({until - now:.2f}s left)'
            return 'channel not ready'
        if self._last_login_at is not None:
            since = now - self._last_login_at
            if since < POST_LOGIN_COOLDOWN_SEC:
                return f'post-login cooldown ({POST_LOGIN_COOLDOWN_SEC - since:.2f}s left)'
        if self._start_retry_after is not None and now < self._start_retry_after:
            return f'waiting retry window ({self._start_retry_after - now:.2f}s left)'
        return None

    async def start_tx(self):
        """Open a Zello stream. Returns True once we hold a stream_id."""
        ws = self._ws
        if ws is None or ws.closed:
            return False
        now = time.monotonic()
        reason = self._tx_blocked_reason(now)
        if reason:
            self._debug_skip(reason)
            return False

        self._stat_start_attempts += 1
        self._stream_id = None
        self._usrp_tx_start = now
        seq_val = self.get_seq()
        start_payload = {
            'command': 'start_stream',
            'seq': seq_val,
            'channel': self._zello_channel,
            'type': 'audio',
            'codec': 'opus',
            'codec_header': self._codec_header_b64,
            'packet_duration': 20
        }
        self._logger.debug("Requesting Zello stream (seq=%s)...", seq_val)
        try:
            reply = await self._send_command(start_payload, START_STREAM_TIMEOUT_SEC)
        except asyncio.TimeoutError:
            self._logger.error('Failed to get stream_id within timeout')
            self._set_retry_after(time.monotonic() + CHANNEL_NOT_READY_BACKOFF_SEC)
            return False
        except Exception as e:
            self._logger.error(f'Failed to start stream: {e}')
            return False

        stream_id = reply.get('stream_id') if reply.get('success') else None
        if stream_id is None:
            self._logger.debug('start_stream refused: %s', self._redact(reply))
            self._set_retry_after(time.monotonic() + CHANNEL_NOT_READY_BACKOFF_SEC)
            return False

        self._stream_id = stream_id
        self._txing = True
        self._pkt_id = 0
        self._frame_window_start = None
        self._frame_count = 0
        self._frame_bytes = 0
        self._start_retry_after = None
        self._stat_start_ok += 1
        self._logger.debug("Started Zello stream %s (seq=%s)", stream_id, seq_val)
        return True

    def _set_retry_after(self, when):
        if self._start_retry_after is None or when > self._start_retry_after:
            self._start_retry_after = when

    async def _send_stop(self, stream_id):
        ws = self._ws
        if ws is None or ws.closed:
            return
        seq_val = self.get_seq()
        stop_payload = {
            'command': 'stop_stream',
            'seq': seq_val,
            'channel': self._zello_channel,
            'stream_id': stream_id
        }
        self._logger.debug("Requesting stop for Zello stream %s (seq=%s)", stream_id, seq_val)
        try:
            await ws.send_str(json.dumps(stop_payload))
        except Exception as e:
            self._logger.error(f'Failed to send stop_stream: {e}')

    async def _end_tx(self):
        sid = self._stream_id
        if sid is None:
            if self._txing:
                self._logger.warning('Ending TX but no stream_id available')
        else:
            await self._send_stop(sid)
        self._usrp_tx_start = None
        self._txing = False
        self._stream_id = None

    def _make_encoder(self):
        encoder = OpusEncoder()
        encoder.set_application('voip')
        encoder.set_sampling_frequency(8000)
        encoder.set_channels(1)
        try:
            comp = int(os.getenv('OPUS_COMPLEXITY', '5'))
            try:
                encoder.set_complexity(comp)
            except Exception:
                pass
            br_env = os.getenv('OPUS_BITRATE', '')
            if br_env:
                try:
                    encoder.set_bitrate(int(br_env))
                except Exception:
                    pass
        except Exception:
            pass
        return encoder

    async def _read_frame(self, buf, timeout):
        """Return exactly one 20 ms PCM frame (320 bytes) from stream_in.

        Partial data is kept in ``buf`` across calls. Raises
        asyncio.TimeoutError if no more data arrives within ``timeout``.
        """
        while len(buf) < PCM_FRAME_BYTES:
            chunk = await self._stream_in.read(PCM_FRAME_BYTES - len(buf), timeout=timeout)
            buf += chunk
        frame = bytes(buf[:PCM_FRAME_BYTES])
        del buf[:PCM_FRAME_BYTES]
        return frame

    async def _send_audio(self, encoder, pcm):
        """Encode and send one frame. Returns False if the stream is unusable."""
        sid = self._stream_id
        if not self._txing or not isinstance(sid, int):
            if self._txing:
                self._logger.warning(f'Invalid stream_id: {sid}, stopping transmission')
            return False
        try:
            opus = encoder.encode(pcm)
        except Exception as e:
            self._logger.error(f'Opus encode failed: {e}')
            return True
        frame = struct.pack('>bii', 1, sid, self._pkt_id) + bytes(opus)
        self._pkt_id = (self._pkt_id + 1) & 0x7FFFFFFF
        ws = self._ws
        if ws is None or ws.closed:
            return False
        try:
            await ws.send_bytes(frame)
        except Exception as e:
            self._logger.error(f'Failed to send audio frame: {e}')
            return False
        self._frame_count += 1
        self._frame_bytes += len(opus)
        self._frame_summary_maybe_emit()
        return True

    def _in_backoff(self, now):
        wp = self._woodpecker_until is not None and now < self._woodpecker_until
        if wp != self._in_woodpecker_backoff:
            self._in_woodpecker_backoff = wp
            self._logger.debug('Woodpecker backoff %s', 'started' if wp else 'ended, resuming TX attempts')
        em = (self._empty_msg_backoff_until is not None
              and now < self._empty_msg_backoff_until)
        if em != self._in_empty_backoff:
            self._in_empty_backoff = em
            self._logger.debug('Empty-message backoff %s', 'started' if em else 'ended, resuming TX attempts')
        return wp or em

    async def run_tx(self):
        self._logger.debug('run_tx starting')
        encoder = self._encoder
        sending = False
        first_pcm_logged = False
        pcm_buf = bytearray()
        ptt_low_since = None
        last_audio_at = None
        try:
            while not self._shutdown:
                now = time.monotonic()

                ptt = self._usrp_ptt.is_set()
                if ptt:
                    ptt_low_since = None
                    if self._ptt_down_at is None:
                        self._ptt_down_at = now
                        self._logger.info('Keyed:USRP')
                elif self._ptt_down_at is not None:
                    self._logger.info(f'UnKeyed:USRP ({now - self._ptt_down_at:.1f}s)')
                    self._ptt_down_at = None
                    ptt_low_since = now

                if sending and not self._txing:
                    sending = False
                    first_pcm_logged = False
                    pcm_buf.clear()

                ws = self._ws
                if ws is None or ws.closed:
                    sending = False
                    first_pcm_logged = False
                    pcm_buf.clear()
                    self._drain_input()
                    await asyncio.sleep(0.5)
                    continue

                if self._in_backoff(now):
                    pcm_buf.clear()
                    self._drain_input()
                    await asyncio.sleep(0.05)
                    continue

                if not ptt and not sending:
                    pcm_buf.clear()
                    self._drain_input()
                    first_pcm_logged = False
                    try:
                        await asyncio.wait_for(self._usrp_ptt.wait(), timeout=0.2)
                    except asyncio.TimeoutError:
                        pass
                    continue

                if not ptt:
                    try:
                        pcm = await self._read_frame(pcm_buf, timeout=0.02)
                    except asyncio.TimeoutError:
                        pcm = None
                    if pcm is not None:
                        if not await self._send_audio(encoder, pcm):
                            sending = False
                            first_pcm_logged = False
                            pcm_buf.clear()
                            await self._end_tx()
                        continue
                    if ptt_low_since is None or time.monotonic() - ptt_low_since >= TX_HANG_SEC:
                        if pcm_buf:
                            tail = bytes(pcm_buf).ljust(PCM_FRAME_BYTES, b'\x00')
                            await self._send_audio(encoder, tail)
                            pcm_buf.clear()
                        await self._end_tx()
                        sending = False
                        first_pcm_logged = False
                        self._drain_input()
                    continue

                if not sending and now - self._ptt_down_at < TX_HOLDOFF_SEC:
                    await asyncio.sleep(0.01)
                    continue

                try:
                    pcm = await self._read_frame(pcm_buf, timeout=0.05)
                except asyncio.TimeoutError:
                    ref = last_audio_at if last_audio_at is not None else self._ptt_down_at
                    if ref is not None and time.monotonic() - ref >= TX_READ_TIMEOUT_SEC:
                        self._stat_read_timeouts += 1
                        self._debug_skip("stream_in read timeout")
                        pcm_buf.clear()
                        last_audio_at = time.monotonic()
                        if sending:
                            await self._end_tx()
                            sending = False
                            first_pcm_logged = False
                    continue

                now = time.monotonic()
                last_audio_at = now
                if self._zello_ptt.is_set():
                    self._debug_skip("RX in progress (zello_ptt set)")
                    continue
                reason = self._tx_blocked_reason(now)
                if reason:
                    self._debug_skip(reason)
                    continue

                if not first_pcm_logged and self._ptt_down_at is not None:
                    self._logger.debug(
                        "First PCM after PTT: %.1f ms",
                        (now - self._ptt_down_at) * 1000.0)
                    first_pcm_logged = True

                if not sending:
                    if not await self.start_tx() or not self._txing:
                        self._debug_skip("start_tx failed or no stream_id")
                        continue
                    sending = True

                if not await self._send_audio(encoder, pcm):
                    sending = False
                    first_pcm_logged = False
                    pcm_buf.clear()
                    await self._end_tx()
        except asyncio.CancelledError:
            self._logger.debug('TX task cancelled')
            if sending:
                await self._end_tx()
            raise
        except Exception:
            if sending:
                try:
                    await self._end_tx()
                except Exception:
                    pass
            raise

    # ------------------------------------------------------------------
    # RX (Zello -> radio)
    # ------------------------------------------------------------------
    async def run_rx(self):
        self._logger.debug('run_rx starting')

        decoder = OpusDecoder()
        decoder.set_channels(1)
        decoder.set_sampling_frequency(8000)

        delay = RECONNECT_BASE_SEC
        while not self._shutdown:
            started = time.monotonic()
            self._kicked = False
            self._session_authed = False
            try:
                await self._run_session(decoder)
            except asyncio.CancelledError:
                self._logger.debug('RX task cancelled')
                raise
            except aiohttp.ClientConnectorError as e:
                self._logger.warning(f'Connection error: {e}')
            except asyncio.TimeoutError:
                self._logger.warning('Connection/authentication timeout')
            except Exception as e:
                self._logger.error(f'WebSocket error: {e}')
            finally:
                self._reset_connection_state()

            if self._shutdown:
                break

            uptime = time.monotonic() - started
            if self._session_authed and uptime >= RECONNECT_STABLE_SEC:
                delay = RECONNECT_BASE_SEC
            wait = delay
            if self._kicked:
                wait = max(wait, KICK_BACKOFF_SEC)
            wait *= random.uniform(0.8, 1.2)
            self._logger.info(f'Reconnecting in {wait:.0f}s')
            await asyncio.sleep(wait)
            delay = min(delay * 2, RECONNECT_MAX_SEC)
        self._logger.debug('RX task exiting')

    async def _run_session(self, decoder):
        kwargs = {'family': socket.AF_INET}
        if not self._ssl_verify:
            kwargs['ssl'] = False
        conn = aiohttp.TCPConnector(**kwargs)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=10)

        self._logger.info(f"Connecting to {self._zello_ws_endpoint}")
        async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
            ws = await asyncio.wait_for(
                session.ws_connect(self._zello_ws_endpoint, autoping=True, heartbeat=30.0),
                20)
            try:
                self._logger.debug("WebSocket connection established")
                self._tune_socket(ws)
                async with self._auth_lock:
                    await asyncio.wait_for(self.authenticate(ws), 10)
                self._ws = ws
                await self._read_loop(ws, decoder)
            finally:
                if not ws.closed:
                    await ws.close()

    def _tune_socket(self, ws):
        try:
            sock = ws._response.connection.transport.get_extra_info('socket')
            if sock is not None:
                socket_setup_keepalive(sock)
        except Exception as e:
            self._logger.debug(f'Could not tune socket: {e}')

    async def _read_loop(self, ws, decoder):
        start_time = time.monotonic()
        async for msg in ws:
            if self._shutdown:
                return
            if msg.type == aiohttp.WSMsgType.TEXT:
                if not await self._handle_text(msg):
                    return
            elif msg.type == aiohttp.WSMsgType.BINARY:
                await self._handle_binary(msg, decoder)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                self._logger.error(f'WebSocket error: {msg.data}')
                return
            else:
                self._logger.warning(f'Unhandled message: {msg}')

        self._logger.warning('Websocket closed!')
        self._logger.debug(
            "WebSocket closed code=%s uptime=%.1fs tx_active=%s channel_ready=%s",
            getattr(ws, 'close_code', None), time.monotonic() - start_time,
            self._txing, self._channel_ready)

    async def _handle_binary(self, msg, decoder):
        data = msg.data
        self._logger.debug("RX BINARY %d bytes", len(data))
        if len(data) <= 9 or data[0] != 1:
            self._logger.debug("Ignoring non-audio binary message (%d bytes)", len(data))
            return
        self._last_rx_audio = time.monotonic()
        try:
            pcm = decoder.decode(bytearray(data[9:]))
            await self._stream_out.write(pcm)
        except Exception as e:
            self._logger.error(f'Failed to decode audio: {e} bytes={len(data)}')

    async def _handle_text(self, msg):
        try:
            data = json.loads(msg.data)
        except json.JSONDecodeError as e:
            self._logger.error(f'Failed to parse JSON: {e}')
            return True
        if not isinstance(data, dict):
            return True
        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(f"RX TEXT: {json.dumps(self._redact(data))}")

        if 'error' in data:
            return await self._handle_error(data)
        if 'command' in data:
            self._handle_command(data)
        if 'success' in data:
            return self._handle_success(data)
        return True

    async def _handle_error(self, data):
        err_msg = data.get('error')
        seq = data.get('seq')
        self._resolve_pending(seq, data)

        if err_msg == 'kicked':
            self._logger.error(f'Kicked from channel: {self._redact(data)}')
            self._kicked = True
            return False

        if err_msg == 'woodpecker prohibited':
            self._logger.warning(f'Woodpecker protection triggered: {self._redact(data)}')
            backoff = self._next_backoff('woodpecker', 3.0, 8.0)
            self._woodpecker_until = time.monotonic() + backoff
            self._logger.debug("Applying woodpecker backoff for %.1fs", backoff)
            if self._txing:
                await self._end_tx()
            return True

        if err_msg == 'empty message':
            self._logger.warning(f'Server error: {self._redact(data)}')
            backoff = self._next_backoff('empty', 1.0, 8.0)
            self._empty_msg_backoff_until = time.monotonic() + backoff
            self._logger.debug("Applying empty-message backoff for %.1fs", backoff)
            if self._txing:
                await self._end_tx()
            return True

        if err_msg == 'channel is not ready':
            self._logger.warning(f"Channel not ready (seq={seq})")
            self._stat_channel_not_ready += 1
            self._channel_ready = False
            self._channel_backoff_until = time.monotonic() + CHANNEL_NOT_READY_BACKOFF_SEC
            self._start_retry_after = self._channel_backoff_until
            return True

        self._logger.error(f'Server error: {self._redact(data)}')
        if not self._logged_in or (seq is not None and seq == self._auth_seq):
            self._logger.error('Authentication failed')
            return False
        self._unknown_errors += 1
        return self._unknown_errors < MAX_UNKNOWN_ERRORS

    def _handle_command(self, data):
        cmd = data['command']
        if cmd == 'on_stream_start':
            user = data.get('from') or data.get('user') or data.get('username') or data.get('display_name')
            self._talk_user = user
            self._talk_start = time.monotonic()
            self._last_rx_audio = self._talk_start
            self._logger.debug("Talk user set: %s", self._talk_user)
            self._logger.info(f'Keyed:{user}' if user else 'Keyed:Unknown')
            self._zello_ptt.set()
        elif cmd == 'on_stream_stop':
            dur = None
            if self._talk_start is not None:
                dur = time.monotonic() - self._talk_start
            user = data.get('from') or data.get('user') or data.get('username') or data.get('display_name') or self._talk_user
            if user is not None and dur is not None:
                self._logger.info(f'UnKeyed:{user} ({dur:.1f}s)')
            elif user is not None:
                self._logger.info(f'UnKeyed:{user}')
            else:
                self._logger.info('UnKeyed:Unknown')
            self._zello_ptt.clear()
            self._talk_user = None
            self._talk_start = None
            self._last_rx_audio = None
        elif cmd == 'on_channel_status':
            status = data.get('status')
            desired = (status == 'online')
            if self._channel_ready != desired:
                self._logger.debug("Channel ready -> %s (status='%s')", desired, status)
            self._channel_ready = desired
            if desired:
                self._logger.info("Channel is ready")
                if not self._logged_in:
                    self._mark_logged_in()

    def _mark_logged_in(self):
        if not self._logged_in:
            self._logger.info('Logged in!')
        self._logged_in = True
        self._session_authed = True
        self._unknown_errors = 0

    def _handle_success(self, data):
        seq = data.get('seq')
        ok = bool(data.get('success'))
        matched = self._resolve_pending(seq, data)

        if ok and 'stream_id' in data and not matched:
            sid = data['stream_id']
            if not (self._txing and self._stream_id == sid):
                self._logger.debug("Closing orphaned stream %s", sid)
                self._spawn(self._send_stop(sid))

        is_auth_reply = (self._auth_seq is not None and seq == self._auth_seq)
        if is_auth_reply and not ok:
            self._logger.error(f'Authentication failed: {self._redact(data)}')
            return False

        if is_auth_reply:
            if self._auth_started_at is not None:
                self._logger.debug(
                    "Auth completed in ~%.0f ms",
                    (time.monotonic() - self._auth_started_at) * 1000.0)
            self._auth_in_progress = False
            self._auth_started_at = None
            self._auth_seq = None
            self._last_login_at = time.monotonic()
            self._start_retry_after = None
            self._mark_logged_in()

        if ok and 'refresh_token' in data:
            self._logger.info('Authentication successful!')
            self._refresh_token = data['refresh_token']
            self._auth_in_progress = False
            self._auth_started_at = None
            self._auth_seq = None
            self._last_login_at = time.monotonic()
            self._start_retry_after = None
            self._mark_logged_in()
            try:
                exp = jwt.decode(self._refresh_token, options={"verify_signature": False}).get('exp')
                if exp:
                    self._token_expiry = datetime.fromtimestamp(exp, tz=timezone.utc)
                    self._logger.debug("Refresh token expiry set to %s", self._token_expiry)
            except Exception as e:
                self._logger.debug(f'Failed to decode refresh token expiry: {e}')
        return True