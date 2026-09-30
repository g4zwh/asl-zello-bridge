"""Tiny stdlib HTTP health endpoint for systemd / monitoring.

Disabled unless ``HEALTH_PORT`` is a positive integer. Bind defaults to
loopback so it is not accidentally exposed.
"""

import json
import logging
import os
import time

logger = logging.getLogger('Health')


def _env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return int(default)


HEALTH_BIND = os.environ.get('HEALTH_BIND', '127.0.0.1')
HEALTH_PORT = _env_int('HEALTH_PORT', 0)


def build_payload(zello, usrp):
    now = time.monotonic()
    zello_health = zello.health(now) if hasattr(zello, 'health') else {}
    usrp_health = usrp.health(now) if hasattr(usrp, 'health') else {}
    ok = bool(zello_health.get('logged_in') and zello_health.get('channel_ready'))
    return {
        'ok': ok,
        'zello': zello_health,
        'usrp': usrp_health,
    }


async def start_health_server(zello, usrp, host=None, port=None):
    """Serve GET /health (and any other path) as JSON. Returns the server."""
    import asyncio

    host = HEALTH_BIND if host is None else host
    port = HEALTH_PORT if port is None else port
    if not port:
        return None

    async def _handle(reader, writer):
        status = 200
        try:
            await reader.read(1024)
            payload = build_payload(zello, usrp)
            status = 200 if payload.get('ok') else 503
            body = json.dumps(payload, default=str).encode('utf-8')
        except Exception:
            logger.exception('Health handler failed')
            status = 500
            body = b'{"ok": false}'
        reason = b'OK' if status == 200 else (b'Service Unavailable' if status == 503 else b'Error')
        try:
            writer.write(
                b'HTTP/1.1 %d %s\r\n'
                b'Content-Type: application/json\r\n'
                b'Content-Length: %d\r\n'
                b'Connection: close\r\n'
                b'\r\n' % (status, reason, len(body))
            )
            writer.write(body)
            await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    server = await asyncio.start_server(_handle, host, port)
    sockets = server.sockets or []
    bound = sockets[0].getsockname() if sockets else (host, port)
    logger.info('Health endpoint on http://%s:%s/health', bound[0], bound[1])
    return server
