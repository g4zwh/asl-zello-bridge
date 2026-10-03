import asyncio
import logging
import os
import signal

from .health import HEALTH_BIND, HEALTH_PORT, start_health_server
from .stream import AsyncByteStream
from .usrp import USRPController
from .zello import ZelloController

log_level = os.environ.get('LOG_LEVEL', 'INFO')
log_format = os.environ.get('LOG_FORMAT', '%(levelname)s:%(name)s:%(message)s')
logging.basicConfig(level=log_level, format=log_format)
logger = logging.getLogger('__main__')


def _env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return int(default)


async def _main():
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    stop_signals = [0]

    def _request_stop():
        stop_signals[0] += 1
        if stop_signals[0] == 1:
            logger.info('Shutdown requested (signal again to force)')
            stop.set()
        else:
            logger.warning('Second signal received; forcing exit')
            raise SystemExit(1)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            # Windows / restricted environments: KeyboardInterrupt still works.
            pass

    # Names match the data direction so the wiring is obvious.
    usrp_to_zello = AsyncByteStream()  # radio PCM → Zello TX
    zello_to_usrp = AsyncByteStream()  # Zello PCM → radio TX

    usrp_ptt = asyncio.Event()
    zello_ptt = asyncio.Event()

    logger.info('Initialising Zello')
    zello = ZelloController(usrp_to_zello, zello_to_usrp, usrp_ptt, zello_ptt)

    logger.info('Initialising USRP')
    usrp = USRPController(zello_to_usrp, usrp_to_zello, usrp_ptt, zello_ptt)

    bind = os.environ.get('USRP_BIND', '0.0.0.0')
    rxport = _env_int('USRP_RXPORT', 34001)
    logger.info('USRP RX bind %s:%s', bind, rxport)

    transport, _protocol = await loop.create_datagram_endpoint(
        lambda: usrp,
        local_addr=(bind, rxport))

    health_srv = None
    if HEALTH_PORT:
        try:
            health_srv = await start_health_server(
                zello, usrp, HEALTH_BIND, HEALTH_PORT)
        except Exception:
            logger.exception('Health server failed to start; continuing without it')

    tasks = [
        asyncio.create_task(zello.run(), name='zello'),
        asyncio.create_task(usrp.run(), name='usrp'),
        asyncio.create_task(stop.wait(), name='stop'),
    ]

    try:
        done, _pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if task.get_name() == 'stop':
                continue
            if task.cancelled():
                logger.warning('Task %s was cancelled', task.get_name())
                continue
            exc = task.exception()
            if exc is not None:
                logger.error('Task %s failed: %s', task.get_name(), exc)
            else:
                logger.warning(
                    'Task %s exited without error; shutting down',
                    task.get_name())
    finally:
        logger.info('Shutting down')
        try:
            await zello.shutdown()
        except Exception:
            logger.exception('Zello shutdown failed')
        try:
            await usrp.shutdown()
        except Exception:
            logger.exception('USRP shutdown failed')
        try:
            transport.close()
        except Exception:
            pass
        if health_srv is not None:
            health_srv.close()
            try:
                await health_srv.wait_closed()
            except Exception:
                pass
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main():
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        # add_signal_handler normally catches this; this path only fires on
        # platforms where it raised NotImplementedError.
        logger.info('Interrupted')


if __name__ == '__main__':
    main()
