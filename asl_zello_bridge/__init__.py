"""AllStarLink ⇔ Zello bridge.

Heavy dependencies (aiohttp, pyogg, jwt) live in ``zello`` and are imported
lazily so stream/usrp/health can be tested and reused without them.

Entry point: ``python -m asl_zello_bridge``.
"""

__all__ = ['AsyncByteStream', 'USRPController', 'ZelloController']


def __getattr__(name):
    if name == 'AsyncByteStream':
        from .stream import AsyncByteStream
        return AsyncByteStream
    if name == 'USRPController':
        from .usrp import USRPController
        return USRPController
    if name == 'ZelloController':
        from .zello import ZelloController
        return ZelloController
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
