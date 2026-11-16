import time

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from redis import Redis

from src.config.settings import REDIS_URL

# 恢复期间业务读写一律拒绝；取点期间拒绝业务写入，读取仍可用于查看进度。
BUSINESS_READ_PREFIXES = ('/datasets', '/jobs', '/reports', '/deliveries', '/overview', '/audits')
BUSINESS_WRITE_PREFIXES = ('/datasets', '/jobs', '/reports', '/deliveries')
CHECKPOINT_PREFIXES = ('/checkpoints',)
OPEN_PREFIXES = ('/health', '/auth', '/meta', '/docs', '/openapi.json', '/redoc')


def _matches(path, prefixes):
    return any(path == prefix or path.startswith(prefix + '/') for prefix in prefixes)


class RequestControl(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = uuid_hex()
        if request.url.path != '/health':
            try:
                r = Redis.from_url(REDIS_URL, socket_timeout=1)
                key = 'rate:' + str(request.client.host) + ':' + str(int(time.time() // 60))
                n = r.incr(key)
                if n == 1:
                    r.expire(key, 65)
                r.close()
                if n > 240:
                    return JSONResponse({'detail': '请求过于频繁，请稍后重试'}, status_code=429)
            except Exception:
                pass
        blocked = await self._gate(request)
        if blocked is not None:
            blocked.headers['X-Request-ID'] = request_id
            return blocked
        response = await call_next(request)
        response.headers['X-Request-ID'] = request_id
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    async def _gate(self, request):
        path = request.url.path
        if path.startswith('/api'):
            path = path[4:]
        if not _matches(path, BUSINESS_READ_PREFIXES):
            return None
        from src.services import checkpoints as service
        db = service.Session()
        try:
            mode = service._ensure_state(db).mode
        finally:
            db.close()
        if mode == service.MODE_RESTORING:
            return JSONResponse({'detail': '整站检查点恢复中，业务读写与派发已暂停，请稍后重试',
                                 'state': mode}, status_code=503)
        if mode != service.MODE_NORMAL and request.method.upper() in ('POST', 'PUT', 'PATCH', 'DELETE'):
            return JSONResponse({'detail': '整站检查点取点中，业务写入与派发已暂停，请稍后重试',
                                 'state': mode}, status_code=503)
        return None


def uuid_hex():
    import uuid
    return uuid.uuid4().hex[:12]
