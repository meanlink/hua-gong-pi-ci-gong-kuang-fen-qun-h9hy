from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from src.config.database import get_db
from src.models import User
from src.services import checkpoints as checkpoint_service
from src.utils.security import decode_token
bearer = HTTPBearer(auto_error=False)


def current_user(token: HTTPAuthorizationCredentials | None = Depends(bearer), db=Depends(get_db)):
    try:
        user = db.get(User, decode_token(token.credentials))
        if user is None:
            raise ValueError('missing user')
        return user
    except Exception:
        raise HTTPException(401, '登录已过期，请重新登录')


def can_write(user=Depends(current_user)):
    if user.role not in ('admin', 'analyst'):
        raise HTTPException(403, '观察员只有读取权限')
    return user


def admin_only(user=Depends(current_user)):
    if user.role != 'admin':
        raise HTTPException(403, '需要业务负责人批准')
    return user


def _write_lease(user):
    """业务写入通道：登记在途租约，取点会等这次写入提交后才固定时点。"""
    # 注意：路由处理函数（以及 here 之后的生成器体）在依赖注入阶段只执行到首个 yield，
    # 因此租约必须在 yield 之前登记，请求结束（响应发出）后才释放。
    try:
        lease = checkpoint_service.inflight('api.write', 'business write')
        lease.__enter__()
    except checkpoint_service.BusyError as exc:
        raise HTTPException(503, str(exc))
    try:
        yield user
    finally:
        lease.__exit__(None, None, None)


def write_lease(user=Depends(can_write)):
    yield from _write_lease(user)


def admin_write(user=Depends(admin_only)):
    yield from _write_lease(user)
