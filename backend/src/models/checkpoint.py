from sqlalchemy import String, Double, Integer, JSON, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base


class SystemState(Base):
    """整站运行状态：normal / quiescing / capturing / restoring。

    epoch 是恢复栅栏：每次开始恢复递增，worker 用它拒收旧执行迟到的结果，
    也用它让恢复前排入队列的旧消息失效。
    """
    __tablename__ = 'system_state'
    id: Mapped[int] = mapped_column(primary_key=True)
    mode: Mapped[str] = mapped_column(String(20), default='normal')
    epoch: Mapped[int] = mapped_column(Integer, default=0)
    checkpoint_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    restore_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    message: Mapped[str] = mapped_column(String(500), default='')
    updated_at: Mapped[float] = mapped_column(Double, default=0.0)


class Checkpoint(Base):
    """一次整站取点的清单与结果。"""
    __tablename__ = 'checkpoints'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160))
    status: Mapped[str] = mapped_column(String(20), default='creating')  # creating/ready/failed
    step: Mapped[str] = mapped_column(String(40), default='queued')
    progress: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[float] = mapped_column(Double)
    taken_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    created_by: Mapped[str] = mapped_column(String(150), default='')
    directory: Mapped[str] = mapped_column(String(500), default='')
    manifest_checksum: Mapped[str] = mapped_column(String(64), default='')
    data_checksum: Mapped[str] = mapped_column(String(64), default='')
    counts: Mapped[dict] = mapped_column(JSON, default=dict)
    artifacts: Mapped[list] = mapped_column(JSON, default=list)
    error: Mapped[str] = mapped_column(String(1000), default='')
    preflight_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    preflight_ok: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    preflight_actor: Mapped[str] = mapped_column(String(150), default='')
    preflight_detail: Mapped[dict] = mapped_column(JSON, default=dict)


class RestoreOperation(Base):
    """可重入的恢复操作。同一检查点重试时复用同一行，不重复出刊。"""
    __tablename__ = 'restore_operations'
    id: Mapped[int] = mapped_column(primary_key=True)
    checkpoint_id: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(20), default='pending')  # pending/running/finalizing/completed/failed
    step: Mapped[str] = mapped_column(String(40), default='queued')
    progress: Mapped[int] = mapped_column(Integer, default=0)
    phase: Mapped[str] = mapped_column(String(40), default='')
    error: Mapped[str] = mapped_column(String(1000), default='')
    detail: Mapped[dict] = mapped_column(JSON, default=dict)
    actor: Mapped[str] = mapped_column(String(150), default='')
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    epoch: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[float] = mapped_column(Double)
    updated_at: Mapped[float] = mapped_column(Double)
    finished_at: Mapped[float | None] = mapped_column(Double, nullable=True)


class InflightOp(Base):
    """在途业务操作租约。取点/恢复先关闭新租约，再等待此表清空。"""
    __tablename__ = 'inflight_ops'
    id: Mapped[int] = mapped_column(primary_key=True)
    kind: Mapped[str] = mapped_column(String(40))
    detail: Mapped[str] = mapped_column(String(300), default='')
    pid: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[float] = mapped_column(Double)
