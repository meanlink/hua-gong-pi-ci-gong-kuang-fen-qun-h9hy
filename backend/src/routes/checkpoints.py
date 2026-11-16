from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import func, select

from src.config.database import get_db
from src.middlewares.auth import admin_only, current_user
from src.models import Checkpoint, Dataset, Job, Report, RestoreOperation
from src.services import checkpoints as service
from src.services.audit import record

router = APIRouter(prefix='/checkpoints')


class CheckpointInput(BaseModel):
    name: str = Field(min_length=2, max_length=160)


@router.get('')
def listing(db=Depends(get_db), user=Depends(current_user)):
    items = [_cp_view(db, cp) for cp in db.scalars(select(Checkpoint).order_by(Checkpoint.id.desc()).limit(50))]
    return {'state': service.state_dict(db), 'items': items}


@router.post('', status_code=201)
def create(data: CheckpointInput, db=Depends(get_db), user=Depends(admin_only)):
    cp = service.create_checkpoint(db, data.name.strip(), user.email)
    record(db, user.email, 'checkpoint.requested', {'id': cp.id, 'name': cp.name})
    db.commit()
    cp_id = cp.id
    service.start_capture(cp_id)
    cp = db.get(Checkpoint, cp_id)
    return _cp_view(db, cp)


@router.get('/status')
def status(db=Depends(get_db), user=Depends(current_user)):
    return _status_payload(db)


@router.get('/{cp_id}')
def detail(cp_id: int, db=Depends(get_db), user=Depends(current_user)):
    cp = db.get(Checkpoint, cp_id)
    if not cp:
        raise HTTPException(404, '检查点不存在')
    return _cp_view(db, cp)


@router.post('/{cp_id}/preflight')
def preflight_check(cp_id: int, db=Depends(get_db), user=Depends(admin_only)):
    return service.preflight(cp_id, user.email)


@router.post('/{cp_id}/restore', status_code=202)
def restore(cp_id: int, db=Depends(get_db), user=Depends(admin_only)):
    op, started = service.request_restore(cp_id, user.email)
    return {'operation': op, 'started': started, 'state': service.state_dict(db)}


@router.get('/restores/{op_id}')
def restore_detail(op_id: int, db=Depends(get_db), user=Depends(current_user)):
    op = service.get_operation(op_id)
    if not op:
        raise HTTPException(404, '恢复操作不存在')
    return op


def _cp_view(db, cp):
    data = service.checkpoint_summary(cp)
    data['restore'] = None
    op = service.latest_operation(cp.id)
    if op:
        data['restore'] = op
    return data


def _status_payload(db):
    state = service.state_dict(db)
    cp = db.get(Checkpoint, state['checkpoint_id']) if state['checkpoint_id'] else None
    op = service.get_operation(state['restore_id']) if state['restore_id'] else None
    if op is None:
        last_id = db.scalar(select(RestoreOperation.id).order_by(RestoreOperation.id.desc()))
        op = service.get_operation(last_id) if last_id else None
    listing = []
    for item_cp in db.scalars(select(Checkpoint).order_by(Checkpoint.id.desc()).limit(20)):
        entry = service.checkpoint_summary(item_cp)
        entry['restore'] = service.latest_operation(item_cp.id)
        listing.append(entry)
    if op is not None:
        op['checkpoint_name'] = db.get(Checkpoint, op['checkpoint_id']).name if db.get(Checkpoint, op['checkpoint_id']) else ''
    counts = {
        'datasets': db.scalar(select(func.count()).select_from(Dataset)),
        'jobs': db.scalar(select(func.count()).select_from(Job)),
        'reports': db.scalar(select(func.count()).select_from(Report)),
    }
    return {'state': state, 'checkpoint': service.checkpoint_summary(cp) if cp else None,
            'operation': op, 'items': listing, 'counts': counts}
