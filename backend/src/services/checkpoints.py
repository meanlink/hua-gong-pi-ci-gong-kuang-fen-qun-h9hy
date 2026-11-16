"""整站检查点：取点、预检、可重入恢复与旧执行栅栏。

取点：关闭写入与派发 -> 等待在途操作收口 -> 固定同一时点的数据库记录与归档文件。
恢复：校验清单与指纹 -> 完整暂存 -> 记录与文件一次性切换 -> 失败回滚旧版 -> 恢复出的计划暂停。
"""
import hashlib
import json
import logging
import os
import shutil
import threading
import time
import uuid
from contextlib import contextmanager

from fastapi import HTTPException
from redis import Redis
from sqlalchemy import delete, func, select

from src.config.database import Session
from src.config.settings import ARTIFACT_DIR, REDIS_URL
from src.models import Checkpoint, Dataset, Delivery, InflightOp, Job, Report, RestoreOperation, SystemState
from src.services.audit import record
from src.utils.serialization import checksum

log = logging.getLogger(__name__)

CHECKPOINT_DIR = os.getenv('CHECKPOINT_DIR') or os.path.join(os.path.dirname(ARTIFACT_DIR.rstrip('/')) or '/', 'checkpoints')

MODE_NORMAL = 'normal'
MODE_QUIESCING = 'quiescing'
MODE_CAPTURING = 'capturing'
MODE_RESTORING = 'restoring'

QUIESCE_TIMEOUT = float(os.getenv('CHECKPOINT_QUIESCE_TIMEOUT', '90'))
DRAIN_TIMEOUT = float(os.getenv('RESTORE_DRAIN_TIMEOUT', '30'))
STALE_INFLIGHT = float(os.getenv('INFLIGHT_STALE_SECONDS', '900'))

_process_lock = threading.RLock()
_active_ops = set()

DATASET_FIELDS = ['id', 'name', 'source', 'records', 'checksum', 'created_at']
JOB_FIELDS = ['id', 'name', 'dataset_id', 'params', 'status', 'result', 'error', 'approved', 'created_by',
              'created_at', 'finished_at', 'lease_epoch']
REPORT_FIELDS = ['id', 'name', 'job_id', 'destination', 'interval_seconds', 'next_run', 'enabled',
                 'created_by', 'restore_paused']
DELIVERY_FIELDS = ['id', 'report_id', 'created_at', 'checksum', 'filename', 'destination']


class RestoreError(Exception):
    pass


class BusyError(Exception):
    pass


# --------------------------------------------------------------------------- 状态

def _ensure_state(db):
    st = db.get(SystemState, 1)
    if st is None:
        st = SystemState(id=1, mode=MODE_NORMAL, epoch=0, checkpoint_id=None, restore_id=None,
                         message='', updated_at=time.time())
        db.add(st)
        try:
            db.commit()
        except Exception:
            db.rollback()
            st = db.get(SystemState, 1)
    return st


def _locked_state(db):
    """锁住状态行，串行化“关闭写入”和“登记在途操作”。"""
    st = db.get(SystemState, 1)
    if st is None:
        st = _ensure_state(db)
    try:
        st = db.execute(select(SystemState).where(SystemState.id == 1).with_for_update()).scalar_one()
    except Exception:
        db.rollback()
        st = db.get(SystemState, 1)
    return st


def state_dict(db):
    st = _ensure_state(db)
    return {'mode': st.mode, 'epoch': st.epoch, 'checkpoint_id': st.checkpoint_id,
            'restore_id': st.restore_id, 'message': st.message, 'updated_at': st.updated_at}


def checkpoint_summary(cp):
    """检查点清单摘要；只暴露文件数与指纹，不返回内部目录。"""
    if not cp:
        return None
    data = _cp_dict(cp)
    data.pop('directory', None)
    return data


def latest_operation(cp_id):
    with Session() as db:
        op = db.scalar(select(RestoreOperation).where(RestoreOperation.checkpoint_id == cp_id)
                       .order_by(RestoreOperation.id.desc()))
        return _op_dict(op) if op else None


def get_operation(op_id):
    with Session() as db:
        op = db.get(RestoreOperation, op_id)
        return _op_dict(op) if op else None


def get_epoch(db=None):
    own = db is None
    db = db or Session()
    try:
        st = _ensure_state(db)
        return int(st.epoch), st.mode
    finally:
        if own:
            db.close()


def _set_state(mode, epoch=None, checkpoint_id='keep', restore_id='keep', message=''):
    with Session() as db:
        st = _locked_state(db)
        st.mode = mode
        if epoch is not None:
            st.epoch = epoch
        if checkpoint_id != 'keep':
            st.checkpoint_id = checkpoint_id
        if restore_id != 'keep':
            st.restore_id = restore_id
        st.message = message
        st.updated_at = time.time()
        db.commit()
        return st.epoch


@contextmanager
def inflight(kind, detail=''):
    """登记一个在途业务操作；取点会等待这些操作收口。

    登记前锁住整站状态行：要么本操作先登记、取点随后的等待会包含它；
    要么取点先关闭写入、本操作被拒绝。
    """
    op_id = None
    db = Session()
    try:
        st = _locked_state(db)
        if st.mode != MODE_NORMAL:
            raise BusyError(_mode_reject_message(st.mode))
        op = InflightOp(kind=kind, detail=str(detail)[:300], pid=os.getpid(), started_at=time.time())
        db.add(op)
        db.commit()
        op_id = op.id
    finally:
        db.close()
    try:
        yield
    finally:
        if op_id is not None:
            with Session() as cleanup:
                cleanup.execute(delete(InflightOp).where(InflightOp.id == op_id))
                cleanup.commit()


def _mode_reject_message(mode):
    return {
        MODE_QUIESCING: '整站检查点取点中，业务写入与派发已暂停，请稍后重试',
        MODE_CAPTURING: '整站检查点正在固定数据时点，业务写入与派发已暂停，请稍后重试',
        MODE_RESTORING: '整站检查点恢复中，业务读写与派发已暂停，请稍后重试',
    }.get(mode, '业务暂不可用，请稍后重试')


def _wait_quiescent(timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with Session() as db:
            cutoff = time.time() - STALE_INFLIGHT
            stale = list(db.scalars(select(InflightOp).where(InflightOp.started_at < cutoff)))
            for item in stale:
                db.delete(item)
            if stale:
                db.commit()
        if _quiesced():
            return True
        time.sleep(0.25)
    return False


def _quiesced(db=None):
    own = db is None
    db = db or Session()
    try:
        ops = db.scalar(select(func.count()).select_from(InflightOp)) or 0
        running = db.scalar(select(func.count()).select_from(Job).where(Job.status == 'running')) or 0
        return ops == 0 and running == 0
    finally:
        if own:
            db.close()


def _enter_capturing(cp_id, timeout):
    """在途操作清空后，锁住状态行把整站切到取点态。

    与 inflight 登记使用同一把行锁：要么本函数先关闭写入，之后的新操作被拒绝；
    要么新操作先登记，本函数看到后再等它收口。由此保证记录与文件取自同一稳定窗口。
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        with Session() as db:
            st = _locked_state(db)
            if not _quiesced(db):
                db.rollback()
                time.sleep(0.15)
                continue
            st.mode = MODE_CAPTURING
            st.checkpoint_id = cp_id
            st.message = '整站取点：正在固定数据时点'
            st.updated_at = time.time()
            db.commit()
            return True
    return False


def _wait_drain(timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with Session() as db:
            cutoff = time.time() - STALE_INFLIGHT
            stale = list(db.scalars(select(InflightOp).where(InflightOp.started_at < cutoff)))
            for item in stale:
                db.delete(item)
            if stale:
                db.commit()
            ops = db.scalar(select(func.count()).select_from(InflightOp)) or 0
        if ops == 0:
            return True
        time.sleep(0.2)
    return False


def _clear_queue():
    try:
        r = Redis.from_url(REDIS_URL, socket_timeout=2)
        r.delete('analysis:queue')
        r.close()
    except Exception:
        logging.debug('Redis queue not reachable while clearing stale messages')


def message_epoch(raw):
    """队列消息格式为 job_id:epoch；旧格式按 epoch 0 处理。"""
    text = raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
    if ':' in text:
        job_id, _, epoch = text.partition(':')
        try:
            return int(job_id), int(epoch)
        except ValueError:
            return None, None
    try:
        return int(text), 0
    except ValueError:
        return None, None


def recover_running_jobs():
    """worker 重启后把原地执行中的任务放回队列，且不跨栅栏复活旧执行。"""
    with Session() as db:
        epoch, mode = get_epoch(db)
        if mode != MODE_NORMAL:
            return 0
        recovered = 0
        for job in db.scalars(select(Job).where(Job.status == 'running')):
            job.status = 'queued'
            job.lease_epoch = epoch
            record(db, 'worker', 'job.recovered', {'id': job.id, 'epoch': epoch})
            recovered += 1
        db.commit()
    return recovered


# --------------------------------------------------------------------------- 清单与文件

def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _artifact_paths(root):
    found = []
    if not os.path.isdir(root):
        return found
    for base, dirs, files in os.walk(root):
        dirs[:] = sorted(d for d in dirs if not d.startswith('.'))
        for name in sorted(files):
            if name.startswith('.'):
                continue
            full = os.path.join(base, name)
            rel = os.path.relpath(full, root).replace(os.sep, '/')
            found.append(rel)
    return found


def _safe_join(root, rel):
    rel = rel.replace('\\', '/')
    if rel.startswith('/') or '..' in rel.split('/') or not rel:
        raise RestoreError('检查点清单包含非法文件路径')
    full = os.path.abspath(os.path.join(root, *rel.split('/')))
    if not full.startswith(os.path.abspath(root) + os.sep):
        raise RestoreError('检查点清单包含非法文件路径')
    return full


def _payload_for(db):
    datasets = [dict({f: getattr(row, f) for f in DATASET_FIELDS})
                for row in db.scalars(select(Dataset).order_by(Dataset.id))]
    jobs = [dict({f: getattr(row, f) for f in JOB_FIELDS})
            for row in db.scalars(select(Job).order_by(Job.id))]
    reports = [dict({f: getattr(row, f) for f in REPORT_FIELDS})
               for row in db.scalars(select(Report).order_by(Report.id))]
    deliveries = [dict({f: getattr(row, f) for f in DELIVERY_FIELDS})
                  for row in db.scalars(select(Delivery).order_by(Delivery.id))]
    return {'datasets': datasets, 'jobs': jobs, 'reports': reports, 'deliveries': deliveries}


def _payload_bytes(payload):
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, allow_nan=False).encode('utf-8')


def _payload_counts(payload):
    return {key: len(items) for key, items in payload.items()}


def _cp_dict(cp):
    return {'id': cp.id, 'name': cp.name, 'status': cp.status, 'step': cp.step, 'progress': cp.progress,
            'created_at': cp.created_at, 'taken_at': cp.taken_at, 'created_by': cp.created_by,
            'directory': cp.directory, 'manifest_checksum': cp.manifest_checksum,
            'data_checksum': cp.data_checksum, 'counts': cp.counts or {}, 'artifacts': cp.artifacts or [],
            'error': cp.error, 'preflight_at': cp.preflight_at,
            'preflight_ok': cp.preflight_ok, 'preflight_actor': cp.preflight_actor,
            'preflight_detail': cp.preflight_detail or {}}


def _op_dict(op):
    return {'id': op.id, 'checkpoint_id': op.checkpoint_id, 'status': op.status, 'step': op.step,
            'phase': op.phase, 'progress': op.progress, 'error': op.error, 'detail': op.detail or {},
            'actor': op.actor, 'attempts': op.attempts, 'epoch': op.epoch,
            'created_at': op.created_at, 'updated_at': op.updated_at, 'finished_at': op.finished_at}


# --------------------------------------------------------------------------- 取点

def create_checkpoint(db, name, actor):
    if db.get(SystemState, 1) is not None and _ensure_state(db).mode != MODE_NORMAL:
        raise HTTPException(409, _mode_reject_message(_ensure_state(db).mode))
    cp = Checkpoint(name=name, status='creating', step='queued', progress=0, created_at=time.time(),
                    created_by=actor, directory='', manifest_checksum='', data_checksum='',
                    counts={}, artifacts=[], error='', preflight_detail={})
    db.add(cp)
    db.flush()
    return cp


def start_capture(cp_id):
    """同步关闭写入后，用后台线程等待在途操作并落盘。"""
    try:
        with Session() as db:
            st = _locked_state(db)
            if st.mode != MODE_NORMAL:
                raise HTTPException(409, _mode_reject_message(st.mode))
            cp = db.get(Checkpoint, cp_id)
            if not cp:
                raise HTTPException(404, '检查点不存在')
            st.mode = MODE_QUIESCING
            st.checkpoint_id = cp_id
            st.message = '整站取点：写入与派发已暂停，等待在途操作收口'
            st.updated_at = time.time()
            db.commit()
    except Exception:
        _update_cp(cp_id, status='failed', step='failed', error='整站当前无法取点，请稍后重试')
        raise
    threading.Thread(target=_capture_worker, args=(cp_id,), daemon=True).start()


def _update_cp(cp_id, **fields):
    with Session() as db:
        cp = db.get(Checkpoint, cp_id)
        if not cp:
            return
        for key, value in fields.items():
            setattr(cp, key, value)
        db.commit()


def _capture_worker(cp_id):
    tmp_dir = None
    acquired = False
    try:
        _update_cp(cp_id, step='quiescing', progress=10)
        # 恢复操作持有同一把进程锁；取点的固定与提交在同一把锁内完成，
        # 期间的业务写入/新恢复请求会立即被拒绝，而不是排在锁后。
        if not _process_lock.acquire(blocking=False):
            raise RestoreError('另一项检查点或恢复操作正在进行，请稍后重试')
        acquired = True
        if not _enter_capturing(cp_id, QUIESCE_TIMEOUT):
            raise RestoreError('在途操作未能在限定时间内收口，检查点未生成')
        _update_cp(cp_id, step='snapshotting', progress=35)

        with Session() as db:
            payload = _payload_for(db)

        payload_bytes = _payload_bytes(payload)
        data_checksum = hashlib.sha256(payload_bytes).hexdigest()
        artifact_rels = _artifact_paths(ARTIFACT_DIR)

        os.makedirs(CHECKPOINT_DIR, exist_ok=True)
        tmp_dir = os.path.join(CHECKPOINT_DIR, f'.tmp-{cp_id}-{uuid.uuid4().hex[:8]}')
        os.makedirs(tmp_dir, exist_ok=False)
        entries = []
        total = 0
        for index, rel in enumerate(artifact_rels):
            src = _safe_join(ARTIFACT_DIR, rel)
            if not os.path.isfile(src):
                continue
            dst = _safe_join(tmp_dir, rel)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copyfile(src, dst)
            digest = _sha256_file(dst)
            if digest != _sha256_file(src):
                raise RestoreError(f'归档文件复制后指纹不符：{rel}')
            size = os.path.getsize(dst)
            entries.append({'path': rel, 'size': size, 'sha256': digest})
            total += size
            if artifact_rels:
                _update_cp(cp_id, progress=min(85, 35 + int(50 * (index + 1) / len(artifact_rels))))
        with open(os.path.join(tmp_dir, 'data.json'), 'wb') as fh:
            fh.write(payload_bytes)
        if _sha256_file(os.path.join(tmp_dir, 'data.json')) != data_checksum:
            raise RestoreError('数据记录写入后指纹不符')

        taken_at = time.time()
        manifest = {'version': 1, 'checkpoint_id': cp_id, 'taken_at': taken_at,
                    'payload': {'file': 'data.json', 'checksum': data_checksum,
                                'counts': _payload_counts(payload)},
                    'artifacts': entries, 'artifact_bytes': total}
        manifest_checksum = checksum(manifest)
        manifest['manifest_checksum'] = manifest_checksum
        with open(os.path.join(tmp_dir, 'manifest.json'), 'w', encoding='utf-8') as fh:
            json.dump(manifest, fh, ensure_ascii=False, sort_keys=True, indent=2)

        final_dir = os.path.join(CHECKPOINT_DIR, f'checkpoint-{cp_id}-{uuid.uuid4().hex[:8]}')
        os.replace(tmp_dir, final_dir)
        tmp_dir = None
        _update_cp(cp_id, status='ready', step='ready', progress=100, taken_at=taken_at,
                   directory=final_dir, manifest_checksum=manifest_checksum, data_checksum=data_checksum,
                   counts=_payload_counts(payload), artifacts=entries, error='')
        with Session() as db:
            record(db, 'system', 'checkpoint.created',
                   {'id': cp_id, 'counts': _payload_counts(payload), 'artifacts': len(entries)})
            db.commit()
        _set_state(MODE_NORMAL, checkpoint_id=None, message='')
    except Exception as exc:
        log.exception('Checkpoint capture failed')
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        _update_cp(cp_id, status='failed', step='failed', progress=100, error=str(exc)[:990])
        with Session() as db:
            record(db, 'system', 'checkpoint.failed', {'id': cp_id, 'error': str(exc)[:500]})
            db.commit()
        try:
            _set_state(MODE_NORMAL, checkpoint_id=None, message='')
        except Exception:
            log.exception('Unable to leave checkpoint capture state')
    finally:
        if acquired:
            _process_lock.release()


# --------------------------------------------------------------------------- 预检

def verify_checkpoint(cp):
    problems = []
    if not cp or cp.status != 'ready':
        return ['检查点尚未就绪']
    root = cp.directory or ''
    if not root or not os.path.isdir(root):
        return ['检查点目录不存在']
    manifest_path = os.path.join(root, 'manifest.json')
    if not os.path.isfile(manifest_path):
        return ['检查点清单文件缺失']
    try:
        with open(manifest_path, 'r', encoding='utf-8') as fh:
            manifest = json.load(fh)
    except Exception as exc:
        return [f'检查点清单无法解析：{exc}']
    stored = manifest.pop('manifest_checksum', None)
    if not stored or stored != checksum(manifest):
        problems.append('检查点清单指纹不符')
    if manifest.get('checkpoint_id') != cp.id:
        problems.append('检查点清单与记录编号不一致')
    payload_meta = manifest.get('payload') or {}
    if payload_meta.get('checksum') != cp.data_checksum:
        problems.append('检查点数据指纹与记录不一致')
    data_file = _safe_join(root, payload_meta.get('file') or '')
    payload = None
    if not os.path.isfile(data_file):
        problems.append('检查点数据文件缺失')
    else:
        if _sha256_file(data_file) != payload_meta.get('checksum'):
            problems.append('检查点数据文件指纹不符')
        else:
            try:
                with open(data_file, 'rb') as fh:
                    payload = json.loads(fh.read().decode('utf-8'))
            except Exception as exc:
                problems.append(f'检查点数据文件无法解析：{exc}')
    entries = manifest.get('artifacts')
    if not isinstance(entries, list):
        problems.append('检查点归档文件清单缺失')
        entries = []
    seen = set()
    for entry in entries:
        rel = entry.get('path') if isinstance(entry, dict) else None
        if not rel or rel in seen:
            problems.append('检查点归档文件清单存在重复或空路径')
            continue
        seen.add(rel)
        try:
            full = _safe_join(root, rel)
        except RestoreError as exc:
            problems.append(str(exc))
            continue
        if not os.path.isfile(full):
            problems.append(f'缺少归档文件：{rel}')
            continue
        if os.path.getsize(full) != entry.get('size'):
            problems.append(f'归档文件大小不符：{rel}')
        elif _sha256_file(full) != entry.get('sha256'):
            problems.append(f'归档文件指纹不符：{rel}')
    if payload is not None:
        problems.extend(_payload_problems(payload))
    return problems


def _payload_problems(payload):
    problems = []
    try:
        dataset_ids = {row['id'] for row in payload.get('datasets', [])}
        job_ids = {row['id'] for row in payload.get('jobs', [])}
        report_ids = {row['id'] for row in payload.get('reports', [])}
    except (KeyError, TypeError):
        return ['检查点数据记录结构不完整']
    for job in payload.get('jobs', []):
        if job.get('dataset_id') not in dataset_ids:
            problems.append(f'分析任务 {job.get("id")} 引用的数据批次缺失')
    for report in payload.get('reports', []):
        if report.get('job_id') not in job_ids:
            problems.append(f'分发计划 {report.get("id")} 引用的分析任务缺失')
    for delivery in payload.get('deliveries', []):
        if delivery.get('report_id') not in report_ids:
            problems.append(f'归档记录 {delivery.get("id")} 引用的分发计划缺失')
    return problems


def preflight(cp_id, actor):
    with Session() as db:
        cp = db.get(Checkpoint, cp_id)
        if not cp:
            raise HTTPException(404, '检查点不存在')
        problems = verify_checkpoint(cp)
        detail = {'problems': problems, 'artifact_count': len(cp.artifacts or []),
                  'counts': cp.counts or {}}
        cp.preflight_at = time.time()
        cp.preflight_ok = not problems
        cp.preflight_actor = actor
        cp.preflight_detail = detail
        record(db, actor, 'checkpoint.preflight', {'id': cp_id, 'ok': not problems,
                                                  'problems': problems[:20]})
        db.commit()
        result = _cp_dict(cp)
    return {'ok': not problems, **result, 'problems': problems}


# --------------------------------------------------------------------------- 恢复

def request_restore(cp_id, actor):
    with Session() as db:
        cp = db.get(Checkpoint, cp_id)
        if not cp:
            raise HTTPException(404, '检查点不存在')
        if cp.status != 'ready':
            raise HTTPException(409, '检查点尚未就绪，不能恢复')
        # 恢复前再核对一次：缺文件或指纹不符一律拒绝，避免拿残缺快照回滚。
        problems = verify_checkpoint(cp)
        cp.preflight_at = time.time()
        cp.preflight_ok = not problems
        cp.preflight_actor = actor
        cp.preflight_detail = {'problems': problems, 'artifact_count': len(cp.artifacts or []),
                               'counts': cp.counts or {}}
        db.commit()
        if problems:
            detail = '；'.join(problems[:3])
            raise HTTPException(409, f'检查点校验失败，拒绝恢复：{detail}')

        st = _locked_state(db)
        if st.mode in (MODE_QUIESCING, MODE_CAPTURING):
            raise HTTPException(409, _mode_reject_message(st.mode))
        existing = db.scalar(select(RestoreOperation).where(RestoreOperation.checkpoint_id == cp_id)
                             .order_by(RestoreOperation.id.desc()))
        if existing and existing.status == 'completed':
            return _op_dict(existing), False
        if existing and existing.status in ('pending', 'running', 'finalizing'):
            if st.mode != MODE_RESTORING or st.restore_id != existing.id:
                if st.mode == MODE_RESTORING and st.restore_id:
                    raise HTTPException(409, '已有另一项恢复操作进行中，请等待完成')
                st.mode = MODE_RESTORING
                st.restore_id = existing.id
                st.checkpoint_id = cp_id
                st.message = '整站恢复：继续未完成的恢复操作'
                st.updated_at = time.time()
            db.commit()
            op_dict = _op_dict(existing)
            op_id = existing.id
            _launch_restore(op_id)
            return op_dict, True
        if st.mode == MODE_RESTORING and st.restore_id and st.restore_id != (existing.id if existing else None):
            raise HTTPException(409, '已有另一项恢复操作进行中，请等待完成')

        now = time.time()
        if existing and existing.status == 'failed':
            op = existing
            previous = op.error
            op.status = 'pending'
            op.step = 'queued'
            op.phase = ''
            op.progress = 0
            op.error = ''
            op.detail = {'previous_error': previous}
            op.attempts = (op.attempts or 0) + 1
            op.updated_at = now
            op.finished_at = None
        else:
            op = RestoreOperation(checkpoint_id=cp_id, status='pending', step='queued', phase='', progress=0,
                                  error='', detail={}, actor=actor, attempts=1, epoch=0,
                                  created_at=now, updated_at=now)
            db.add(op)
            db.flush()
        epoch = int(st.epoch or 0) + 1
        op.epoch = epoch
        st.mode = MODE_RESTORING
        st.epoch = epoch
        st.restore_id = op.id
        st.checkpoint_id = cp_id
        st.message = '整站恢复：业务读写与派发已暂停'
        st.updated_at = now
        record(db, actor, 'checkpoint.restore.requested', {'id': cp_id, 'operation': op.id, 'epoch': epoch,
                                                          'attempt': op.attempts})
        db.commit()
        op_dict = _op_dict(op)
    _clear_queue()
    _launch_restore(op_dict['id'])
    return op_dict, True


def _launch_restore(op_id):
    with _process_lock:
        if op_id in _active_ops:
            return
        _active_ops.add(op_id)
    _clear_queue()
    threading.Thread(target=_restore_worker, args=(op_id,), daemon=True).start()


def _update_op(op_id, **fields):
    with Session() as db:
        op = db.get(RestoreOperation, op_id)
        if not op:
            return
        for key, value in fields.items():
            setattr(op, key, value)
        op.updated_at = time.time()
        db.commit()


def _load_payload(cp):
    with open(_safe_join(cp.directory, 'data.json'), 'rb') as fh:
        payload = json.loads(fh.read().decode('utf-8'))
    return payload


def _staging_dir(op_id):
    return os.path.join(ARTIFACT_DIR, f'.restore-staging-{op_id}')


def _backup_dir(op_id):
    return os.path.join(ARTIFACT_DIR, f'.restore-backup-{op_id}')


def _clean_restore_dirs(op_id):
    shutil.rmtree(_staging_dir(op_id), ignore_errors=True)
    shutil.rmtree(_backup_dir(op_id), ignore_errors=True)


def _snapshot_artifacts_to_staging(cp, op_id):
    """把清单中的归档文件完整复制到暂存目录并逐一核对指纹。

    先完成全部复制与校验、再做切换，保证不会出现“记录来自检查点、文件来自当前”的混合版本。
    """
    staging = _staging_dir(op_id)
    shutil.rmtree(staging, ignore_errors=True)
    os.makedirs(staging, exist_ok=True)
    with open(os.path.join(cp.directory, 'manifest.json'), 'r', encoding='utf-8') as fh:
        manifest = json.load(fh)
    entries = manifest.get('artifacts') or []
    for index, entry in enumerate(entries):
        src = _safe_join(cp.directory, entry['path'])
        dst = _safe_join(staging, entry['path'])
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(src, dst)
        if _sha256_file(dst) != entry.get('sha256'):
            raise RestoreError(f'归档文件暂存后指纹不符：{entry["path"]}')
        if entries:
            _update_op(op_id, progress=25 + int(18 * (index + 1) / len(entries)))
    return manifest


def _move_tree_files(src_root, dest_root, rels):
    for rel in rels:
        src = _safe_join(src_root, rel)
        dst = _safe_join(dest_root, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if os.path.exists(dst):
            os.remove(dst)
        shutil.move(src, dst)


def _backup_complete_marker(op_id):
    return os.path.join(_backup_dir(op_id), '.complete')


def _backup_current_artifacts(op_id, exclude=()):
    """复制（而非移动）当前归档到备份；完整写完标记后才允许切换。"""
    backup = _backup_dir(op_id)
    shutil.rmtree(backup, ignore_errors=True)
    os.makedirs(backup, exist_ok=True)
    excluded = tuple(os.path.abspath(path) + os.sep for path in exclude)
    rels = [rel for rel in _artifact_paths(ARTIFACT_DIR)
            if not os.path.abspath(_safe_join(ARTIFACT_DIR, rel)).startswith(excluded)]
    for rel in rels:
        src = _safe_join(ARTIFACT_DIR, rel)
        dst = _safe_join(backup, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(src, dst)
    with open(_backup_complete_marker(op_id), 'w', encoding='utf-8') as fh:
        fh.write(str(len(rels)))


def _swap_in_artifacts(op_id):
    """一次性把归档切到暂存版本；中途失败由备份整体回滚。"""
    staging = _staging_dir(op_id)
    if not os.path.isfile(_backup_complete_marker(op_id)):
        raise RestoreError('归档备份未完成，拒绝切换')
    staging_root = os.path.abspath(staging) + os.sep
    for rel in _artifact_paths(ARTIFACT_DIR):
        if os.path.abspath(_safe_join(ARTIFACT_DIR, rel)).startswith(staging_root):
            continue
        os.remove(_safe_join(ARTIFACT_DIR, rel))
    _move_tree_files(staging, ARTIFACT_DIR, _artifact_paths(staging))


def _rollback_artifacts(op_id):
    """把备份文件整体复制回归档目录，保留切换前的旧版本。"""
    backup = _backup_dir(op_id)
    if not os.path.isfile(_backup_complete_marker(op_id)):
        _clean_restore_dirs(op_id)
        return
    for rel in _artifact_paths(ARTIFACT_DIR):
        if rel.startswith(('.restore-staging-', '.restore-backup-')):
            continue
        try:
            os.remove(_safe_join(ARTIFACT_DIR, rel))
        except OSError:
            log.exception('Unable to remove partially switched artifact %s', rel)
    back = _artifact_paths(backup)
    for rel in back:
        dst = _safe_join(ARTIFACT_DIR, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(_safe_join(backup, rel), dst)
    _clean_restore_dirs(op_id)


def _recover_interrupted_switch(op_id):
    """中断后恢复同一操作：已提交记录则沿用新版，否则回到旧版再重试。"""
    with Session() as db:
        op = db.get(RestoreOperation, op_id)
        phase = op.phase if op else ''
    if phase in ('records_committed', 'finalizing'):
        _clean_restore_dirs(op_id)
        return True
    if op and op.status == 'failed':
        return False
    if os.path.isfile(_backup_complete_marker(op_id)):
        _rollback_artifacts(op_id)
    else:
        _clean_restore_dirs(op_id)
    return False


def _replace_records(payload, epoch, op_id):
    """记录切换与“已提交”标记在同一事务内完成，重试不会重复出刊。"""
    with Session() as db:
        db.execute(delete(Delivery))
        db.execute(delete(Report))
        db.execute(delete(Job))
        db.execute(delete(Dataset))
        db.flush()
        for row in payload.get('datasets', []):
            db.add(Dataset(**{k: row.get(k) for k in DATASET_FIELDS}))
        db.flush()
        for row in payload.get('jobs', []):
            values = {k: row.get(k) for k in JOB_FIELDS}
            status = values.get('status')
            if status in ('queued', 'running'):
                values['status'] = 'paused'
                message = '检查点恢复时该分析尚未完成，已暂停；请重新发起分析。'
                old = values.get('error') or ''
                values['error'] = (old + ' ' + message).strip()[:990]
            # 用新一轮栅栏标记全部恢复出的任务，旧执行无法再写回结果。
            values['lease_epoch'] = epoch
            db.add(Job(**values))
        db.flush()
        for row in payload.get('reports', []):
            values = {k: row.get(k) for k in REPORT_FIELDS}
            # 恢复出的计划先暂停，等待负责人逐条确认。
            values['enabled'] = False
            values['restore_paused'] = True
            db.add(Report(**values))
        db.flush()
        for row in payload.get('deliveries', []):
            db.add(Delivery(**{k: row.get(k) for k in DELIVERY_FIELDS}))
        op = db.get(RestoreOperation, op_id)
        if op:
            op.phase = 'records_committed'
            op.step = 'switched-records'
            op.progress = 85
            op.updated_at = time.time()
        db.commit()


def _finalize(op_id, cp):
    """完成收尾：暂停恢复出的计划、解除恢复态并标记操作完成，全部一次提交。"""
    _update_op(op_id, status='finalizing', step='finalizing', phase='finalizing', progress=90)
    with Session() as db:
        # 恢复出的计划一律暂停；负责人确认后再逐条启用。
        for report in db.scalars(select(Report)):
            report.enabled = False
            report.restore_paused = True
        st = db.get(SystemState, 1)
        if st and st.mode == MODE_RESTORING and st.restore_id == op_id:
            st.mode = MODE_NORMAL
            st.checkpoint_id = None
            st.restore_id = None
            st.message = ''
            st.updated_at = time.time()
        op = db.get(RestoreOperation, op_id)
        if op:
            op.status = 'completed'
            op.step = 'completed'
            op.phase = 'completed'
            op.progress = 100
            op.error = ''
            op.finished_at = time.time()
            op.updated_at = time.time()
        record(db, (op.actor if op else 'system') or 'system', 'checkpoint.restored',
               {'checkpoint': cp.id, 'operation': op_id, 'counts': cp.counts or {},
                'epoch': (op.epoch if op else None)})
        db.commit()
    _clean_restore_dirs(op_id)


def _claim_operation(op_id):
    """只允许一个执行者领取恢复操作；已在别处运行的操作不会被重复执行。"""
    with Session() as db:
        op = db.get(RestoreOperation, op_id, with_for_update=True)
        if not op or op.status != 'pending':
            db.rollback()
            return False
        op.status = 'running'
        op.step = 'verifying'
        op.phase = 'claimed'
        op.progress = 5
        op.updated_at = time.time()
        db.commit()
        return True


def _restore_worker(op_id):
    acquired = False
    try:
        for _ in range(40):
            acquired = _process_lock.acquire(blocking=False)
            if acquired:
                break
            time.sleep(0.25)
        if not acquired:
            raise RestoreError('另一项检查点或恢复操作正在进行，请稍后重试')
        if not _claim_operation(op_id):
            return
        with Session() as db:
            op = db.get(RestoreOperation, op_id)
            if not op:
                return
            cp = db.get(Checkpoint, op.checkpoint_id)
            epoch = op.epoch
        problems = verify_checkpoint(cp)
        if problems:
            raise RestoreError('检查点校验失败：' + '；'.join(problems[:5]))

        # 中断后重试同一操作：已提交记录则直接收尾，绝不重复出刊或重复切换。
        if op and op.phase in ('records_committed', 'finalizing'):
            _clean_restore_dirs(op_id)
            _finalize(op_id, cp)
            return
        if _recover_interrupted_switch(op_id):
            _finalize(op_id, cp)
            return

        if not _wait_drain(DRAIN_TIMEOUT):
            raise RestoreError('在途业务操作未能在限定时间内收口，恢复已取消并保留旧版本')

        _update_op(op_id, step='staging', progress=20)
        _snapshot_artifacts_to_staging(cp, op_id)
        _update_op(op_id, step='staged', phase='staged', progress=40)

        _update_op(op_id, step='backing-up', progress=45)
        _backup_current_artifacts(op_id, exclude=(_staging_dir(op_id), _backup_dir(op_id)))
        _update_op(op_id, step='switching-files', phase='files_backed_up', progress=55)
        _swap_in_artifacts(op_id)
        _update_op(op_id, step='switched-files', phase='files_swapped', progress=65)

        payload = _load_payload(cp)
        if checksum(payload) != cp.data_checksum:
            raise RestoreError('检查点数据记录指纹不符')
        _update_op(op_id, step='switching-records', phase='switching_records', progress=75)
        _replace_records(payload, epoch, op_id)
        _finalize(op_id, cp)
    except Exception as exc:
        log.exception('Checkpoint restore failed')
        with Session() as db:
            op = db.get(RestoreOperation, op_id)
            phase = op.phase if op else ''
        if phase in ('records_committed', 'finalizing'):
            # 新版已完整提交，保留它；重试只会完成收尾。
            _clean_restore_dirs(op_id)
        else:
            try:
                _rollback_artifacts(op_id)
            except Exception:
                log.exception('Unable to roll back artifacts')
        _update_op(op_id, status='failed', step='failed', error=str(exc)[:990], finished_at=time.time())
        with Session() as db:
            st = db.get(SystemState, 1)
            if st and st.mode == MODE_RESTORING and st.restore_id == op_id:
                st.mode = MODE_NORMAL
                st.restore_id = None
                st.checkpoint_id = None
                st.message = '整站恢复失败，已保留旧版本' if phase not in ('records_committed', 'finalizing') else '整站恢复已提交，收尾失败，可重试'
                st.updated_at = time.time()
                db.commit()
            record(db, 'system', 'checkpoint.restore.failed', {'operation': op_id, 'error': str(exc)[:500]})
            db.commit()
    finally:
        if acquired:
            _process_lock.release()
        with _process_lock:
            _active_ops.discard(op_id)


# --------------------------------------------------------------------------- 启动恢复

def recover_after_startup():
    """进程重启后收口中断的取点，并继续未完成的同一恢复操作。"""
    with Session() as db:
        # 进程重启后不可能还有在途租约；残留行只会挡住取点与恢复。
        db.execute(delete(InflightOp))
        st = _ensure_state(db)
        if st.mode in (MODE_QUIESCING, MODE_CAPTURING):
            st.mode = MODE_NORMAL
            st.checkpoint_id = None
            st.message = ''
            st.updated_at = time.time()
            for cp in db.scalars(select(Checkpoint).where(Checkpoint.status == 'creating')):
                cp.status = 'failed'
                cp.step = 'failed'
                cp.error = '服务重启中断了取点，请重新发起'
            db.commit()
        elif st.mode == MODE_RESTORING:
            op = db.get(RestoreOperation, st.restore_id) if st.restore_id else None
            if not op or op.status == 'completed':
                st.mode = MODE_NORMAL
                st.restore_id = None
                st.message = ''
                db.commit()
            elif op.status == 'failed':
                st.mode = MODE_NORMAL
                st.restore_id = None
                st.message = '整站恢复失败，已保留旧版本'
                db.commit()
            else:
                if op.status in ('running', 'finalizing'):
                    # 本进程刚启动，没有本地恢复线程；把中断的操作重置为待执行，
                    # 继续同一操作号，已提交的切换不会重复。
                    op.status = 'pending'
                    op.step = 'queued'
                    op.phase = op.phase if op.phase in ('records_committed', 'finalizing') else op.phase
                    op.updated_at = time.time()
                    db.commit()
                _launch_restore(op.id)


def resume_pending_restores():
    with Session() as db:
        st = _ensure_state(db)
        if st.mode == MODE_RESTORING and st.restore_id:
            op = db.get(RestoreOperation, st.restore_id)
            if op and op.status in ('pending', 'running', 'finalizing'):
                if op.status in ('running', 'finalizing'):
                    op.status = 'pending'
                    op.updated_at = time.time()
                    db.commit()
                _launch_restore(op.id)
