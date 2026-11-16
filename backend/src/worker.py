import json
import logging
import time
from pathlib import Path

from redis import Redis
from sqlalchemy import select

from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models import Dataset, Job, Report
from src.services.audit import record
from src.services.business import analyze
from src.services.checkpoints import (BusyError, MODE_NORMAL, MODE_RESTORING, get_epoch, inflight,
                                      recover_after_startup, recover_running_jobs, message_epoch,
                                      resume_pending_restores)
from src.services.reports import deliver

logging.basicConfig(level=logging.INFO)


def _requeue(job_id, reason, detail=None):
    with Session() as db:
        current, mode = get_epoch(db)
        job = db.get(Job, job_id)
        if not job or (job.lease_epoch or 0) != current:
            return
        job.status = 'queued'
        job.lease_epoch = current
        record(db, 'worker', reason, {'id': job_id, **(detail or {})})
        db.commit()


def execute(job_id, message_epoch_value):
    """执行分析。消息自带入队时的整站栅栏，迟到或恢复前的旧消息由此作废。"""
    with Session() as db:
        current, mode = get_epoch(db)
        job = db.get(Job, job_id)
        if mode != MODE_NORMAL or not job or job.status != 'queued':
            return
        if message_epoch_value is not None and message_epoch_value != current:
            # 恢复后旧队列消息不能复活已回退的任务。
            record(db, 'worker', 'job.stale_message', {'id': job_id, 'epoch': message_epoch_value})
            db.commit()
            return

    try:
        lease = inflight('analysis', f'job:{job_id}')
        lease.__enter__()
    except BusyError:
        # 取点/恢复已关闭写入，任务保持排队，服务恢复后再执行。
        return

    try:
        with Session() as db:
            current, mode = get_epoch(db)
            job = db.get(Job, job_id)
            if mode != MODE_NORMAL or not job or job.status != 'queued' or (job.lease_epoch or 0) != current:
                return
            job.status = 'running'
            job.lease_epoch = current
            db.commit()
        with Session() as db:
            job = db.get(Job, job_id)
            dataset = db.get(Dataset, job.dataset_id)
            result = analyze(dataset.records, job.params)
            json.dumps(result, allow_nan=False)
            current, mode = get_epoch(db)
            db.refresh(job)
            if (job.lease_epoch or 0) != current or mode == MODE_RESTORING or job.status != 'running':
                # 恢复推进了栅栏，本次执行的成果拒收，任务退回排队。
                db.rollback()
                _requeue(job_id, 'job.fenced', {'epoch': current})
                return
            job.result = result
            job.status = 'completed'
            job.finished_at = time.time()
            record(db, 'worker', 'job.completed', {'id': job.id, 'epoch': current})
            db.commit()
    except Exception as exc:
        logging.exception('Analysis failed')
        with Session() as db:
            current, mode = get_epoch(db)
            job = db.get(Job, job_id)
            if job and (job.lease_epoch or 0) == current and mode != MODE_RESTORING:
                job.status = 'failed'
                job.error = str(exc)[:990]
                job.finished_at = time.time()
                record(db, 'worker', 'job.failed', {'id': job.id, 'error': job.error})
                db.commit()
    finally:
        lease.__exit__(None, None, None)


def tick():
    epoch, mode = get_epoch()
    if mode != MODE_NORMAL:
        return
    with Session() as db:
        queued = list(db.scalars(select(Job.id).where(Job.status == 'queued', Job.lease_epoch == epoch).limit(3)))
    for job_id in queued:
        try:
            execute(job_id, epoch)
        except Exception:
            logging.exception('Job execution retry')
            time.sleep(1)
    epoch, mode = get_epoch()
    if mode != MODE_NORMAL:
        return
    with Session() as db:
        due = list(db.scalars(select(Report.id).where(
            Report.enabled == True,  # noqa: E712
            Report.restore_paused == False,  # noqa: E712
            Report.next_run <= time.time())))
    for report_id in due:
        deliver_due(report_id, epoch)


def deliver_due(report_id, epoch):
    try:
        with inflight('delivery', f'report:{report_id}'):
            with Session() as db:
                current, mode = get_epoch(db)
                if mode != MODE_NORMAL:
                    return
                report = db.get(Report, report_id)
                if not report or not report.enabled or report.restore_paused or report.next_run > time.time():
                    return
                delivery = deliver(db, report)
                if delivery:
                    record(db, 'worker', 'report.delivered',
                           {'id': report.id, 'delivery': delivery.id, 'epoch': current})
                db.commit()
    except BusyError:
        return
    except Exception:
        logging.exception('Delivery failed for report %s', report_id)


def main():
    recover_after_startup()
    r = Redis.from_url(REDIS_URL, socket_timeout=3)
    recover_running_jobs()
    resume_pending_restores()
    while True:
        Path('/tmp/heartbeat').touch()
        epoch, mode = get_epoch()
        if mode == MODE_RESTORING:
            # 恢复期间不取消息、不派发；旧消息留在队列里，靠栅栏拒收。
            time.sleep(0.5)
            continue
        try:
            item = r.blpop('analysis:queue', timeout=1)
        except Exception:
            logging.exception('Worker queue read failed')
            time.sleep(2)
            continue
        try:
            if item:
                job_id, msg_epoch = message_epoch(item[1])
                execute(job_id, msg_epoch)
            tick()
        except Exception:
            logging.exception('Worker retry')
            time.sleep(2)


if __name__ == '__main__':
    main()
