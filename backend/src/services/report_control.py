from fastapi import HTTPException

from src.models import Report
from src.services.audit import record


def toggle(db, report_id, actor):
    report = db.get(Report, report_id)
    if not report:
        raise HTTPException(404, '分发计划不存在')
    enabled = not report.enabled
    if enabled:
        # 检查点恢复出的计划必须先由负责人确认再启用。
        report.restore_paused = False
    report.enabled = enabled
    record(db, actor, 'report.toggle', {'id': report_id, 'enabled': report.enabled})
    db.commit()
    return report


def acknowledge(db, report_id, actor):
    report = db.get(Report, report_id)
    if not report:
        raise HTTPException(404, '分发计划不存在')
    if not report.restore_paused:
        raise HTTPException(409, '该计划不是恢复出的暂停计划')
    report.restore_paused = False
    report.enabled = False
    record(db, actor, 'report.restore.acknowledged', {'id': report_id})
    db.commit()
    return report
