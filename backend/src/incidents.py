import logging

from redis.exceptions import RedisError
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session as DatabaseSession

from .classifier import snapshot_digest
from .database import WAKE_QUEUE, queue
from .models import Action, BerthRequest, DelayClass, DelayIncident, DispatchReview, IncidentState, RequestState
from .planning import Rejected, audit, find, require_window, tide_clearance, utc_now
from .schemas import CloseInput, DispatchInput, IncidentInput, MarginDecision, WithdrawInput

log = logging.getLogger(__name__)

WITHDRAWABLE = (
    IncidentState.QUEUED, IncidentState.FAILED, IncidentState.MARGIN_REVIEW, IncidentState.READY,
)


def notify(code):
    try:
        queue.lpush(WAKE_QUEUE, code)
    except RedisError:
        log.exception('Redis wake-up failed for %s; worker scans durable queue', code)


def locked(db: DatabaseSession, code: str):
    """Read the current committed row under a lock so concurrent writers serialize on state."""
    incident = db.scalar(
        select(DelayIncident).where(DelayIncident.code == code)
        .with_for_update().execution_options(populate_existing=True)
    )
    if incident is None:
        raise Rejected('NOT_FOUND', f'找不到记录 {code}', code, 404)
    return incident


def same_submission(incident: DelayIncident, data: IncidentInput):
    features = incident.features or {}
    return (
        incident.request_code == data.request_code
        and incident.radio_reference == data.radio_reference
        and incident.observed_at == data.observed_at
        and incident.event_note == data.event_note
        and features.get('wind_kn') == data.wind_kn
        and features.get('tug_wait_min') == data.tug_wait_min
    )


def report(db: DatabaseSession, actor: str, data: IncidentInput):
    request = find(db, BerthRequest, data.request_code, lock=True)
    previous = db.scalar(select(DelayIncident).where(or_(
        DelayIncident.code == data.code,
        DelayIncident.request_code == request.code,
        DelayIncident.radio_reference == data.radio_reference,
    )).limit(1))
    if previous:
        # A replayed submission returns the durable record without enqueueing again. A different
        # payload reusing an identifier is a genuine conflict.
        if same_submission(previous, data):
            return previous, False
        raise Rejected('DUPLICATE_INCIDENT', f'关联申请、案件编号或无线电记录已登记在 {previous.code}', data.code)
    if request.state != RequestState.CONFIRMED:
        raise Rejected('CONFIRMATION_REQUIRED', '请先核验潮窗并确认靠泊', request.code)
    offset = (data.observed_at - request.eta).total_seconds() / 60
    if not 0 < offset <= 360:
        raise Rejected('OBSERVATION_TIME', '延误观测必须晚于计划引航时刻，且在六小时内', data.code)
    window = require_window(db, request.window_code, request.berth_code, request.code)
    reserve, _ = tide_clearance(request, window)
    features = {
        'eta_offset_min': round(offset, 3), 'tide_reserve_min': round(reserve, 3),
        'wind_kn': data.wind_kn, 'tug_wait_min': data.tug_wait_min,
    }
    incident = DelayIncident(code=data.code, request_code=request.code, radio_reference=data.radio_reference,
                             observed_at=data.observed_at, event_note=data.event_note, features=features,
                             input_digest=snapshot_digest(features), state=IncidentState.QUEUED)
    try:
        with db.begin_nested():
            db.add(incident)
            db.flush()
    except IntegrityError:
        # A concurrent duplicate landed first. Keep its record and skip a second enqueue.
        previous = db.scalar(select(DelayIncident).where(or_(
            DelayIncident.code == data.code,
            DelayIncident.request_code == request.code,
            DelayIncident.radio_reference == data.radio_reference,
        )).limit(1))
        if previous and same_submission(previous, data):
            return previous, False
        raise Rejected('DUPLICATE_INCIDENT', f'关联申请、案件编号或无线电记录已登记在 {previous.code if previous else data.code}',
                       data.code) from None
    audit(db, actor, incident.code, 'INCIDENT_QUEUED', f'{request.code} · {incident.radio_reference}')
    return incident, True


def retry(db: DatabaseSession, actor: str, code: str):
    incident = locked(db, code)
    if incident.state == IncidentState.QUEUED and incident.attempts:
        # A repeated retry of an already requeued failure must not enqueue or classify twice.
        return incident, False
    if incident.state != IncidentState.FAILED:
        raise Rejected('RETRY_STATE', '只有计算失败的案件可以重试', code)
    incident.state = IncidentState.QUEUED
    incident.error = None
    audit(db, actor, code, 'INCIDENT_REQUEUED', f'复用输入摘要 {incident.input_digest}')
    return incident, True


def withdraw(db: DatabaseSession, actor: str, code: str, data: WithdrawInput):
    incident = locked(db, code)
    reason = data.reason.strip()
    if incident.state == IncidentState.WITHDRAWN:
        # Already withdrawn: keep the original record and reason untouched.
        return incident, False
    if incident.state in (IncidentState.CLOSED,):
        raise Rejected('WITHDRAW_CLOSED', '案件已结案，不能撤销', code)
    if incident.state == IncidentState.WITHDRAW_REVIEW:
        if incident.withdraw_reason != reason:
            incident.withdraw_reason = reason
            audit(db, actor, code, 'WITHDRAW_REVIEW_UPDATED', reason)
        return incident, False
    if incident.state == IncidentState.ACTIONED:
        # An issued instruction belongs to the watch and is preserved for the supervisor check.
        incident.withdraw_reason = reason
        incident.withdraw_requested_by = actor
        incident.withdraw_requested_at = utc_now()
        incident.state = IncidentState.WITHDRAW_REVIEW
        audit(db, actor, code, 'WITHDRAW_REVIEW_REQUESTED', f'保留原指令 · 撤销理由：{reason}')
        return incident, False
    if incident.state not in WITHDRAWABLE:
        raise Rejected('WITHDRAW_STATE', '当前案件状态不能撤销', code)
    now = utc_now()
    incident.withdraw_reason = reason
    incident.withdraw_requested_by = actor
    incident.withdraw_requested_at = now
    incident.withdrawn_by = actor
    incident.withdrawn_at = now
    incident.state = IncidentState.WITHDRAWN
    kept = ' · 保留模型结果' if incident.result else ''
    audit(db, actor, code, 'INCIDENT_WITHDRAWN', f'撤销理由：{reason}{kept}')
    return incident, False


def confirm_withdraw(db: DatabaseSession, actor: str, code: str, data: WithdrawInput):
    incident = locked(db, code)
    if incident.state == IncidentState.WITHDRAWN:
        return incident, False
    if incident.state != IncidentState.WITHDRAW_REVIEW:
        raise Rejected('WITHDRAW_REVIEW_STATE', '只有等待主管核查的撤销案件可以确认', code)
    note = data.reason.strip()
    incident.withdrawn_by = actor
    incident.withdrawn_at = utc_now()
    incident.state = IncidentState.WITHDRAWN
    kept = ' · 保留模型结果及原指令' if incident.result else ' · 保留原指令'
    audit(db, actor, code, 'WITHDRAW_REVIEWED',
          f'核查说明：{note} · 撤销理由：{incident.withdraw_reason}{kept}')
    return incident, False


def review_margin(db: DatabaseSession, actor: str, code: str, data: MarginDecision):
    incident = locked(db, code)
    if incident.state != IncidentState.MARGIN_REVIEW:
        raise Rejected('NOT_IN_MARGIN_REVIEW', '案件当前未等待间隔带复核', code)
    incident.decision = data.decision
    incident.decided_by = actor
    incident.decision_reason = data.reason
    incident.decided_at = utc_now()
    incident.state = IncidentState.READY
    audit(db, actor, code, 'MARGIN_REVIEWED', f'{data.decision} · {data.reason}')
    return incident


def dispatch(db: DatabaseSession, actor: str, code: str, data: DispatchInput):
    incident = locked(db, code)
    if incident.state == IncidentState.MARGIN_REVIEW:
        raise Rejected('MARGIN_REVIEW_REQUIRED', '间隔带案件必须由主管签署人工分类', code)
    if incident.state != IncidentState.READY or not incident.decision:
        raise Rejected('DISPATCH_STATE', '处置仅适用于已完成分类且尚未发布动作的案件', code)
    if incident.decision == DelayClass.PRIORITY and data.action == Action.MONITOR:
        raise Rejected('PRIORITY_ACTION_REQUIRED', '优先处置须发布待命或改期指令', code)
    record = DispatchReview(incident_code=code, issued_by=actor, issued_at=utc_now(), **data.model_dump())
    incident.state = IncidentState.ACTIONED
    db.add(record)
    audit(db, actor, code, 'ACTION_ISSUED', f'{data.action} · {data.instruction}')
    return record


def close(db: DatabaseSession, actor: str, code: str, data: CloseInput):
    incident = locked(db, code)
    if incident.state != IncidentState.ACTIONED:
        raise Rejected('CLOSE_STATE', '请先发布调度动作再结案', code)
    record = find(db, DispatchReview, code)
    request = find(db, BerthRequest, incident.request_code, lock=True)
    if data.all_fast_at < incident.observed_at:
        raise Rejected('ALL_FAST_TIME', '全缆系妥时刻不得早于延误观测', code)
    record.all_fast_at = data.all_fast_at
    record.outcome = data.outcome
    record.closed_at = utc_now()
    record.closed_by = actor
    incident.state = IncidentState.CLOSED
    request.state = RequestState.COMPLETED
    audit(db, actor, code, 'REVIEW_CLOSED', f'{data.all_fast_at:%m-%d %H:%M} · {data.outcome}')
    return record
