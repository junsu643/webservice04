import { AppError, missing, forbidden, invalidState } from './errors.js';

// 트랜잭션 경계: BEGIN IMMEDIATE로 쓰기 잠금을 먼저 얻고, 그 안에서 "최신 상태"를 다시 읽어
// 검사·저장한다. 콜백이 예외 없이 끝나면 COMMIT, 어떤 오류든 ROLLBACK 후 다시 던진다.
// 서버가 2개여도 같은 SQLite 파일의 쓰기 잠금으로 직렬화되므로(busy_timeout=5000)
// 정원 검사와 승인 저장 사이에 다른 요청이 끼어들 수 없다.
function runInTransaction(db, work) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const result = work();
    db.exec('COMMIT');
    return result;
  } catch (e) {
    try { db.exec('ROLLBACK'); } catch { /* 이미 롤백된 경우 */ }
    throw e;
  }
}

const isUniqueViolation = e => /UNIQUE constraint failed/i.test(String(e?.message));
const alreadyApplied = () => new AppError(409, 'ALREADY_APPLIED', '이미 신청한 행사입니다.');
const capacityFull = () => new AppError(409, 'CAPACITY_FULL', '승인 정원이 가득 찼습니다.');

export function createApplicationService(repo) {
  const db = repo.db;

  // 대기 순서 고정: 같은 행사 PENDING 중 created_at ASC, id ASC 첫 1건.
  const nextPending = eventId => db.prepare(
    "SELECT id FROM applications WHERE event_id=? AND status='PENDING' ORDER BY created_at ASC, id ASC LIMIT 1"
  ).get(eventId);

  // 대상 신청과 소속 행사를 (트랜잭션 안에서) 읽고, 담당 운영자인지 확인한다.
  function loadForManager(actor, applicationId) {
    const app = repo.application(applicationId);
    if (!app) throw missing('신청');
    const event = repo.event(app.eventId);
    if (actor.role !== 'MANAGER' || event.managerId !== actor.id) throw forbidden();
    return { app, event };
  }

  return {
    apply(actor, eventId) {
      try {
        return runInTransaction(db, () => {
          const event = repo.event(eventId);
          if (!event) throw missing('행사');
          if (actor.role !== 'STUDENT') throw forbidden();
          if (repo.existing(eventId, actor.id)) throw alreadyApplied(); // CANCELLED·REVOKED 포함
          const created = repo.insert(eventId, actor.id);
          repo.addHistory({
            applicationId: created.id, actorId: actor.id, action: 'APPLY',
            fromStatus: null, toStatus: 'PENDING'
          });
          return created;
        });
      } catch (e) {
        if (isUniqueViolation(e)) throw alreadyApplied(); // 최종 중복 방어
        throw e;
      }
    },

    cancel(actor, applicationId) {
      return runInTransaction(db, () => {
        const app = repo.application(applicationId);
        if (!app) throw missing('신청');
        if (actor.role !== 'STUDENT' || app.userId !== actor.id) throw forbidden();
        if (app.status !== 'PENDING') throw invalidState();
        const updated = repo.setStatus(app.id, 'CANCELLED');
        repo.addHistory({
          applicationId: app.id, actorId: actor.id, action: 'CANCEL',
          fromStatus: 'PENDING', toStatus: 'CANCELLED'
        });
        return updated; // 대기 취소는 자리를 비우지 않으므로 자동 승급 없음
      });
    },

    approve(actor, applicationId) {
      return runInTransaction(db, () => {
        const { app, event } = loadForManager(actor, applicationId);
        if (app.status !== 'PENDING') throw invalidState();
        if (repo.approvedCount(event.id) >= event.capacity) throw capacityFull();
        const updated = repo.setStatus(app.id, 'APPROVED');
        repo.addHistory({
          applicationId: app.id, actorId: actor.id, action: 'APPROVE',
          fromStatus: 'PENDING', toStatus: 'APPROVED'
        });
        return updated;
      });
    },

    revoke(actor, applicationId, reason) {
      return runInTransaction(db, () => {
        // 1. 최신 대상·권한·상태 확인
        const { app } = loadForManager(actor, applicationId);
        if (app.status !== 'APPROVED') throw invalidState();

        // 2. REVOKED 저장 + REVOKE 이력
        const revoked = repo.setStatus(app.id, 'REVOKED');
        const revokeHistoryId = repo.addHistory({
          applicationId: app.id, actorId: actor.id, action: 'REVOKE',
          fromStatus: 'APPROVED', toStatus: 'REVOKED', reason
        });

        // 3~4. 같은 행사 대기자 1명 선택 → APPROVED + AUTO_APPROVE 이력(원인 ID 연결)
        let promoted = null;
        const next = nextPending(app.eventId);
        if (next) {
          promoted = repo.setStatus(next.id, 'APPROVED');
          repo.addHistory({
            applicationId: next.id, actorId: actor.id, action: 'AUTO_APPROVE',
            fromStatus: 'PENDING', toStatus: 'APPROVED', causeHistoryId: revokeHistoryId
          });
        }
        // 5. 여기까지 오류가 없으면 COMMIT, 어느 단계든 예외면 ROLLBACK
        return { revoked, promoted };
      });
    }
  };
}
