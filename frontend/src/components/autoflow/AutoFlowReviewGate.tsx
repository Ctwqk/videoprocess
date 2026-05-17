import { useState, type CSSProperties } from 'react';
import type { AutoFlowPlan, AutoFlowPublishMode } from '../../types/autoflow';

function boolFromRecord(record: Record<string, unknown>, keys: string[]) {
  for (const key of keys) {
    if (typeof record[key] === 'boolean') return record[key];
  }
  return null;
}

function statusFromRecord(record: Record<string, unknown>) {
  const status = record.decision ?? record.status ?? record.rights_status;
  return typeof status === 'string' ? status.toLowerCase() : '';
}

function planStatus(plan: AutoFlowPlan) {
  return (plan.status ?? '').toLowerCase();
}

function isRejected(plan: AutoFlowPlan) {
  return planStatus(plan) === 'rejected' || Boolean(plan.rejected_reason);
}

function isBlocked(plan: AutoFlowPlan) {
  const status = statusFromRecord(plan.rights);
  const explicitAllowed = boolFromRecord(plan.rights, ['allowed', 'execute_allowed', 'can_execute']);
  if (explicitAllowed === false) return true;
  return ['blocked', 'denied', 'rejected'].includes(status) || ['blocked', 'denied'].includes(planStatus(plan));
}

function isPublishBlocked(plan: AutoFlowPlan) {
  const publishAllowed = boolFromRecord(plan.rights, ['publish_allowed', 'can_publish']);
  if (publishAllowed === false) return true;
  const publishStatus = plan.rights.publish_status ?? plan.rights.publish_decision;
  return typeof publishStatus === 'string' && ['blocked', 'denied', 'rejected'].includes(publishStatus.toLowerCase());
}

function isReviewApproved(plan: AutoFlowPlan) {
  return !plan.needs_review || Boolean(plan.review_approved_at) || planStatus(plan) === 'approved';
}

function isPublicAfterReview(plan: AutoFlowPlan, publishMode?: AutoFlowPublishMode) {
  if (publishMode) return publishMode === 'public_after_review';
  return plan.request.publish_mode === 'public_after_review' || plan.intent.publish_mode === 'public_after_review';
}

function isPublicApproved(plan: AutoFlowPlan) {
  return Boolean(plan.public_approved_at) || planStatus(plan) === 'public_approved';
}

function gateMessage(plan: AutoFlowPlan, publishMode?: AutoFlowPublishMode) {
  if (isRejected(plan)) return plan.rejected_reason || 'Plan was rejected.';
  if (isBlocked(plan)) return 'Execution is blocked by rights policy.';
  if (plan.needs_review && !isReviewApproved(plan)) return 'Human review is required before execution.';
  if (isPublishBlocked(plan)) return 'Execution is allowed, but publishing is blocked.';
  if (isPublicAfterReview(plan, publishMode) && !isPublicApproved(plan)) return 'Public publishing requires explicit approval.';
  return 'Plan is ready to execute.';
}

function gateLabel(plan: AutoFlowPlan, publishMode?: AutoFlowPublishMode) {
  if (isRejected(plan)) return 'Rejected';
  if (isBlocked(plan)) return 'Blocked';
  if (plan.needs_review && !isReviewApproved(plan)) return 'Review required';
  if (isPublicAfterReview(plan, publishMode) && !isPublicApproved(plan)) return 'Public approval required';
  return 'Executable';
}

function gateColor(plan: AutoFlowPlan, publishMode?: AutoFlowPublishMode) {
  if (isRejected(plan) || isBlocked(plan)) return '#fca5a5';
  if (plan.needs_review && !isReviewApproved(plan)) return '#fde68a';
  if (isPublicAfterReview(plan, publishMode) && !isPublicApproved(plan)) return '#fde68a';
  return '#86efac';
}

export default function AutoFlowReviewGate({
  plan,
  approving,
  publicApproving,
  rejecting,
  saving,
  executing,
  hasUnsavedEdits,
  publishMode,
  onApprove,
  onApprovePublic,
  onReject,
  onExecute,
}: {
  plan: AutoFlowPlan | null;
  approving: boolean;
  publicApproving: boolean;
  rejecting: boolean;
  saving: boolean;
  executing: boolean;
  hasUnsavedEdits: boolean;
  publishMode?: AutoFlowPublishMode;
  onApprove: () => void;
  onApprovePublic: (reviewNotes: string) => void;
  onReject: (reason: string) => void;
  onExecute: () => void;
}) {
  const [reviewNotes, setReviewNotes] = useState('');
  const [rejectReason, setRejectReason] = useState('');

  if (!plan) {
    return (
      <section style={sectionStyle}>
        <h2 style={{ margin: '0 0 8px', fontSize: 14, color: '#f8fafc' }}>Review Gate</h2>
        <div style={{ fontSize: 13, color: '#94a3b8' }}>Generate a plan to review rights and execute controls.</div>
      </section>
    );
  }

  const blocked = isBlocked(plan);
  const rejected = isRejected(plan);
  const publishBlocked = isPublishBlocked(plan);
  const reviewApproved = isReviewApproved(plan);
  const publicAfterReview = isPublicAfterReview(plan, publishMode);
  const publicApproved = isPublicApproved(plan);
  const busy = approving || publicApproving || rejecting || saving || executing;
  const executeDisabled = rejected
    || blocked
    || publishBlocked
    || !reviewApproved
    || (publicAfterReview && !publicApproved)
    || busy;
  const canApproveReview = !rejected && !blocked && plan.needs_review && !reviewApproved && !busy;
  const canApprovePublic = !rejected && !blocked && !publishBlocked && reviewApproved && publicAfterReview && !publicApproved && !busy;
  const canReject = !rejected && !busy;
  const borderColor = rejected || blocked ? '#7f1d1d' : executeDisabled ? '#854d0e' : '#166534';

  return (
    <section style={{ ...sectionStyle, border: `1px solid ${borderColor}` }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, marginBottom: 10 }}>
        <h2 style={{ margin: 0, fontSize: 14, color: '#f8fafc' }}>Review Gate</h2>
        <div style={{ color: gateColor(plan, publishMode), fontSize: 12, fontWeight: 700 }}>
          {gateLabel(plan, publishMode)}
        </div>
      </div>

      <div style={{ fontSize: 13, color: '#cbd5e1', marginBottom: 12 }}>
        {gateMessage(plan, publishMode)}
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(125px, 1fr))', gap: 8, marginBottom: 12 }}>
        <StatusTile label="Execute" value={blocked || rejected ? 'Denied' : 'Allowed'} danger={blocked || rejected} />
        <StatusTile label="Review" value={reviewApproved ? 'Approved' : 'Needed'} warning={!reviewApproved} />
        <StatusTile
          label="Public"
          value={!publicAfterReview ? 'Not requested' : publicApproved ? 'Approved' : 'Approval needed'}
          warning={publicAfterReview && !publicApproved}
        />
      </div>

      {hasUnsavedEdits ? (
        <div style={{ color: '#fde68a', fontSize: 12, marginBottom: 10 }}>
          Unsaved edits will be saved before approval or execution.
        </div>
      ) : null}

      {Object.keys(plan.rights).length > 0 ? (
        <details style={{ marginBottom: 12 }}>
          <summary style={{ cursor: 'pointer', color: '#93c5fd', fontSize: 12 }}>Rights details</summary>
          <pre style={detailsStyle}>
            {JSON.stringify(plan.rights, null, 2)}
          </pre>
        </details>
      ) : null}

      <textarea
        value={reviewNotes}
        onChange={event => setReviewNotes(event.target.value)}
        placeholder="Review notes"
        rows={2}
        style={textareaStyle}
      />

      <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, flexWrap: 'wrap', marginTop: 10 }}>
        <button
          type="button"
          disabled={!canApproveReview}
          onClick={onApprove}
          style={buttonStyle(canApproveReview, '#0f766e')}
        >
          {approving ? 'Approving...' : reviewApproved ? 'Approved' : 'Approve'}
        </button>
        <button
          type="button"
          disabled={!canApprovePublic}
          onClick={() => onApprovePublic(reviewNotes)}
          style={buttonStyle(canApprovePublic, '#7c3aed')}
        >
          {publicApproving ? 'Approving...' : publicApproved ? 'Public approved' : 'Approve public'}
        </button>
        <button
          type="button"
          disabled={!canReject}
          onClick={() => onReject(rejectReason || reviewNotes)}
          style={buttonStyle(canReject, '#991b1b')}
        >
          {rejecting ? 'Rejecting...' : 'Reject'}
        </button>
        <button
          type="button"
          disabled={executeDisabled}
          onClick={onExecute}
          style={buttonStyle(!executeDisabled, '#2563eb', 112)}
        >
          {executing || saving ? 'Working...' : 'Execute'}
        </button>
      </div>

      <input
        value={rejectReason}
        onChange={event => setRejectReason(event.target.value)}
        placeholder="Reject reason"
        style={{ ...textareaStyle, minHeight: 'auto', marginTop: 10 }}
      />
    </section>
  );
}

function StatusTile({
  label,
  value,
  danger = false,
  warning = false,
}: {
  label: string;
  value: string;
  danger?: boolean;
  warning?: boolean;
}) {
  const color = danger ? '#fca5a5' : warning ? '#fde68a' : '#86efac';
  return (
    <div style={{ padding: 8, borderRadius: 6, backgroundColor: '#020617', border: '1px solid #1e293b' }}>
      <div style={{ fontSize: 11, color: '#64748b' }}>{label}</div>
      <div style={{ fontSize: 13, color, fontWeight: 700 }}>{value}</div>
    </div>
  );
}

const sectionStyle: CSSProperties = {
  backgroundColor: '#0f172a',
  border: '1px solid #1e293b',
  borderRadius: 8,
  padding: 14,
};

const detailsStyle: CSSProperties = {
  margin: '8px 0 0',
  whiteSpace: 'pre-wrap',
  wordBreak: 'break-word',
  color: '#94a3b8',
  backgroundColor: '#020617',
  border: '1px solid #1e293b',
  borderRadius: 6,
  padding: 10,
  fontSize: 11,
};

const textareaStyle: CSSProperties = {
  width: '100%',
  boxSizing: 'border-box',
  minHeight: 58,
  borderRadius: 6,
  border: '1px solid #334155',
  backgroundColor: '#020617',
  color: '#e2e8f0',
  padding: '8px 10px',
  fontSize: 12,
  resize: 'vertical',
};

function buttonStyle(enabled: boolean, color: string, minWidth?: number): CSSProperties {
  return {
    border: 'none',
    borderRadius: 6,
    padding: '8px 12px',
    backgroundColor: enabled ? color : '#334155',
    color: '#fff',
    cursor: enabled ? 'pointer' : 'default',
    fontSize: 13,
    fontWeight: 700,
    minWidth,
  };
}
