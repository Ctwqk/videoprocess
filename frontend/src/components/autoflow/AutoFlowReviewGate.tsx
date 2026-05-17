import type { AutoFlowPlan } from '../../types/autoflow';

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

function isBlocked(plan: AutoFlowPlan) {
  const status = statusFromRecord(plan.rights);
  const explicitlyAllowed = boolFromRecord(plan.rights, ['allowed', 'execute_allowed', 'can_execute']);
  if (explicitlyAllowed === false) return true;
  return ['blocked', 'denied', 'rejected'].includes(status);
}

function isPublishBlocked(plan: AutoFlowPlan) {
  const publishAllowed = boolFromRecord(plan.rights, ['publish_allowed', 'can_publish']);
  if (publishAllowed === false) return true;
  const publishStatus = plan.rights.publish_status ?? plan.rights.publish_decision;
  return typeof publishStatus === 'string' && ['blocked', 'denied', 'rejected'].includes(publishStatus.toLowerCase());
}

function gateMessage(plan: AutoFlowPlan, approved: boolean) {
  if (isBlocked(plan)) return 'Execution is blocked by rights policy.';
  if (plan.needs_review && !approved) return 'Human review is required before execution.';
  if (isPublishBlocked(plan)) return 'Execution is allowed, but publishing is blocked.';
  return 'Plan is ready to execute.';
}

export default function AutoFlowReviewGate({
  plan,
  approved,
  approving,
  executing,
  onApprove,
  onExecute,
}: {
  plan: AutoFlowPlan | null;
  approved: boolean;
  approving: boolean;
  executing: boolean;
  onApprove: () => void;
  onExecute: () => void;
}) {
  if (!plan) {
    return (
      <section
        style={{
          backgroundColor: '#0f172a',
          border: '1px solid #1e293b',
          borderRadius: 8,
          padding: 14,
        }}
      >
        <h2 style={{ margin: '0 0 8px', fontSize: 14, color: '#f8fafc' }}>Review Gate</h2>
        <div style={{ fontSize: 13, color: '#94a3b8' }}>Generate a plan to review rights and execute controls.</div>
      </section>
    );
  }

  const blocked = isBlocked(plan);
  const publishBlocked = isPublishBlocked(plan);
  const approvalSatisfied = !plan.needs_review || approved;
  const executeDisabled = blocked || !approvalSatisfied || approving || executing;
  const statusColor = blocked ? '#fca5a5' : approvalSatisfied ? '#86efac' : '#fde68a';

  return (
    <section
      style={{
        backgroundColor: '#0f172a',
        border: `1px solid ${blocked ? '#7f1d1d' : approvalSatisfied ? '#166534' : '#854d0e'}`,
        borderRadius: 8,
        padding: 14,
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, marginBottom: 10 }}>
        <h2 style={{ margin: 0, fontSize: 14, color: '#f8fafc' }}>Review Gate</h2>
        <div style={{ color: statusColor, fontSize: 12, fontWeight: 700 }}>
          {blocked ? 'Blocked' : approvalSatisfied ? 'Executable' : 'Review required'}
        </div>
      </div>

      <div style={{ fontSize: 13, color: '#cbd5e1', marginBottom: 12 }}>
        {gateMessage(plan, approved)}
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: 8, marginBottom: 12 }}>
        <div style={{ padding: 8, borderRadius: 6, backgroundColor: '#020617', border: '1px solid #1e293b' }}>
          <div style={{ fontSize: 11, color: '#64748b' }}>Execute</div>
          <div style={{ fontSize: 13, color: blocked ? '#fca5a5' : '#86efac', fontWeight: 700 }}>
            {blocked ? 'Denied' : 'Allowed'}
          </div>
        </div>
        <div style={{ padding: 8, borderRadius: 6, backgroundColor: '#020617', border: '1px solid #1e293b' }}>
          <div style={{ fontSize: 11, color: '#64748b' }}>Publish</div>
          <div style={{ fontSize: 13, color: publishBlocked ? '#fca5a5' : '#86efac', fontWeight: 700 }}>
            {publishBlocked ? 'Blocked' : 'Allowed'}
          </div>
        </div>
        <div style={{ padding: 8, borderRadius: 6, backgroundColor: '#020617', border: '1px solid #1e293b' }}>
          <div style={{ fontSize: 11, color: '#64748b' }}>Review</div>
          <div style={{ fontSize: 13, color: approvalSatisfied ? '#86efac' : '#fde68a', fontWeight: 700 }}>
            {approvalSatisfied ? 'Approved' : 'Needed'}
          </div>
        </div>
      </div>

      {Object.keys(plan.rights).length > 0 ? (
        <details style={{ marginBottom: 12 }}>
          <summary style={{ cursor: 'pointer', color: '#93c5fd', fontSize: 12 }}>Rights details</summary>
          <pre
            style={{
              margin: '8px 0 0',
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
              color: '#94a3b8',
              backgroundColor: '#020617',
              border: '1px solid #1e293b',
              borderRadius: 6,
              padding: 10,
              fontSize: 11,
            }}
          >
            {JSON.stringify(plan.rights, null, 2)}
          </pre>
        </details>
      ) : null}

      <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, flexWrap: 'wrap' }}>
        <button
          type="button"
          disabled={!plan.needs_review || approved || approving}
          onClick={onApprove}
          style={{
            border: '1px solid #115e59',
            borderRadius: 6,
            padding: '8px 12px',
            backgroundColor: !plan.needs_review || approved ? '#1e293b' : '#0f766e',
            color: !plan.needs_review || approved ? '#94a3b8' : '#ccfbf1',
            cursor: !plan.needs_review || approved || approving ? 'default' : 'pointer',
            fontSize: 13,
            fontWeight: 700,
          }}
        >
          {approving ? 'Approving...' : approved || !plan.needs_review ? 'Approved' : 'Approve'}
        </button>
        <button
          type="button"
          disabled={executeDisabled}
          onClick={onExecute}
          style={{
            border: 'none',
            borderRadius: 6,
            padding: '8px 14px',
            backgroundColor: executeDisabled ? '#334155' : '#2563eb',
            color: '#fff',
            cursor: executeDisabled ? 'default' : 'pointer',
            fontSize: 13,
            fontWeight: 700,
            minWidth: 112,
          }}
        >
          {executing ? 'Executing...' : 'Execute'}
        </button>
      </div>
    </section>
  );
}
