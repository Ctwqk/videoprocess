import type { AutoFlowPlan, GraphPlanningAttempt, GraphPlanningResult } from '../../types/autoflow';

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function graphPlanningResult(plan: AutoFlowPlan | null): GraphPlanningResult | null {
  const raw = plan?.validation.graph_planning;
  if (!isRecord(raw) || !Array.isArray(raw.attempts)) return null;
  return raw as unknown as GraphPlanningResult;
}

function issueLabel(issue: Record<string, unknown>) {
  const code = typeof issue.code === 'string' ? issue.code : typeof issue.type === 'string' ? issue.type : 'issue';
  const message = typeof issue.message === 'string' ? issue.message : '';
  return message ? `${code}: ${message}` : code;
}

function AttemptSummary({ attempt }: { attempt: GraphPlanningAttempt }) {
  return (
    <div className="autoflow-graph-attempt">
      <div className="autoflow-graph-attempt-header">
        <span>Attempt {attempt.attempt}</span>
        <span>{attempt.source}</span>
        <span className={attempt.valid ? 'autoflow-graph-ok' : 'autoflow-graph-error'}>
          {attempt.valid ? 'valid' : 'invalid'}
        </span>
      </div>
      {attempt.repairs.length > 0 ? (
        <div className="autoflow-graph-list">
          {attempt.repairs.map(repair => (
            <span key={repair}>{repair}</span>
          ))}
        </div>
      ) : null}
      {attempt.errors.length > 0 ? (
        <ul className="autoflow-graph-issues">
          {attempt.errors.map((issue, index) => (
            <li key={`${attempt.attempt}-error-${index}`}>{issueLabel(issue)}</li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}

export default function AutoFlowGraphPlannerDetails({ plan }: { plan: AutoFlowPlan | null }) {
  const graph = graphPlanningResult(plan);
  if (!graph) return null;

  const draft = graph.draft;
  const policy = isRecord(graph.policy) ? graph.policy : {};
  const policyRepairs = Array.isArray(policy.repairs) ? policy.repairs.map(String) : [];

  return (
    <section className="autoflow-graph-panel">
      <div className="autoflow-graph-header">
        <div>
          <h2>AI Graph Planner</h2>
          <p>{draft?.name ?? 'Generated graph plan'}</p>
        </div>
        <span>{plan?.request.planning_mode ?? 'auto'}</span>
      </div>

      {draft?.assumptions.length ? (
        <div className="autoflow-graph-block">
          <div className="autoflow-graph-label">Assumptions</div>
          <div className="autoflow-graph-list">
            {draft.assumptions.map(item => (
              <span key={item}>{item}</span>
            ))}
          </div>
        </div>
      ) : null}

      {draft?.risk_flags.length ? (
        <div className="autoflow-graph-block">
          <div className="autoflow-graph-label">Risk flags</div>
          <div className="autoflow-graph-list">
            {draft.risk_flags.map(item => (
              <span key={item}>{item}</span>
            ))}
          </div>
        </div>
      ) : null}

      {policyRepairs.length ? (
        <div className="autoflow-graph-block">
          <div className="autoflow-graph-label">Policy repairs</div>
          <div className="autoflow-graph-list">
            {policyRepairs.map(item => (
              <span key={item}>{item}</span>
            ))}
          </div>
        </div>
      ) : null}

      <div className="autoflow-graph-attempts">
        {graph.attempts.map(attempt => (
          <AttemptSummary key={`${attempt.attempt}-${attempt.source}`} attempt={attempt} />
        ))}
      </div>
    </section>
  );
}
