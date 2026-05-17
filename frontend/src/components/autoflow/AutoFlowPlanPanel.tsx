import type { ReactNode } from 'react';
import type { AutoFlowPlan, WorkflowTemplate } from '../../types/autoflow';

function formatValue(value: unknown) {
  if (value === null || value === undefined || value === '') return '-';
  if (typeof value === 'boolean') return value ? 'Yes' : 'No';
  if (Array.isArray(value)) return value.length ? value.join(', ') : '-';
  if (typeof value === 'object') return JSON.stringify(value, null, 2);
  return String(value);
}

function getValidationStatus(validation: Record<string, unknown>) {
  const status = validation.status ?? validation.result ?? validation.valid;
  if (typeof status === 'boolean') return status ? 'Valid' : 'Invalid';
  if (typeof status === 'string') return status;
  return Object.keys(validation).length ? 'Available' : 'Not reported';
}

function getRightsStatus(rights: Record<string, unknown>) {
  const status = rights.decision ?? rights.status ?? rights.rights_status;
  return typeof status === 'string' && status ? status : Object.keys(rights).length ? 'Review required' : 'Not reported';
}

function Section({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <section
      style={{
        backgroundColor: '#0f172a',
        border: '1px solid #1e293b',
        borderRadius: 8,
        padding: 14,
      }}
    >
      <h2 style={{ margin: '0 0 10px', fontSize: 14, color: '#f8fafc' }}>{title}</h2>
      {children}
    </section>
  );
}

function KeyValueGrid({
  items,
}: {
  items: Array<{ label: string; value: unknown }>;
}) {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: 10 }}>
      {items.map(item => (
        <div key={item.label}>
          <div style={{ fontSize: 11, color: '#64748b', marginBottom: 3 }}>{item.label}</div>
          <div style={{ fontSize: 13, color: '#e2e8f0', wordBreak: 'break-word' }}>
            {formatValue(item.value)}
          </div>
        </div>
      ))}
    </div>
  );
}

export default function AutoFlowPlanPanel({
  plan,
  templates,
}: {
  plan: AutoFlowPlan | null;
  templates: WorkflowTemplate[];
}) {
  if (!plan) {
    return (
      <Section title="Generated Plan">
        <div style={{ color: '#94a3b8', fontSize: 13 }}>
          Generate a plan to inspect intent, selected template, validation, rights, and metadata.
        </div>
      </Section>
    );
  }

  const template = templates.find(item => item.id === plan.template_id);
  const selectedTitle = plan.metadata.selected_title ?? plan.metadata.title_candidates[0] ?? '-';

  return (
    <div style={{ display: 'grid', gap: 12 }}>
      <Section title="Intent">
        <KeyValueGrid
          items={[
            { label: 'Plan ID', value: plan.plan_id },
            { label: 'Type', value: plan.intent.intent_type },
            { label: 'Subject', value: plan.intent.subject },
            { label: 'Style', value: plan.intent.style },
            { label: 'Duration', value: `${plan.intent.duration_sec}s` },
            { label: 'Aspect', value: plan.intent.aspect_ratio },
            { label: 'Platforms', value: plan.intent.target_platforms },
          ]}
        />
      </Section>

      <Section title="Template And Checks">
        <KeyValueGrid
          items={[
            { label: 'Template', value: template?.name ?? plan.template_id },
            { label: 'Validation', value: getValidationStatus(plan.validation) },
            { label: 'Rights', value: getRightsStatus(plan.rights) },
            { label: 'Needs review', value: plan.needs_review },
            { label: 'Source policy', value: plan.intent.source_policy },
            { label: 'Publish mode', value: plan.intent.publish_mode },
          ]}
        />
      </Section>

      <Section title="Metadata">
        <KeyValueGrid
          items={[
            { label: 'Selected title', value: selectedTitle },
            { label: 'Tags', value: plan.metadata.tags },
            { label: 'Hashtags', value: plan.metadata.hashtags },
            { label: 'Thumbnail text', value: plan.metadata.thumbnail_text_candidates },
          ]}
        />
        {plan.metadata.description ? (
          <div style={{ marginTop: 10, fontSize: 12, color: '#94a3b8', lineHeight: 1.5 }}>
            {plan.metadata.description}
          </div>
        ) : null}
      </Section>

      {plan.warnings.length > 0 ? (
        <Section title="Warnings">
          <div style={{ display: 'grid', gap: 6 }}>
            {plan.warnings.map(warning => (
              <div
                key={warning}
                style={{
                  padding: '8px 10px',
                  borderRadius: 6,
                  border: '1px solid #854d0e',
                  backgroundColor: '#422006',
                  color: '#fde68a',
                  fontSize: 12,
                }}
              >
                {warning}
              </div>
            ))}
          </div>
        </Section>
      ) : null}
    </div>
  );
}
