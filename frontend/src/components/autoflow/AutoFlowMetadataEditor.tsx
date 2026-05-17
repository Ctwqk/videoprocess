import type { CSSProperties } from 'react';
import type { AutoFlowMetadataEditDraft, AutoFlowPlan, AutoFlowPublishMode } from '../../types/autoflow';

const PUBLISH_MODES: AutoFlowPublishMode[] = [
  'preview_only',
  'private_upload',
  'unlisted_upload',
  'public_after_review',
];

function parseList(value: string) {
  return value
    .split(',')
    .map(item => item.trim())
    .filter(Boolean);
}

function parseHashtags(value: string) {
  return parseList(value).map(item => item.startsWith('#') ? item : `#${item}`);
}

function titleCase(value: string) {
  return value
    .split(/[_-]/g)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

export default function AutoFlowMetadataEditor({
  plan,
  draft,
  dirty,
  saving,
  onChange,
  onSave,
}: {
  plan: AutoFlowPlan | null;
  draft: AutoFlowMetadataEditDraft;
  dirty: boolean;
  saving: boolean;
  onChange: (next: AutoFlowMetadataEditDraft) => void;
  onSave: () => void;
}) {
  if (!plan) {
    return (
      <section style={sectionStyle}>
        <h2 style={headingStyle}>Metadata</h2>
        <div style={mutedStyle}>Generate a plan to edit metadata.</div>
      </section>
    );
  }

  const titleListId = `autoflow-title-candidates-${plan.plan_id}`;
  const update = <K extends keyof AutoFlowMetadataEditDraft>(key: K, value: AutoFlowMetadataEditDraft[K]) => {
    onChange({ ...draft, [key]: value });
  };

  return (
    <section style={sectionStyle}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center' }}>
        <h2 style={headingStyle}>Metadata</h2>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          {dirty ? <span style={{ ...mutedStyle, color: '#fde68a' }}>Unsaved</span> : null}
          <button
            type="button"
            disabled={!dirty || saving}
            onClick={onSave}
            style={{
              border: 'none',
              borderRadius: 6,
              padding: '7px 10px',
              backgroundColor: !dirty || saving ? '#334155' : '#2563eb',
              color: '#fff',
              cursor: !dirty || saving ? 'default' : 'pointer',
              fontSize: 12,
              fontWeight: 700,
            }}
          >
            {saving ? 'Saving...' : 'Save'}
          </button>
        </div>
      </div>

      <label style={labelStyle}>
        Selected title
        <input
          list={titleListId}
          value={draft.selected_title}
          onChange={event => update('selected_title', event.target.value)}
          style={inputStyle}
        />
        <datalist id={titleListId}>
          {plan.metadata.title_candidates.map(title => (
            <option key={title} value={title} />
          ))}
        </datalist>
      </label>

      <label style={labelStyle}>
        Description
        <textarea
          value={draft.description}
          onChange={event => update('description', event.target.value)}
          rows={4}
          style={{ ...inputStyle, resize: 'vertical', lineHeight: 1.5 }}
        />
      </label>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 10 }}>
        <label style={labelStyle}>
          Tags
          <input
            value={draft.tags.join(', ')}
            onChange={event => update('tags', parseList(event.target.value))}
            style={inputStyle}
          />
        </label>

        <label style={labelStyle}>
          Hashtags
          <input
            value={draft.hashtags.join(', ')}
            onChange={event => update('hashtags', parseHashtags(event.target.value))}
            style={inputStyle}
          />
        </label>

        <label style={labelStyle}>
          Publish mode
          <select
            value={draft.publish_mode}
            onChange={event => update('publish_mode', event.target.value as AutoFlowPublishMode)}
            style={inputStyle}
          >
            {PUBLISH_MODES.map(mode => (
              <option key={mode} value={mode}>{titleCase(mode)}</option>
            ))}
          </select>
        </label>
      </div>
    </section>
  );
}

const sectionStyle: CSSProperties = {
  backgroundColor: '#0f172a',
  border: '1px solid #1e293b',
  borderRadius: 8,
  padding: 14,
  display: 'grid',
  gap: 12,
};

const headingStyle: CSSProperties = {
  margin: 0,
  fontSize: 14,
  color: '#f8fafc',
};

const labelStyle: CSSProperties = {
  display: 'grid',
  gap: 6,
  fontSize: 12,
  color: '#cbd5e1',
  fontWeight: 600,
};

const inputStyle: CSSProperties = {
  width: '100%',
  boxSizing: 'border-box',
  borderRadius: 6,
  border: '1px solid #334155',
  backgroundColor: '#020617',
  color: '#e2e8f0',
  padding: '8px 10px',
  fontSize: 13,
};

const mutedStyle: CSSProperties = {
  color: '#94a3b8',
  fontSize: 12,
};
