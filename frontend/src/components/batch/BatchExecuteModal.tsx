import type { CSSProperties } from 'react';

export function parseBatchItems(input: string): Array<Record<string, unknown>> {
  const parsed = JSON.parse(input);
  if (!Array.isArray(parsed) || parsed.some(item => !item || typeof item !== 'object' || Array.isArray(item))) {
    throw new Error('Batch input must be a JSON array of parameter dictionaries');
  }
  if (parsed.length === 0) {
    throw new Error('Batch input cannot be empty');
  }
  return parsed as Array<Record<string, unknown>>;
}

export default function BatchExecuteModal({
  title,
  description,
  value,
  submitting,
  error,
  onChange,
  onClose,
  onSubmit,
}: {
  title: string;
  description: string;
  value: string;
  submitting: boolean;
  error: string | null;
  onChange: (value: string) => void;
  onClose: () => void;
  onSubmit: () => void;
}) {
  return (
    <div style={overlayStyle}>
      <div style={modalStyle}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <div>
            <div style={{ fontSize: 18, fontWeight: 700 }}>{title}</div>
            <div style={{ fontSize: 12, color: '#94a3b8', marginTop: 4 }}>{description}</div>
          </div>
          <button
            onClick={onClose}
            disabled={submitting}
            style={{
              background: 'none',
              border: 'none',
              color: '#94a3b8',
              fontSize: 24,
              cursor: submitting ? 'default' : 'pointer',
            }}
          >
            ×
          </button>
        </div>

        <div style={hintStyle}>
          {`支持两种 key 形式：
1. 点路径：trim.duration, src.asset_id
2. 节点字典：{"trim":{"duration":"2"}}

每个数组元素会提交成一个 job。`}
        </div>

        {error ? (
          <div style={errorStyle}>
            {error}
          </div>
        ) : null}

        <textarea
          value={value}
          onChange={e => onChange(e.target.value)}
          spellCheck={false}
          style={textareaStyle}
        />

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10, marginTop: 14 }}>
          <button onClick={onClose} disabled={submitting} style={secondaryButtonStyle}>
            Cancel
          </button>
          <button onClick={onSubmit} disabled={submitting} style={primaryButtonStyle}>
            {submitting ? 'Submitting...' : 'Submit Batch'}
          </button>
        </div>
      </div>
    </div>
  );
}

const overlayStyle: CSSProperties = {
  position: 'fixed',
  inset: 0,
  backgroundColor: 'rgba(15, 23, 42, 0.72)',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  padding: 24,
  zIndex: 40,
};

const modalStyle: CSSProperties = {
  width: 'min(920px, 100%)',
  maxHeight: '85vh',
  overflow: 'auto',
  backgroundColor: '#0f172a',
  border: '1px solid #334155',
  borderRadius: 14,
  padding: 20,
  boxShadow: '0 24px 80px rgba(2, 6, 23, 0.45)',
};

const hintStyle: CSSProperties = {
  marginBottom: 12,
  padding: '10px 12px',
  borderRadius: 8,
  backgroundColor: '#111827',
  border: '1px solid #1f2937',
  color: '#cbd5e1',
  fontSize: 12,
  lineHeight: 1.6,
  whiteSpace: 'pre-wrap',
};

const errorStyle: CSSProperties = {
  marginBottom: 12,
  padding: '10px 12px',
  borderRadius: 8,
  backgroundColor: '#450a0a',
  color: '#fca5a5',
  border: '1px solid #7f1d1d',
  fontSize: 13,
};

const textareaStyle: CSSProperties = {
  width: '100%',
  minHeight: 320,
  resize: 'vertical',
  borderRadius: 10,
  padding: 14,
  backgroundColor: '#020617',
  color: '#e2e8f0',
  border: '1px solid #334155',
  fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
  fontSize: 13,
  lineHeight: 1.5,
};

const secondaryButtonStyle: CSSProperties = {
  padding: '8px 16px',
  backgroundColor: '#0f172a',
  color: '#cbd5e1',
  border: '1px solid #334155',
  borderRadius: 8,
  cursor: 'pointer',
};

const primaryButtonStyle: CSSProperties = {
  padding: '8px 16px',
  backgroundColor: '#2563eb',
  color: '#fff',
  border: 'none',
  borderRadius: 8,
  cursor: 'pointer',
  fontWeight: 600,
};
