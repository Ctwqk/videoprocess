import type {
  AutoFlowAspectRatio,
  AutoFlowPlanningMode,
  AutoFlowPublishMode,
  AutoFlowRequest,
  AutoFlowSourcePolicy,
  AutoFlowSourceStrategy,
  CapabilityManifest,
  WorkflowTemplate,
} from '../../types/autoflow';

const DEFAULT_PLATFORMS = ['youtube', 'youtube_shorts', 'x', 'xiaohongshu'];
const DEFAULT_SOURCE_PLATFORMS = ['youtube', 'bilibili', 'x', 'xiaohongshu'];
const DEFAULT_ASPECT_RATIOS: AutoFlowAspectRatio[] = ['auto', '9:16', '16:9', '1:1'];
const DEFAULT_SOURCE_POLICIES: AutoFlowSourcePolicy[] = [
  'owned_only',
  'licensed_only',
  'public_domain_or_cc',
  'research_only',
  'remix_with_review',
];
const DEFAULT_SOURCE_STRATEGIES: AutoFlowSourceStrategy[] = [
  'auto',
  'input_video',
  'material_library',
  'hybrid',
  'generate_missing',
];
const DEFAULT_PUBLISH_MODES: AutoFlowPublishMode[] = [
  'preview_only',
  'private_upload',
  'unlisted_upload',
  'public_after_review',
];
const PLANNING_MODES: AutoFlowPlanningMode[] = ['auto', 'template', 'storyboard', 'ai_graph'];

const SOURCE_POLICY_LABELS: Record<AutoFlowSourcePolicy, string> = {
  owned_only: 'Owned only',
  licensed_only: 'Licensed only',
  public_domain_or_cc: 'Public domain / CC',
  research_only: 'Research only',
  remix_with_review: 'Remix with review',
};

const SOURCE_STRATEGY_LABELS: Record<AutoFlowSourceStrategy, string> = {
  auto: 'Auto',
  input_video: 'Input video',
  material_library: 'Material library',
  external_research: 'External research',
  generate_missing: 'Generate missing',
  hybrid: 'Hybrid',
};

const PUBLISH_MODE_LABELS: Record<AutoFlowPublishMode, string> = {
  preview_only: 'Preview only',
  private_upload: 'Private upload',
  unlisted_upload: 'Unlisted upload',
  public_after_review: 'Public after review',
};

const PLANNING_MODE_LABELS: Record<AutoFlowPlanningMode, string> = {
  auto: 'Auto',
  template: 'Template',
  storyboard: 'Storyboard',
  ai_graph: 'AI graph',
};

function titleCase(value: string) {
  return value
    .split(/[_-]/g)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function getPreferredTemplateId(request: AutoFlowRequest) {
  const id = request.user_constraints.preferred_template_id;
  return typeof id === 'string' ? id : '';
}

export default function AutoFlowPromptBox({
  value,
  templates,
  capabilities,
  planning,
  loadingReferenceData,
  onChange,
  onSubmit,
}: {
  value: AutoFlowRequest;
  templates: WorkflowTemplate[];
  capabilities: CapabilityManifest | null;
  planning: boolean;
  loadingReferenceData: boolean;
  onChange: (next: AutoFlowRequest) => void;
  onSubmit: () => void;
}) {
  const platformOptions = capabilities?.target_platforms ?? capabilities?.platforms ?? DEFAULT_PLATFORMS;
  const sourcePlatformOptions = capabilities?.source_platforms ?? DEFAULT_SOURCE_PLATFORMS;
  const aspectRatios = capabilities?.aspect_ratios ?? DEFAULT_ASPECT_RATIOS;
  const sourcePolicies = capabilities?.source_policies ?? DEFAULT_SOURCE_POLICIES;
  const sourceStrategies = DEFAULT_SOURCE_STRATEGIES;
  const publishModes = capabilities?.publish_modes ?? DEFAULT_PUBLISH_MODES;
  const materialLibraries = capabilities?.material_libraries ?? [];

  const update = <K extends keyof AutoFlowRequest>(key: K, next: AutoFlowRequest[K]) => {
    onChange({ ...value, [key]: next });
  };

  const togglePlatform = (platform: string) => {
    const enabled = value.target_platforms.includes(platform);
    update(
      'target_platforms',
      enabled
        ? value.target_platforms.filter(item => item !== platform)
        : [...value.target_platforms, platform],
    );
  };

  const toggleSourcePlatform = (platform: string) => {
    const enabled = value.source_platforms.includes(platform);
    update(
      'source_platforms',
      enabled
        ? value.source_platforms.filter(item => item !== platform)
        : [...value.source_platforms, platform],
    );
  };

  const toggleMaterialLibrary = (libraryId: string) => {
    const enabled = value.material_library_ids.includes(libraryId);
    update(
      'material_library_ids',
      enabled
        ? value.material_library_ids.filter(item => item !== libraryId)
        : [...value.material_library_ids, libraryId],
    );
  };

  const updatePreferredTemplate = (templateId: string) => {
    const userConstraints = { ...value.user_constraints };
    if (templateId) {
      userConstraints.preferred_template_id = templateId;
    } else {
      delete userConstraints.preferred_template_id;
    }
    update('user_constraints', userConstraints);
  };

  return (
    <form
      onSubmit={event => {
        event.preventDefault();
        onSubmit();
      }}
      style={{
        display: 'grid',
        gap: 14,
        backgroundColor: '#0f172a',
        border: '1px solid #1e293b',
        borderRadius: 8,
        padding: 16,
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center' }}>
        <div>
          <h1 style={{ margin: 0, fontSize: 20, lineHeight: 1.2, color: '#f8fafc' }}>AutoFlow</h1>
          <div style={{ marginTop: 4, fontSize: 12, color: '#94a3b8' }}>
            Generate a reviewed workflow without opening the editor.
          </div>
        </div>
        {loadingReferenceData ? (
          <div style={{ fontSize: 12, color: '#93c5fd' }}>Loading capabilities...</div>
        ) : null}
      </div>

      <label style={{ display: 'grid', gap: 8, fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>
        Prompt
        <textarea
          value={value.prompt}
          onChange={event => update('prompt', event.target.value)}
          placeholder="Create a 45 second vertical preview from owned clips, with subtitles and safe metadata."
          rows={5}
          style={{
            width: '100%',
            boxSizing: 'border-box',
            resize: 'vertical',
            minHeight: 112,
            borderRadius: 8,
            border: '1px solid #334155',
            backgroundColor: '#020617',
            color: '#e2e8f0',
            padding: 12,
            fontSize: 13,
            lineHeight: 1.5,
          }}
        />
      </label>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))', gap: 12 }}>
        <label style={{ display: 'grid', gap: 6, fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>
          Planning mode
          <select
            value={value.planning_mode}
            onChange={event => update('planning_mode', event.target.value as AutoFlowPlanningMode)}
            style={{
              borderRadius: 6,
              border: '1px solid #334155',
              backgroundColor: '#020617',
              color: '#e2e8f0',
              padding: '8px 10px',
              fontSize: 13,
            }}
          >
            {PLANNING_MODES.map(option => (
              <option key={option} value={option}>{PLANNING_MODE_LABELS[option]}</option>
            ))}
          </select>
        </label>

        <label style={{ display: 'grid', gap: 6, fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>
          Duration
          <input
            type="number"
            min={5}
            max={3600}
            value={value.duration_sec ?? ''}
            onChange={event => update('duration_sec', event.target.value ? Number(event.target.value) : null)}
            placeholder="Auto"
            style={{
              borderRadius: 6,
              border: '1px solid #334155',
              backgroundColor: '#020617',
              color: '#e2e8f0',
              padding: '8px 10px',
              fontSize: 13,
            }}
          />
        </label>

        <label style={{ display: 'grid', gap: 6, fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>
          Aspect ratio
          <select
            value={value.aspect_ratio}
            onChange={event => update('aspect_ratio', event.target.value as AutoFlowAspectRatio)}
            style={{
              borderRadius: 6,
              border: '1px solid #334155',
              backgroundColor: '#020617',
              color: '#e2e8f0',
              padding: '8px 10px',
              fontSize: 13,
            }}
          >
            {aspectRatios.map(option => (
              <option key={option} value={option}>{option}</option>
            ))}
          </select>
        </label>

        <label style={{ display: 'grid', gap: 6, fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>
          Source policy
          <select
            value={value.source_policy}
            onChange={event => update('source_policy', event.target.value as AutoFlowSourcePolicy)}
            style={{
              borderRadius: 6,
              border: '1px solid #334155',
              backgroundColor: '#020617',
              color: '#e2e8f0',
              padding: '8px 10px',
              fontSize: 13,
            }}
          >
            {sourcePolicies.map(option => (
              <option key={option} value={option}>{SOURCE_POLICY_LABELS[option] ?? titleCase(option)}</option>
            ))}
          </select>
        </label>

        <label style={{ display: 'grid', gap: 6, fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>
          Source strategy
          <select
            value={value.source_strategy}
            onChange={event => update('source_strategy', event.target.value as AutoFlowSourceStrategy)}
            style={{
              borderRadius: 6,
              border: '1px solid #334155',
              backgroundColor: '#020617',
              color: '#e2e8f0',
              padding: '8px 10px',
              fontSize: 13,
            }}
          >
            {sourceStrategies.map(option => (
              <option key={option} value={option}>{SOURCE_STRATEGY_LABELS[option]}</option>
            ))}
          </select>
        </label>

        <label style={{ display: 'grid', gap: 6, fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>
          Publish mode
          <select
            value={value.publish_mode}
            onChange={event => update('publish_mode', event.target.value as AutoFlowPublishMode)}
            style={{
              borderRadius: 6,
              border: '1px solid #334155',
              backgroundColor: '#020617',
              color: '#e2e8f0',
              padding: '8px 10px',
              fontSize: 13,
            }}
          >
            {publishModes.map(option => (
              <option key={option} value={option}>{PUBLISH_MODE_LABELS[option] ?? titleCase(option)}</option>
            ))}
          </select>
        </label>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))', gap: 12 }}>
        <label style={{ display: 'grid', gap: 6, fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>
          Input asset ID
          <input
            type="text"
            value={value.input_asset_id ?? ''}
            onChange={event => update('input_asset_id', event.target.value.trim() || null)}
            placeholder="Optional uploaded video asset"
            style={{
              borderRadius: 6,
              border: '1px solid #334155',
              backgroundColor: '#020617',
              color: '#e2e8f0',
              padding: '8px 10px',
              fontSize: 13,
            }}
          />
        </label>

        <label style={{ display: 'grid', gap: 6, fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>
          Min shots
          <input
            type="number"
            min={1}
            max={24}
            value={value.min_shots}
            onChange={event => update('min_shots', Number(event.target.value) || 1)}
            style={{
              borderRadius: 6,
              border: '1px solid #334155',
              backgroundColor: '#020617',
              color: '#e2e8f0',
              padding: '8px 10px',
              fontSize: 13,
            }}
          />
        </label>

        <label style={{ display: 'grid', gap: 6, fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>
          Max shots
          <input
            type="number"
            min={1}
            max={24}
            value={value.max_shots}
            onChange={event => update('max_shots', Number(event.target.value) || 1)}
            style={{
              borderRadius: 6,
              border: '1px solid #334155',
              backgroundColor: '#020617',
              color: '#e2e8f0',
              padding: '8px 10px',
              fontSize: 13,
            }}
          />
        </label>

        <label
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            fontSize: 12,
            color: '#cbd5e1',
            fontWeight: 600,
            paddingTop: 20,
          }}
        >
          <input
            type="checkbox"
            checked={value.allow_video_generation}
            onChange={event => update('allow_video_generation', event.target.checked)}
          />
          Allow generation placeholders
        </label>
      </div>

      <div style={{ display: 'grid', gap: 8 }}>
        <div style={{ fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>Target platforms</div>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          {platformOptions.map(platform => {
            const selected = value.target_platforms.includes(platform);
            return (
              <button
                key={platform}
                type="button"
                onClick={() => togglePlatform(platform)}
                style={{
                  border: `1px solid ${selected ? '#2563eb' : '#334155'}`,
                  borderRadius: 6,
                  padding: '7px 10px',
                  backgroundColor: selected ? '#172554' : '#020617',
                  color: selected ? '#bfdbfe' : '#94a3b8',
                  cursor: 'pointer',
                  fontSize: 12,
                  fontWeight: 600,
                }}
              >
                {titleCase(platform)}
              </button>
            );
          })}
        </div>
      </div>

      <div style={{ display: 'grid', gap: 8 }}>
        <div style={{ fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>Source platforms</div>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          {sourcePlatformOptions.map(platform => {
            const selected = value.source_platforms.includes(platform);
            return (
              <button
                key={platform}
                type="button"
                onClick={() => toggleSourcePlatform(platform)}
                style={{
                  border: `1px solid ${selected ? '#0f766e' : '#334155'}`,
                  borderRadius: 6,
                  padding: '7px 10px',
                  backgroundColor: selected ? '#134e4a' : '#020617',
                  color: selected ? '#ccfbf1' : '#94a3b8',
                  cursor: 'pointer',
                  fontSize: 12,
                  fontWeight: 600,
                }}
              >
                {titleCase(platform)}
              </button>
            );
          })}
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 12 }}>
        <label style={{ display: 'grid', gap: 6, fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>
          Template hint
          <select
            value={getPreferredTemplateId(value)}
            onChange={event => updatePreferredTemplate(event.target.value)}
            style={{
              borderRadius: 6,
              border: '1px solid #334155',
              backgroundColor: '#020617',
              color: '#e2e8f0',
              padding: '8px 10px',
              fontSize: 13,
            }}
          >
            <option value="">Auto select</option>
            {templates.map(template => (
              <option key={template.id} value={template.id}>{template.name}</option>
            ))}
          </select>
        </label>

        <label style={{ display: 'grid', gap: 6, fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>
          Material library IDs
          <input
            value={value.material_library_ids.join(', ')}
            onChange={event => update('material_library_ids', event.target.value.split(',').map(item => item.trim()).filter(Boolean))}
            placeholder="library-id-1, library-id-2"
            style={{
              borderRadius: 6,
              border: '1px solid #334155',
              backgroundColor: '#020617',
              color: '#e2e8f0',
              padding: '8px 10px',
              fontSize: 13,
            }}
          />
        </label>
      </div>

      {materialLibraries.length > 0 ? (
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          {materialLibraries.map(library => {
            const selected = value.material_library_ids.includes(library.id);
            return (
              <button
                key={library.id}
                type="button"
                onClick={() => toggleMaterialLibrary(library.id)}
                title={library.description ?? undefined}
                style={{
                  border: `1px solid ${selected ? '#0f766e' : '#334155'}`,
                  borderRadius: 6,
                  padding: '6px 9px',
                  backgroundColor: selected ? '#134e4a' : '#020617',
                  color: selected ? '#ccfbf1' : '#94a3b8',
                  cursor: 'pointer',
                  fontSize: 12,
                }}
              >
                {library.name}
              </button>
            );
          })}
        </div>
      ) : null}

      <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
        <button
          type="submit"
          disabled={planning || !value.prompt.trim()}
          style={{
            border: 'none',
            borderRadius: 6,
            padding: '9px 16px',
            backgroundColor: planning || !value.prompt.trim() ? '#334155' : '#2563eb',
            color: '#fff',
            cursor: planning || !value.prompt.trim() ? 'default' : 'pointer',
            fontSize: 13,
            fontWeight: 700,
            minWidth: 128,
          }}
        >
          {planning ? 'Generating...' : 'Generate Plan'}
        </button>
      </div>
    </form>
  );
}
