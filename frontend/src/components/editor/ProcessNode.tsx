import { memo, useEffect } from 'react';
import { Handle, Position, useConnection, useUpdateNodeInternals, type NodeProps } from '@xyflow/react';
import useNodeTypes from '../../hooks/useNodeTypes';
import NodeIcon from './NodeIcon';
import { getZipChannelCount } from '../../utils/zipRecords';

type ProcessNodeData = {
  label: string;
  config: Record<string, unknown>;
  asset_id?: string;
  nodeType?: string;
};

const PORT_COLORS: Record<string, string> = {
  video: '#3b82f6',
  audio: '#22c55e',
  image: '#f59e0b',
  subtitle: '#a855f7',
  any_media: '#6b7280',
  search_results: '#f97316',
  url_value: '#14b8a6',
  asset_value: '#e879f9',
};

const TOP_INPUT_PORTS = new Set([
  'subtitle_file',
  'audio',
  'music',
  'overlay',
]);

function shouldRenderInputOnTop(nodeType: string, portName: string) {
  if (nodeType === 'subtitle_to_speech') {
    return portName === 'reference_audio' || portName === 'ref_text';
  }
  if (nodeType === 'concat_vertical_timeline') {
    return portName === 'image_top' || portName === 'image_bottom';
  }
  return TOP_INPUT_PORTS.has(portName);
}

function formatPortLabel(name: string) {
  return name.replace(/_/g, ' ');
}

function ProcessNode({ id, data, selected }: NodeProps) {
  const { nodeTypes } = useNodeTypes();
  const updateNodeInternals = useUpdateNodeInternals();
  const nodeData = (data ?? {}) as ProcessNodeData;
  const typeName = nodeData.nodeType || 'unknown';
  const typeDef = nodeTypes.find(t => t.type_name === typeName);

  const zipChannelCount = getZipChannelCount(nodeData.config);

  const inputs = typeName === 'zip_records'
    ? Array.from({ length: zipChannelCount }, (_, index) => ({
        name: `input_${index + 1}`,
        port_type: 'search_results',
      }))
    : (typeDef?.inputs || []);
  const outputs = typeName === 'zip_records'
    ? Array.from({ length: zipChannelCount }, (_, index) => ({
        name: `output_${index + 1}`,
        port_type: 'url_value',
      }))
    : (typeDef?.outputs || []);
  const icon = typeDef?.icon || '';
  const leftInputs = inputs.filter((port) => !shouldRenderInputOnTop(typeName, port.name));
  const topInputs = inputs.filter((port) => shouldRenderInputOnTop(typeName, port.name));
  const connection = useConnection();
  const showConnectionHints = connection.inProgress;

  useEffect(() => {
    updateNodeInternals(id);
  }, [id, typeName, inputs.length, outputs.length, leftInputs.length, topInputs.length, updateNodeInternals]);

  return (
    <div
      style={{
        position: 'relative',
        background: selected ? '#1e293b' : '#0f172a',
        border: `2px solid ${selected ? '#3b82f6' : '#334155'}`,
        borderRadius: 8,
        padding: '8px 12px',
        minWidth: 150,
        color: '#e2e8f0',
        fontSize: 12,
      }}
    >
      {/* Left input handles */}
      {leftInputs.map((port, i) => (
        <div
          key={`in-wrap-${port.name}`}
          style={{
            position: 'absolute',
            top: `${((i + 1) / (leftInputs.length + 1)) * 100}%`,
            left: 0,
            transform: 'translate(-100%, -50%)',
            display: 'flex',
            alignItems: 'center',
            gap: 6,
            pointerEvents: 'none',
          }}
        >
          {showConnectionHints ? (
            <div
              style={{
                padding: '3px 8px',
                borderRadius: 999,
                background: 'rgba(15, 23, 42, 0.96)',
                border: '1px solid #334155',
                color: '#cbd5e1',
                fontSize: 11,
                whiteSpace: 'nowrap',
              }}
            >
              {formatPortLabel(port.name)}
            </div>
          ) : null}
          <Handle
            type="target"
            position={Position.Left}
            id={port.name}
            style={{
              top: '50%',
              position: 'relative',
              transform: 'translateY(-50%)',
              background: PORT_COLORS[port.port_type] || '#6b7280',
              width: 10,
              height: 10,
              pointerEvents: 'all',
            }}
            title={`${port.name} (${port.port_type})`}
          />
        </div>
      ))}

      {/* Top input handles for auxiliary inputs */}
      {topInputs.map((port, i) => (
        <div
          key={`top-wrap-${port.name}`}
          style={{
            position: 'absolute',
            left: `${((i + 1) / (topInputs.length + 1)) * 100}%`,
            top: 0,
            transform: 'translate(-50%, -100%)',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            gap: 6,
            pointerEvents: 'none',
          }}
        >
          {showConnectionHints ? (
            <div
              style={{
                padding: '3px 8px',
                borderRadius: 999,
                background: 'rgba(15, 23, 42, 0.96)',
                border: '1px solid #334155',
                color: '#cbd5e1',
                fontSize: 11,
                whiteSpace: 'nowrap',
              }}
            >
              {formatPortLabel(port.name)}
            </div>
          ) : null}
          <Handle
            type="target"
            position={Position.Top}
            id={port.name}
            style={{
              position: 'relative',
              left: 'auto',
              top: 'auto',
              background: PORT_COLORS[port.port_type] || '#6b7280',
              width: 10,
              height: 10,
              pointerEvents: 'all',
            }}
            title={`${port.name} (${port.port_type})`}
          />
        </div>
      ))}

      {/* Node content */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
        <span style={{ display: 'inline-flex', alignItems: 'center', justifyContent: 'center' }}>
          <NodeIcon name={icon} size={16} fallback={<span style={{ fontSize: 16, lineHeight: 1 }}>⬡</span>} />
        </span>
        <span style={{ fontWeight: 600 }}>{nodeData.label || typeDef?.display_name || typeName}</span>
      </div>
      <div style={{ color: '#94a3b8', fontSize: 11 }}>
        {typeDef?.category || ''}
      </div>

      {/* Output handles */}
      {outputs.map((port, i) => (
        <Handle
          key={`out-${port.name}`}
          type="source"
          position={Position.Right}
          id={port.name}
          style={{
            top: `${((i + 1) / (outputs.length + 1)) * 100}%`,
            background: PORT_COLORS[port.port_type] || '#6b7280',
            width: 10,
            height: 10,
          }}
          title={`${port.name} (${port.port_type})`}
        />
      ))}
    </div>
  );
}

export default memo(ProcessNode);
