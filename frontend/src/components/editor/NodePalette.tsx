import useNodeTypes from '../../hooks/useNodeTypes';
import type { NodeTypeInfo } from '../../api/types';
import NodeIcon from './NodeIcon';

interface NodePaletteProps {
  onAddNode: (typeName: string) => void;
}

export default function NodePalette({ onAddNode }: NodePaletteProps) {
  const { nodeTypes, loading } = useNodeTypes();

  if (loading) return <div style={{ padding: 16, color: '#94a3b8' }}>Loading...</div>;

  const categories = nodeTypes.reduce<Record<string, NodeTypeInfo[]>>((acc, nt) => {
    const cat = nt.category || 'Other';
    (acc[cat] = acc[cat] || []).push(nt);
    return acc;
  }, {});

  return (
    <div style={{
      width: 200,
      backgroundColor: '#0f172a',
      borderRight: '1px solid #1e293b',
      overflowY: 'auto',
      padding: '12px 0',
    }}>
      <div style={{ padding: '0 12px 12px', fontSize: 13, fontWeight: 600, color: '#e2e8f0' }}>
        Nodes
      </div>
      {Object.entries(categories).map(([cat, types]) => (
        <div key={cat}>
          <div style={{
            padding: '8px 12px 4px',
            fontSize: 11,
            color: '#64748b',
            textTransform: 'uppercase',
            letterSpacing: '0.05em',
          }}>
            {cat}
          </div>
          {types.map(nt => (
            <div
              key={nt.type_name}
              draggable
              onDragStart={(e) => {
                e.dataTransfer.setData('application/reactflow-type', nt.type_name);
                e.dataTransfer.effectAllowed = 'move';
              }}
              onClick={() => onAddNode(nt.type_name)}
              style={{
                padding: '6px 12px',
                cursor: 'grab',
                display: 'flex',
                alignItems: 'center',
                gap: 8,
                color: '#cbd5e1',
                fontSize: 13,
              }}
              onMouseEnter={e => (e.currentTarget.style.backgroundColor = '#1e293b')}
              onMouseLeave={e => (e.currentTarget.style.backgroundColor = 'transparent')}
              title={nt.description}
            >
              <NodeIcon name={nt.icon} size={14} fallback={<span style={{ width: 16 }} />} />
              <span>{nt.display_name}</span>
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}
