import { useMemo } from 'react';
import { Background, Controls, ReactFlow, type Edge, type Node } from '@xyflow/react';
import '@xyflow/react/dist/style.css';

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function getString(value: unknown, fallback: string) {
  return typeof value === 'string' && value ? value : fallback;
}

function getPosition(value: unknown, index: number) {
  if (isRecord(value) && typeof value.x === 'number' && typeof value.y === 'number') {
    return { x: value.x, y: value.y };
  }
  return { x: (index % 4) * 220, y: Math.floor(index / 4) * 130 };
}

function buildPreviewNodes(definition: Record<string, unknown>): Node<{ label: string; subtitle: string }>[] {
  const rawNodes = Array.isArray(definition.nodes) ? definition.nodes : [];
  return rawNodes.map((rawNode, index) => {
    const node = isRecord(rawNode) ? rawNode : {};
    const data = isRecord(node.data) ? node.data : {};
    const id = getString(node.id, `node-${index + 1}`);
    const nodeType = getString(node.type, getString(data.nodeType, 'process'));
    const label = getString(data.label, nodeType);

    return {
      id,
      type: 'default',
      position: getPosition(node.position, index),
      data: {
        label,
        subtitle: nodeType,
      },
      style: {
        minWidth: 150,
        borderRadius: 8,
        border: '1px solid #334155',
        background: '#0f172a',
        color: '#e2e8f0',
        fontSize: 12,
        padding: 10,
      },
    };
  });
}

function buildPreviewEdges(definition: Record<string, unknown>): Edge[] {
  const rawEdges = Array.isArray(definition.edges) ? definition.edges : [];
  return rawEdges.flatMap((rawEdge, index) => {
    if (!isRecord(rawEdge)) return [];
    const source = getString(rawEdge.source, '');
    const target = getString(rawEdge.target, '');
    if (!source || !target) return [];

    return [{
      id: getString(rawEdge.id, `edge-${index + 1}`),
      source,
      target,
      sourceHandle: typeof rawEdge.sourceHandle === 'string' ? rawEdge.sourceHandle : undefined,
      targetHandle: typeof rawEdge.targetHandle === 'string' ? rawEdge.targetHandle : undefined,
      animated: false,
      style: { stroke: '#3b82f6', strokeWidth: 1.5 },
    }];
  });
}

export default function AutoFlowWorkflowPreview({
  pipelineDefinition,
}: {
  pipelineDefinition: Record<string, unknown> | null;
}) {
  const nodes = useMemo(
    () => (pipelineDefinition ? buildPreviewNodes(pipelineDefinition) : []),
    [pipelineDefinition],
  );
  const edges = useMemo(
    () => (pipelineDefinition ? buildPreviewEdges(pipelineDefinition) : []),
    [pipelineDefinition],
  );

  return (
    <section
      style={{
        backgroundColor: '#0f172a',
        border: '1px solid #1e293b',
        borderRadius: 8,
        padding: 14,
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, marginBottom: 12 }}>
        <h2 style={{ margin: 0, fontSize: 14, color: '#f8fafc' }}>Workflow Preview</h2>
        <div style={{ fontSize: 12, color: '#94a3b8' }}>
          {nodes.length} nodes · {edges.length} edges
        </div>
      </div>

      {nodes.length === 0 ? (
        <div style={{ color: '#94a3b8', fontSize: 13 }}>Generated workflow will appear here.</div>
      ) : (
        <>
          <div
            style={{
              height: 320,
              borderRadius: 8,
              overflow: 'hidden',
              border: '1px solid #1e293b',
              backgroundColor: '#020617',
            }}
          >
            <ReactFlow
              nodes={nodes}
              edges={edges}
              nodesDraggable={false}
              nodesConnectable={false}
              elementsSelectable={false}
              panOnDrag
              zoomOnScroll
              fitView
              proOptions={{ hideAttribution: true }}
              style={{ backgroundColor: '#020617' }}
            >
              <Background color="#1e293b" gap={20} />
              <Controls showInteractive={false} />
            </ReactFlow>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(min(100%, 180px), 1fr))', gap: 12, marginTop: 12 }}>
            <div>
              <div style={{ fontSize: 12, color: '#94a3b8', marginBottom: 6 }}>Nodes</div>
              <div style={{ display: 'grid', gap: 5 }}>
                {nodes.map(node => (
                  <div key={node.id} style={{ fontSize: 12, color: '#cbd5e1', wordBreak: 'break-word' }}>
                    <span style={{ color: '#60a5fa' }}>{node.id}</span> · {node.data.label}
                  </div>
                ))}
              </div>
            </div>
            <div>
              <div style={{ fontSize: 12, color: '#94a3b8', marginBottom: 6 }}>Edges</div>
              <div style={{ display: 'grid', gap: 5 }}>
                {edges.length > 0 ? edges.map(edge => (
                  <div key={edge.id} style={{ fontSize: 12, color: '#cbd5e1', wordBreak: 'break-word' }}>
                    <span style={{ color: '#60a5fa' }}>{edge.source}</span> → <span style={{ color: '#60a5fa' }}>{edge.target}</span>
                  </div>
                )) : (
                  <div style={{ fontSize: 12, color: '#64748b' }}>No edges</div>
                )}
              </div>
            </div>
          </div>
        </>
      )}
    </section>
  );
}
