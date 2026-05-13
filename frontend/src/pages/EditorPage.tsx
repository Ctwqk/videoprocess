import { useCallback, useRef, useEffect, useMemo } from 'react';
import { useParams } from 'react-router-dom';
import { ReactFlow, Background, Controls, MiniMap } from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import useEditorStore from '../store/editorStore';
import useNodeTypes from '../hooks/useNodeTypes';
import apiClient from '../api/client';

import ProcessNode from '../components/editor/ProcessNode';
import NodePalette from '../components/editor/NodePalette';
import ConfigPanel from '../components/editor/ConfigPanel';
import EditorToolbar from '../components/editor/EditorToolbar';
import { createEditorNode } from '../utils/editorNodes';

export default function EditorPage() {
  const { pipelineId: routePipelineId } = useParams<{ pipelineId?: string }>();
  const {
    nodes, edges, onNodesChange, onEdgesChange, onConnect,
    addNode, setSelectedNodeId, setPipeline, clear,
  } = useEditorStore();
  const { nodeTypes } = useNodeTypes();
  const reactFlowWrapper = useRef<HTMLDivElement>(null);

  const rfNodeTypes = useMemo(() => ({ processNode: ProcessNode }), []);

  // Load pipeline from route
  useEffect(() => {
    if (routePipelineId) {
      apiClient.get(`/pipelines/${routePipelineId}`).then(res => {
        const p = res.data;
        const def = p.definition;
        const loadedNodes = (def.nodes || []).map((n: Record<string, unknown>) => ({
          id: n.id as string,
          type: 'processNode',
          position: n.position as { x: number; y: number },
          data: {
            ...(n.data as Record<string, unknown>),
            nodeType: n.type as string,
          },
        }));
        setPipeline(p.id, p.name, loadedNodes, def.edges || []);
      }).catch(() => {
        // Pipeline not found, start fresh
        clear();
      });
    }
  }, [routePipelineId, setPipeline, clear]);

  const handleAddNode = useCallback((typeName: string) => {
    addNode(createEditorNode(
      typeName,
      nodeTypes,
      { x: 250 + Math.random() * 200, y: 100 + Math.random() * 200 },
    ));
  }, [nodeTypes, addNode]);

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    const typeName = e.dataTransfer.getData('application/reactflow-type');
    if (!typeName) return;

    // Get position relative to the React Flow canvas
    const bounds = reactFlowWrapper.current?.getBoundingClientRect();
    const position = {
      x: e.clientX - (bounds?.left || 0),
      y: e.clientY - (bounds?.top || 0),
    };

    addNode(createEditorNode(typeName, nodeTypes, position));
  }, [nodeTypes, addNode]);

  const onNodeClick = useCallback((_: React.MouseEvent, node: { id: string }) => {
    setSelectedNodeId(node.id);
  }, [setSelectedNodeId]);

  const onPaneClick = useCallback(() => {
    setSelectedNodeId(null);
  }, [setSelectedNodeId]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <EditorToolbar />
      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        <NodePalette onAddNode={handleAddNode} />
        <div ref={reactFlowWrapper} style={{ flex: 1 }} onDragOver={onDragOver} onDrop={onDrop}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onNodeClick={onNodeClick}
            onPaneClick={onPaneClick}
            nodeTypes={rfNodeTypes}
            deleteKeyCode={["Backspace", "Delete"]}
            fitView
            style={{ backgroundColor: '#020617' }}
          >
            <Background color="#1e293b" gap={20} />
            <Controls />
            <MiniMap
              nodeColor={() => '#3b82f6'}
              maskColor="rgba(0,0,0,0.7)"
              style={{ backgroundColor: '#0f172a' }}
            />
          </ReactFlow>
        </div>
        <ConfigPanel />
      </div>
    </div>
  );
}
