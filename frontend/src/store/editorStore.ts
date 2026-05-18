import { create } from 'zustand';
import { createJSONStorage, persist } from 'zustand/middleware';
import {
  type Node,
  type Edge,
  type OnNodesChange,
  type OnEdgesChange,
  type OnConnect,
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
} from '@xyflow/react';

interface EditorState {
  nodes: Node[];
  edges: Edge[];
  selectedNodeId: string | null;
  pipelineId: string | null;
  pipelineName: string;
  isDirty: boolean;
  onNodesChange: OnNodesChange;
  onEdgesChange: OnEdgesChange;
  onConnect: OnConnect;
  addNode: (node: Node) => void;
  setSelectedNodeId: (id: string | null) => void;
  updateNodeConfig: (nodeId: string, config: Record<string, unknown>) => void;
  updateNodeLabel: (nodeId: string, label: string) => void;
  removeNode: (nodeId: string) => void;
  setPipeline: (id: string | null, name: string, nodes: Node[], edges: Edge[]) => void;
  setPipelineName: (name: string) => void;
  clear: () => void;
}

const DYNAMIC_CONCAT_NODE_TYPES = new Set(['concat_timeline', 'concat_many']);
const VIDEO_INPUT_HANDLE_RE = /^video_(\d+)$/;

function getNodeType(node: Node) {
  const data = node.data as { nodeType?: unknown } | undefined;
  return typeof data?.nodeType === 'string' ? data.nodeType : String(node.type || '');
}

function getNodeConfig(node: Node): Record<string, unknown> {
  const data = node.data as { config?: unknown } | undefined;
  return typeof data?.config === 'object' && data.config !== null
    ? data.config as Record<string, unknown>
    : {};
}

function numericInputCount(value: unknown) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.max(2, Math.floor(parsed)) : 2;
}

function maybeExpandDynamicConcatInput(node: Node, targetNodeId: string | null | undefined, targetHandle: string | null | undefined) {
  if (node.id !== targetNodeId || !targetHandle || !DYNAMIC_CONCAT_NODE_TYPES.has(getNodeType(node))) {
    return node;
  }
  const match = VIDEO_INPUT_HANDLE_RE.exec(targetHandle);
  if (!match) return node;

  const connectedIndex = Number(match[1]);
  const config = getNodeConfig(node);
  const currentCount = numericInputCount(config.input_count);
  const nextCount = Math.max(currentCount, connectedIndex + 1);
  if (nextCount === currentCount) return node;

  return {
    ...node,
    data: {
      ...node.data,
      config: {
        ...config,
        input_count: nextCount,
      },
    },
  };
}

const useEditorStore = create<EditorState>()(persist((set, get) => ({
  nodes: [],
  edges: [],
  selectedNodeId: null,
  pipelineId: null,
  pipelineName: 'Untitled Pipeline',
  isDirty: false,

  onNodesChange: (changes) => {
    set({
      nodes: applyNodeChanges(changes, get().nodes),
      isDirty: true,
    });
  },

  onEdgesChange: (changes) => {
    set({
      edges: applyEdgeChanges(changes, get().edges),
      isDirty: true,
    });
  },

  onConnect: (connection) => {
    set({
      edges: addEdge(connection, get().edges),
      nodes: get().nodes.map(node => maybeExpandDynamicConcatInput(node, connection.target, connection.targetHandle)),
      isDirty: true,
    });
  },

  addNode: (node) => {
    set({
      nodes: [...get().nodes, node],
      isDirty: true,
    });
  },

  setSelectedNodeId: (id) => set({ selectedNodeId: id }),

  updateNodeConfig: (nodeId, config) => {
    set({
      nodes: get().nodes.map(n =>
        n.id === nodeId
          ? (() => {
              const nextConfig = { ...(n.data.config as Record<string, unknown>), ...config };
              for (const [key, value] of Object.entries(config)) {
                if (value === undefined) {
                  delete nextConfig[key];
                }
              }
              return { ...n, data: { ...n.data, config: nextConfig } };
            })()
          : n
      ),
      isDirty: true,
    });
  },

  updateNodeLabel: (nodeId, label) => {
    set({
      nodes: get().nodes.map(n =>
        n.id === nodeId
          ? { ...n, data: { ...n.data, label } }
          : n
      ),
      isDirty: true,
    });
  },

  removeNode: (nodeId) => {
    set({
      nodes: get().nodes.filter(n => n.id !== nodeId),
      edges: get().edges.filter(e => e.source !== nodeId && e.target !== nodeId),
      selectedNodeId: get().selectedNodeId === nodeId ? null : get().selectedNodeId,
      isDirty: true,
    });
  },

  setPipeline: (id, name, nodes, edges) => {
    set({
      pipelineId: id,
      pipelineName: name,
      nodes,
      edges,
      isDirty: false,
      selectedNodeId: null,
    });
  },

  setPipelineName: (name) => set({ pipelineName: name, isDirty: true }),

  clear: () => set({
    nodes: [],
    edges: [],
    selectedNodeId: null,
    pipelineId: null,
    pipelineName: 'Untitled Pipeline',
    isDirty: false,
  }),
}), {
  name: 'videoprocess-editor-draft',
  storage: createJSONStorage(() => sessionStorage),
  partialize: (state) => ({
    nodes: state.nodes,
    edges: state.edges,
    selectedNodeId: state.selectedNodeId,
    pipelineId: state.pipelineId,
    pipelineName: state.pipelineName,
    isDirty: state.isDirty,
  }),
}));

export default useEditorStore;
