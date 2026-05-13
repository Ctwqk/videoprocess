import type { Edge, Node } from '@xyflow/react';

import type { NodeTypeInfo, PipelineDefinition } from '../api/types';
import { applyNodeDefaults } from './nodeConfig';

type BuildPipelineDefinitionOptions = {
  applyDefaults?: boolean;
  nodeTypes?: NodeTypeInfo[];
};

export function buildPipelineDefinition(
  nodes: Node[],
  edges: Edge[],
  options?: BuildPipelineDefinitionOptions,
): PipelineDefinition {
  const shouldApplyDefaults = options?.applyDefaults === true;
  const nodeTypes = options?.nodeTypes || [];

  return {
    nodes: nodes.map((node) => {
      const nodeTypeName = ((node.data.nodeType as string | undefined) || node.type || '');
      const rawConfig = ((node.data.config as Record<string, unknown>) || {});
      const config = shouldApplyDefaults
        ? applyNodeDefaults(nodeTypeName, nodeTypes, rawConfig)
        : { ...rawConfig };

      return {
        id: node.id,
        type: nodeTypeName,
        position: node.position,
        data: {
          label: (node.data.label as string) || '',
          config,
          asset_id: (config.asset_id as string) || undefined,
        },
      };
    }),
    edges: edges.map((edge) => ({
      id: edge.id,
      source: edge.source,
      target: edge.target,
      sourceHandle: edge.sourceHandle || 'output',
      targetHandle: edge.targetHandle || 'input',
    })),
    viewport: { x: 0, y: 0, zoom: 1 },
  };
}
