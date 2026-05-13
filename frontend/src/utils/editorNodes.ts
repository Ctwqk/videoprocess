import type { Node } from '@xyflow/react';

import type { NodeTypeInfo } from '../api/types';
import { applyNodeDefaults } from './nodeConfig';

let nodeIdCounter = 0;

export function createEditorNode(
  typeName: string,
  nodeTypes: NodeTypeInfo[],
  position: { x: number; y: number },
): Node {
  const typeDef = nodeTypes.find(type => type.type_name === typeName);
  nodeIdCounter += 1;

  return {
    id: `node_${Date.now()}_${nodeIdCounter}`,
    type: 'processNode',
    position,
    data: {
      label: typeDef?.display_name || typeName,
      config: applyNodeDefaults(typeName, nodeTypes),
      nodeType: typeName,
    },
  };
}
