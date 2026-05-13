import type { NodeTypeInfo } from '../api/types';

export function applyNodeDefaults(
  nodeTypeName: string,
  nodeTypes: NodeTypeInfo[],
  config?: Record<string, unknown>,
): Record<string, unknown> {
  const mergedConfig = { ...(config || {}) };
  const nodeType = nodeTypes.find(type => type.type_name === nodeTypeName);
  if (!nodeType) {
    return mergedConfig;
  }

  for (const param of nodeType.params) {
    if (mergedConfig[param.name] === undefined || mergedConfig[param.name] === null) {
      mergedConfig[param.name] = param.default;
    }
  }

  return mergedConfig;
}
