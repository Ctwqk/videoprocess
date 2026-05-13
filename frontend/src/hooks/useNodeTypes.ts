import { useState, useEffect } from 'react';
import apiClient from '../api/client';
import type { NodeTypeInfo } from '../api/types';

let cachedNodeTypes: NodeTypeInfo[] | null = null;

export default function useNodeTypes() {
  const [nodeTypes, setNodeTypes] = useState<NodeTypeInfo[]>(cachedNodeTypes || []);
  const [loading, setLoading] = useState(!cachedNodeTypes);

  useEffect(() => {
    if (cachedNodeTypes) return;
    apiClient.get<NodeTypeInfo[]>('/node-types').then(res => {
      cachedNodeTypes = res.data;
      setNodeTypes(res.data);
      setLoading(false);
    });
  }, []);

  return { nodeTypes, loading };
}
