import React from 'react';
import * as LucideIcons from 'lucide-react';

export default function NodeIcon({
  name,
  size = 16,
  fallback,
}: {
  name: string;
  size?: number;
  fallback?: React.ReactNode;
}) {
  const key = name
    .split('-')
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join('');
  const Icon = (LucideIcons as unknown as Record<string, React.ComponentType<{ size?: number }>>)[key];
  if (!Icon) {
    return <>{fallback ?? <span style={{ width: size }} />}</>;
  }
  return <Icon size={size} />;
}

