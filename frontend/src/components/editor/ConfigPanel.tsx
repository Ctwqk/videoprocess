import { useState, useEffect, type CSSProperties } from 'react';
import useEditorStore from '../../store/editorStore';
import useNodeTypes from '../../hooks/useNodeTypes';
import apiClient from '../../api/client';
import type { Asset, MaterialLibrary, ParamDefinition, PipelineDefinition } from '../../api/types';
import type { PlannerSearchResult } from '../../utils/plannerBatch';
import { getSelectedPlannerResultIds, getZipConnectionSummary } from '../../utils/plannerBatch';
import { formatFileSize } from '../../utils/fileSize';
import { buildPipelineDefinition } from '../../utils/pipelineDefinition';

type PlannerNodeSearchResult = PlannerSearchResult;
type SourceMediaKind = 'video' | 'audio' | 'subtitle' | 'image';
type RemoteSearchNodeType = 'youtube_search' | 'x_search' | 'xiaohongshu_search' | 'bilibili_search';

const REMOTE_SEARCH_NODE_CONFIG: Record<RemoteSearchNodeType, {
  platform: 'youtube' | 'x' | 'xiaohongshu' | 'bilibili';
  label: string;
  placeholder: string;
  description: string;
  buttonLabel: string;
  endpoint: string;
}> = {
  youtube_search: {
    platform: 'youtube',
    label: 'YouTube',
    placeholder: 'Search YouTube videos',
    description: 'Search YouTube, then select which videos this channel should contribute to batch records.',
    buttonLabel: 'Search YouTube',
    endpoint: '/youtube/api/search',
  },
  x_search: {
    platform: 'x',
    label: 'X',
    placeholder: 'Search X posts',
    description: 'Search X with the attached browser session, then select which posts should contribute to batch records.',
    buttonLabel: 'Search X',
    endpoint: '/platforms/api/platforms/x/search',
  },
  xiaohongshu_search: {
    platform: 'xiaohongshu',
    label: 'Xiaohongshu',
    placeholder: 'Search Xiaohongshu posts',
    description: 'Search Xiaohongshu with the logged-in browser session, then select which posts should contribute to batch records.',
    buttonLabel: 'Search Xiaohongshu',
    endpoint: '/platforms/api/platforms/xiaohongshu/search',
  },
  bilibili_search: {
    platform: 'bilibili',
    label: 'Bilibili',
    placeholder: 'Search Bilibili videos',
    description: 'Search Bilibili with the logged-in browser session, then select which videos should contribute to batch records.',
    buttonLabel: 'Search Bilibili',
    endpoint: '/platforms/api/platforms/bilibili/search',
  },
};

const SOURCE_MEDIA_OPTIONS: Array<{ value: SourceMediaKind; label: string }> = [
  { value: 'video', label: 'Video' },
  { value: 'audio', label: 'Audio' },
  { value: 'subtitle', label: 'Subtitle' },
  { value: 'image', label: 'Image' },
];

export default function ConfigPanel() {
  const { nodes, edges, selectedNodeId, updateNodeConfig, updateNodeLabel, removeNode } = useEditorStore();
  const { nodeTypes } = useNodeTypes();
  const [assets, setAssets] = useState<Asset[]>([]);
  const [materialLibraries, setMaterialLibraries] = useState<MaterialLibrary[]>([]);
  const [minimaxModels, setMinimaxModels] = useState<string[]>([]);
  const [minimaxBlankLabel, setMinimaxBlankLabel] = useState('Select MiniMax model');
  const [minimaxLoading, setMinimaxLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<PlannerNodeSearchResult[]>([]);
  const [selectedVideoIds, setSelectedVideoIds] = useState<string[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [channelFilter, setChannelFilter] = useState('');
  const [durationFilter, setDurationFilter] = useState<'any' | 'short' | 'medium' | 'long'>('any');

  const node = nodes.find(n => n.id === selectedNodeId);
  const typeDef = node ? nodeTypes.find(t => t.type_name === (node.data.nodeType as string || node.type)) : null;

  useEffect(() => {
    apiClient.get('/assets?limit=500').then(res => setAssets(res.data.items || [])).catch(() => {});
    apiClient.get('/material-libraries?limit=200').then(res => setMaterialLibraries(res.data.items || [])).catch(() => {});
  }, []);

  useEffect(() => {
    if (node?.data.nodeType !== 'subtitle_translate') {
      return;
    }

    let cancelled = false;

    setMinimaxLoading(true);
    fetch('/api/v1/llm/provider-models?provider_config_id=minimax')
      .then(async (response) => {
        if (!response.ok) {
          throw new Error(`Failed to load MiniMax models (${response.status})`);
        }
        return response.json() as Promise<{ models?: string[]; blank_label?: string }>;
      })
      .then((payload) => {
        if (cancelled) {
          return;
        }
        setMinimaxModels(Array.isArray(payload.models) ? payload.models.map(String) : []);
        setMinimaxBlankLabel(payload.blank_label || 'Select MiniMax model');
      })
      .catch(() => {
        if (cancelled) {
          return;
        }
        setMinimaxModels([]);
        setMinimaxBlankLabel('Select MiniMax model');
      })
      .finally(() => {
        if (!cancelled) {
          setMinimaxLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [node?.id, node?.data.nodeType]);

  useEffect(() => {
    setSearchResults([]);
    setSelectedVideoIds([]);
    setSearchError(null);
    setSearchLoading(false);
    setChannelFilter('');
    setDurationFilter('any');

    if (isRemoteSearchNodeType(node?.data.nodeType) || node?.data.nodeType === 'material_search') {
      const nodeConfig = (node.data.config as Record<string, unknown> | undefined) || {};
      setSearchQuery(String(nodeConfig.query || ''));
      setSearchResults(Array.isArray(nodeConfig.search_results) ? nodeConfig.search_results as PlannerNodeSearchResult[] : []);
      setSelectedVideoIds(getSelectedPlannerResultIds(nodeConfig));
      return;
    }

    setSearchQuery('');
  }, [node?.id, node?.data.nodeType]);

  if (!node || !typeDef) {
    return (
      <div style={emptyStyle}>
        Select a node to configure
      </div>
    );
  }

  const config = (node.data.config as Record<string, unknown>) || {};
  const sourceMediaType = normalizeSourceMediaType(config.media_type);
  const sourceAssets = assets.filter(asset => inferAssetKind(asset) === sourceMediaType);
  const selectedSourceAsset = sourceAssets.find(asset => asset.id === config.asset_id) ?? null;
  const currentDefinition: PipelineDefinition = buildPipelineDefinition(nodes, edges);

  const handleChange = (name: string, value: unknown) => {
    updateNodeConfig(node.id, { [name]: value });
  };

  const handleSearch = async () => {
    const query = searchQuery.trim();
    if (!query) {
      setSearchError('Enter a search query first');
      setSearchResults([]);
      return;
    }

    if (!isRemoteSearchNodeType(node?.data.nodeType)) {
      return;
    }

    const maxResults = Number(config.max_results || 8);
    const searchConfig = REMOTE_SEARCH_NODE_CONFIG[node.data.nodeType];

    try {
      setSearchLoading(true);
      setSearchError(null);
      const response = await fetch(searchConfig.endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query, max_results: maxResults }),
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => null);
        throw new Error(payload?.detail || `Search failed with status ${response.status}`);
      }
      const payload = await response.json() as { results?: PlannerNodeSearchResult[] };
      const nextResults = ensurePlannerResultPlatform(payload.results || [], searchConfig.platform);
      const nextResultIds = new Set(nextResults.map(result => result.id));
      const preservedIds = selectedVideoIds.filter(id => nextResultIds.has(id));
      const nextSelectedIds = preservedIds.length > 0 ? preservedIds : nextResults.map(result => result.id);
      setSearchResults(nextResults);
      setSelectedVideoIds(nextSelectedIds);
      updateNodeConfig(node.id, {
        query,
        search_results: nextResults,
        selected_result_ids: nextSelectedIds,
        selected_video_ids: undefined,
        selected_material_result_ids: undefined,
      });
    } catch (error) {
      setSearchError(error instanceof Error ? error.message : `${searchConfig.label} search failed`);
      setSearchResults([]);
    } finally {
      setSearchLoading(false);
    }
  };

  const handleMaterialSearch = async () => {
    const query = searchQuery.trim();
    if (!query) {
      setSearchError('Enter a material search query first');
      setSearchResults([]);
      return;
    }

    const sourceLibraryIds = Array.isArray(config.source_library_ids) ? config.source_library_ids.map(String) : [];
    const resultLibraryIds = Array.isArray(config.result_library_ids) ? config.result_library_ids.map(String) : [];
    if (sourceLibraryIds.length === 0) {
      setSearchError('Select at least one source library');
      setSearchResults([]);
      return;
    }

    try {
      setSearchLoading(true);
      setSearchError(null);
      const response = await fetch('/api/v1/material-search/materialize', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query,
          source_library_ids: sourceLibraryIds,
          result_library_ids: resultLibraryIds,
          top_k: Number(config.top_k || 50),
          merge_gap: Number(config.merge_gap || 5),
          expand_left: Number(config.expand_left || 4),
          expand_right: Number(config.expand_right || 4),
          rerank_top_m: Number(config.rerank_top_m || 8),
          min_duration: Number(config.min_duration || 1.5),
          max_duration: Number(config.max_duration || 20),
          dedupe_overlap_threshold: Number(config.dedupe_overlap_threshold || 0.6),
        }),
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => null);
        throw new Error(payload?.detail || `Material search failed with status ${response.status}`);
      }
      const payload = await response.json() as { results?: PlannerNodeSearchResult[] };
      const nextResults = ensurePlannerResultPlatform(payload.results || [], 'material');
      const nextSelectedIds = nextResults.map(result => result.id);
      setSearchResults(nextResults);
      setSelectedVideoIds(nextSelectedIds);
      updateNodeConfig(node.id, {
        query,
        search_results: nextResults,
        selected_result_ids: nextSelectedIds,
        selected_video_ids: undefined,
        selected_material_result_ids: undefined,
      });
    } catch (error) {
      setSearchError(error instanceof Error ? error.message : 'Material search failed');
      setSearchResults([]);
    } finally {
      setSearchLoading(false);
    }
  };

  const filteredSearchResults = searchResults.filter(result => {
    const channel = result.channel?.toLowerCase() || '';
    const channelNeedle = channelFilter.trim().toLowerCase();
    if (channelNeedle && !channel.includes(channelNeedle)) {
      return false;
    }

    const duration = result.duration || 0;
    if (durationFilter === 'short' && duration > 4 * 60) {
      return false;
    }
    if (durationFilter === 'medium' && (duration <= 4 * 60 || duration > 20 * 60)) {
      return false;
    }
    if (durationFilter === 'long' && duration <= 20 * 60) {
      return false;
    }
    return true;
  });

  const toggleSelectedVideo = (videoId: string) => {
    const next = selectedVideoIds.includes(videoId)
      ? selectedVideoIds.filter(id => id !== videoId)
      : [...selectedVideoIds, videoId];
    setSelectedVideoIds(next);
    updateNodeConfig(node.id, {
      selected_result_ids: next,
      selected_video_ids: undefined,
      selected_material_result_ids: undefined,
    });
  };

  const selectVisibleVideos = () => {
    const next = filteredSearchResults.map(result => result.id);
    setSelectedVideoIds(next);
    updateNodeConfig(node.id, {
      selected_result_ids: next,
      selected_video_ids: undefined,
      selected_material_result_ids: undefined,
    });
  };

  const clearSelectedVideos = () => {
    setSelectedVideoIds([]);
    updateNodeConfig(node.id, {
      selected_result_ids: [],
      selected_video_ids: undefined,
      selected_material_result_ids: undefined,
    });
  };

  const remoteSearchConfig = isRemoteSearchNodeType(node.data.nodeType)
    ? REMOTE_SEARCH_NODE_CONFIG[node.data.nodeType]
    : null;

  return (
    <div style={panelStyle}>
      <div style={{ marginBottom: 16 }}>
        <label style={labelStyle}>Label</label>
        <input
          value={(node.data.label as string) || ''}
          onChange={e => updateNodeLabel(node.id, e.target.value)}
          style={inputStyle}
        />
      </div>

      <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 12 }}>
        {typeDef.icon} {typeDef.display_name}
      </div>

      {node.data.nodeType === 'source' && (
        <div style={cardStyle}>
          <div style={{ fontSize: 11, color: '#93c5fd', fontWeight: 700, marginBottom: 8 }}>
            Source Input
          </div>
          <div style={{ fontSize: 11, color: '#64748b', marginBottom: 8 }}>
            Choose what kind of asset this node should output, then pick one uploaded file of that type.
          </div>

          <div style={{ marginBottom: 12 }}>
            <label style={labelStyle}>Source type</label>
            <select
              value={sourceMediaType}
              onChange={e => {
                const nextType = e.target.value as SourceMediaKind;
                const nextAssets = assets.filter(asset => inferAssetKind(asset) === nextType);
                const selectedStillMatches = nextAssets.some(asset => asset.id === config.asset_id);
                updateNodeConfig(node.id, {
                  media_type: nextType,
                  asset_id: selectedStillMatches ? config.asset_id : '',
                });
              }}
              style={inputStyle}
            >
              {SOURCE_MEDIA_OPTIONS.map(option => (
                <option key={option.value} value={option.value}>{option.label}</option>
              ))}
            </select>
          </div>

          <div style={{ marginBottom: 12 }}>
            <label style={labelStyle}>Asset</label>
            <select
              value={(config.asset_id as string) || ''}
              onChange={e => handleChange('asset_id', e.target.value)}
              style={inputStyle}
            >
              <option value="">{sourceAssets.length > 0 ? '-- Select asset --' : '-- No matching assets --'}</option>
              {sourceAssets.map(asset => (
                <option key={asset.id} value={asset.id}>
                  {formatAssetOption(asset)}
                </option>
              ))}
            </select>
          </div>

          <div style={{ fontSize: 11, color: '#94a3b8' }}>
            {selectedSourceAsset
              ? `Selected: ${selectedSourceAsset.original_name}`
              : sourceAssets.length > 0
                ? `${sourceAssets.length} matching assets available`
                : `No ${sourceMediaType} assets uploaded yet`}
          </div>
        </div>
      )}

      {remoteSearchConfig && (
        <div style={cardStyle}>
          <div style={{ fontSize: 11, color: '#93c5fd', fontWeight: 700, marginBottom: 8 }}>
            Search and Select
          </div>
          <div style={{ fontSize: 11, color: '#64748b', marginBottom: 8 }}>
            {remoteSearchConfig.description}
          </div>
          <input
            value={searchQuery}
            onChange={e => {
              setSearchQuery(e.target.value);
              handleChange('query', e.target.value);
            }}
            placeholder={remoteSearchConfig.placeholder}
            style={{ ...inputStyle, marginBottom: 8 }}
          />
          <button
            type="button"
            onClick={() => void handleSearch()}
            disabled={searchLoading}
            style={primaryButtonStyle}
          >
            {searchLoading ? 'Searching...' : remoteSearchConfig.buttonLabel}
          </button>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, marginTop: 8 }}>
            <button type="button" onClick={selectVisibleVideos} style={smallButtonStyle}>
              Select Visible
            </button>
            <button type="button" onClick={clearSelectedVideos} style={smallButtonStyle}>
              Clear
            </button>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 110px', gap: 8, marginTop: 8 }}>
            <input
              value={channelFilter}
              onChange={e => setChannelFilter(e.target.value)}
              placeholder="Filter by channel"
              style={{ ...inputStyle, fontSize: 12, backgroundColor: '#0f172a' }}
            />
            <select
              value={durationFilter}
              onChange={e => setDurationFilter(e.target.value as 'any' | 'short' | 'medium' | 'long')}
              style={{ ...inputStyle, fontSize: 12, backgroundColor: '#0f172a' }}
            >
              <option value="any">Any length</option>
              <option value="short">Short</option>
              <option value="medium">Medium</option>
              <option value="long">Long</option>
            </select>
          </div>
          {searchError ? (
            <div style={{ fontSize: 11, color: '#fca5a5', marginTop: 8 }}>{searchError}</div>
          ) : null}
          {searchResults.length > 0 ? (
            <div style={{ marginTop: 10, display: 'grid', gap: 8 }}>
              <div style={{ fontSize: 11, color: '#64748b' }}>
                Showing {filteredSearchResults.length} of {searchResults.length} results · selected {selectedVideoIds.length}
              </div>
              {filteredSearchResults.map(result => (
                <div
                  key={result.id}
                  style={{
                    padding: 10,
                    borderRadius: 6,
                    border: '1px solid #334155',
                    backgroundColor: '#0f172a',
                    color: '#e2e8f0',
                  }}
                >
                  <label style={{ display: 'grid', gridTemplateColumns: '20px 96px 1fr', gap: 10, alignItems: 'start', cursor: 'pointer' }}>
                    <input
                      type="checkbox"
                      checked={selectedVideoIds.includes(result.id)}
                      onChange={() => toggleSelectedVideo(result.id)}
                      style={{ marginTop: 4 }}
                    />
                    <div style={thumbnailWrapStyle}>
                      {result.thumbnail ? (
                        <img
                          src={result.thumbnail}
                          alt={result.title}
                          style={{ width: '100%', height: '100%', objectFit: 'cover', display: 'block' }}
                        />
                      ) : null}
                    </div>
                    <div>
                      <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 4 }}>
                        {result.title}
                      </div>
                      <div style={{ fontSize: 11, color: '#94a3b8', marginBottom: 4 }}>
                        {[result.channel, formatDuration(result.duration)].filter(Boolean).join(' · ')}
                      </div>
                      <div style={{ fontSize: 10, color: '#60a5fa', wordBreak: 'break-all' }}>
                        {result.url}
                      </div>
                    </div>
                  </label>
                </div>
              ))}
              {filteredSearchResults.length === 0 ? (
                <div style={{ fontSize: 11, color: '#94a3b8' }}>
                  No results match the current filters.
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      )}

      {node.data.nodeType === 'material_search' && (
        <div style={cardStyle}>
          <div style={{ fontSize: 11, color: '#93c5fd', fontWeight: 700, marginBottom: 8 }}>
            Material Search
          </div>
          <div style={{ fontSize: 11, color: '#64748b', marginBottom: 8 }}>
            Search one or more material libraries, materialize refined clip assets, then select which results should feed batch records.
          </div>
          <input
            value={searchQuery}
            onChange={e => {
              setSearchQuery(e.target.value);
              handleChange('query', e.target.value);
            }}
            placeholder="Describe the clip you want"
            style={{ ...inputStyle, marginBottom: 8 }}
          />
          <div style={{ display: 'grid', gap: 8, marginBottom: 10 }}>
            <div>
              <div style={labelStyle}>Search libraries</div>
              <div style={libraryListStyle}>
                {materialLibraries.map(library => {
                  const selected = Array.isArray(config.source_library_ids) && config.source_library_ids.map(String).includes(library.id);
                  return (
                    <label key={`source-lib-${library.id}`} style={libraryRowStyle}>
                      <input
                        type="checkbox"
                        checked={selected}
                        onChange={e => {
                          const current = Array.isArray(config.source_library_ids) ? config.source_library_ids.map(String) : [];
                          const next = e.target.checked
                            ? [...new Set([...current, library.id])]
                            : current.filter(id => id !== library.id);
                          handleChange('source_library_ids', next);
                        }}
                      />
                      <span>{library.name}</span>
                    </label>
                  );
                })}
              </div>
            </div>
            <div>
              <div style={labelStyle}>Save refined clips to</div>
              <div style={libraryListStyle}>
                {materialLibraries.map(library => {
                  const selected = Array.isArray(config.result_library_ids) && config.result_library_ids.map(String).includes(library.id);
                  return (
                    <label key={`result-lib-${library.id}`} style={libraryRowStyle}>
                      <input
                        type="checkbox"
                        checked={selected}
                        onChange={e => {
                          const current = Array.isArray(config.result_library_ids) ? config.result_library_ids.map(String) : [];
                          const next = e.target.checked
                            ? [...new Set([...current, library.id])]
                            : current.filter(id => id !== library.id);
                          handleChange('result_library_ids', next);
                        }}
                      />
                      <span>{library.name}</span>
                    </label>
                  );
                })}
              </div>
            </div>
          </div>
          <button
            type="button"
            onClick={() => void handleMaterialSearch()}
            disabled={searchLoading}
            style={primaryButtonStyle}
          >
            {searchLoading ? 'Searching...' : 'Search Material Library'}
          </button>
          {searchError ? (
            <div style={{ fontSize: 11, color: '#fca5a5', marginTop: 8 }}>{searchError}</div>
          ) : null}
          {searchResults.length > 0 ? (
            <div style={{ marginTop: 10, display: 'grid', gap: 8 }}>
              <div style={{ fontSize: 11, color: '#64748b' }}>
                Showing {searchResults.length} results · selected {selectedVideoIds.length}
              </div>
              {searchResults.map(result => (
                <div
                  key={result.id}
                  style={{
                    padding: 10,
                    borderRadius: 6,
                    border: '1px solid #334155',
                    backgroundColor: '#0f172a',
                    color: '#e2e8f0',
                  }}
                >
                  <label style={{ display: 'grid', gridTemplateColumns: '20px 1fr', gap: 10, alignItems: 'start', cursor: 'pointer' }}>
                    <input
                      type="checkbox"
                      checked={selectedVideoIds.includes(result.id)}
                      onChange={() => toggleSelectedVideo(result.id)}
                      style={{ marginTop: 4 }}
                    />
                    <div>
                      <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 4 }}>
                        {result.title}
                      </div>
                      <div style={{ fontSize: 11, color: '#94a3b8', marginBottom: 4 }}>
                        {result.start_sec != null && result.end_sec != null ? `${Number(result.start_sec).toFixed(1)}s → ${Number(result.end_sec).toFixed(1)}s` : ''}
                      </div>
                      {result.subtitle_text ? (
                        <div style={{ fontSize: 11, color: '#cbd5e1' }}>
                          {String(result.subtitle_text).slice(0, 180)}
                        </div>
                      ) : null}
                    </div>
                  </label>
                </div>
              ))}
            </div>
          ) : null}
        </div>
      )}

      {node.data.nodeType === 'material_library_ingest' && (
        <div style={cardStyle}>
          <div style={{ fontSize: 11, color: '#93c5fd', fontWeight: 700, marginBottom: 8 }}>
            Material Library Target
          </div>
          <div style={{ fontSize: 11, color: '#64748b', marginBottom: 8 }}>
            Select which material libraries this source video should be sliced and indexed into.
          </div>
          <div style={libraryListStyle}>
            {materialLibraries.map(library => {
              const selected = Array.isArray(config.target_library_ids) && config.target_library_ids.map(String).includes(library.id);
              return (
                <label key={`ingest-lib-${library.id}`} style={libraryRowStyle}>
                  <input
                    type="checkbox"
                    checked={selected}
                    onChange={e => {
                      const current = Array.isArray(config.target_library_ids) ? config.target_library_ids.map(String) : [];
                      const next = e.target.checked
                        ? [...new Set([...current, library.id])]
                        : current.filter(id => id !== library.id);
                      handleChange('target_library_ids', next);
                    }}
                  />
                  <span>{library.name}</span>
                </label>
              );
            })}
          </div>
        </div>
      )}

      {node.data.nodeType === 'zip_records' && (
        <div style={cardStyle}>
          <div style={{ fontSize: 11, color: '#c4b5fd', fontWeight: 700, marginBottom: 8 }}>
            Zip Summary
          </div>
          <div style={{ fontSize: 11, color: '#64748b', marginBottom: 8 }}>
            Output records use the shortest selected search channel and respect record_limit when set.
          </div>
          <div style={{ display: 'grid', gap: 8 }}>
            {getZipConnectionSummary(currentDefinition, node.id).map(summary => (
              <div key={summary.channel} style={summaryCardStyle}>
                <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 4 }}>
                  Channel {summary.channel}
                </div>
                <div style={{ fontSize: 11, color: '#94a3b8' }}>
                  Search: {summary.searchLabel || 'unconnected'}
                </div>
                <div style={{ fontSize: 11, color: '#94a3b8' }}>
                  Selected videos: {summary.selectedCount}
                </div>
                <div style={{ fontSize: 11, color: '#94a3b8' }}>
                  URL Download target: {summary.downloadLabel || 'unconnected'}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {node.data.nodeType === 'url_download' && (
        <div style={cardStyle}>
          <div style={{ fontSize: 11, color: '#60a5fa', fontWeight: 700, marginBottom: 6 }}>
            URL Download
          </div>
          <div style={{ fontSize: 11, color: '#64748b' }}>
            Use the URL field directly, or connect a Zip Records output into this node.
          </div>
        </div>
      )}

      {node.data.nodeType === 'subtitle_translate' && (
        <div style={{ marginBottom: 12 }}>
          <label style={labelStyle}>model</label>
          <select
            value={String(config.model || '')}
            onChange={e => handleChange('model', e.target.value)}
            style={inputStyle}
          >
            <option value="">
              {minimaxLoading ? 'Loading MiniMax models...' : minimaxBlankLabel}
            </option>
            {minimaxModels.map(model => (
              <option key={model} value={model}>{model}</option>
            ))}
          </select>
          <div style={{ fontSize: 11, color: '#475569', marginTop: 2 }}>
            Optional MiniMax translation model override from Exo Watchdog.
          </div>
        </div>
      )}

      {typeDef.params
        .filter(p => p.name !== 'asset_id' && p.name !== 'media_type' && !(node.data.nodeType === 'subtitle_translate' && p.name === 'model'))
        .filter(p => !(isRemoteSearchNodeType(node.data.nodeType) && p.name === 'query'))
        .filter(p => !(node.data.nodeType === 'material_search' && ['query', 'source_library_ids', 'result_library_ids'].includes(p.name)))
        .filter(p => !(node.data.nodeType === 'material_library_ingest' && p.name === 'target_library_ids'))
        .map(param => (
          <ParamField
            key={param.name}
            param={param}
            value={config[param.name]}
            onChange={val => handleChange(param.name, val)}
          />
        ))}

      <div style={{ borderTop: '1px solid #334155', marginTop: 16, paddingTop: 16 }}>
        <button
          onClick={() => removeNode(node.id)}
          style={{
            width: '100%',
            padding: '8px 12px',
            backgroundColor: '#7f1d1d',
            border: '1px solid #991b1b',
            borderRadius: 4,
            color: '#fca5a5',
            fontSize: 13,
            cursor: 'pointer',
          }}
        >
          Delete Node
        </button>
      </div>
    </div>
  );
}

function formatDuration(duration?: number | null) {
  if (!duration || duration <= 0) return null;
  const hours = Math.floor(duration / 3600);
  const minutes = Math.floor((duration % 3600) / 60);
  const seconds = duration % 60;
  const parts = [hours, minutes, seconds]
    .filter((value, index) => value > 0 || index > 0)
    .map(value => String(value).padStart(2, '0'));
  return parts.join(':');
}

function isRemoteSearchNodeType(value: unknown): value is RemoteSearchNodeType {
  return value === 'youtube_search' || value === 'x_search' || value === 'xiaohongshu_search' || value === 'bilibili_search';
}

function ensurePlannerResultPlatform(
  results: PlannerNodeSearchResult[],
  platform: NonNullable<PlannerNodeSearchResult['platform']>,
): PlannerNodeSearchResult[] {
  return results.map(result => ({
    ...result,
    platform: result.platform || platform,
  }));
}

function normalizeSourceMediaType(value: unknown): SourceMediaKind {
  if (value === 'audio' || value === 'subtitle' || value === 'image' || value === 'video') {
    return value;
  }
  return 'video';
}

function inferAssetKind(asset: Asset): SourceMediaKind | 'other' {
  const mime = (asset.mime_type || '').toLowerCase();
  const name = asset.original_name.toLowerCase();

  if (
    mime.startsWith('video/') ||
    ['.mp4', '.mov', '.mkv', '.avi', '.webm'].some(ext => name.endsWith(ext))
  ) {
    return 'video';
  }
  if (
    mime.startsWith('audio/') ||
    ['.wav', '.mp3', '.m4a', '.aac', '.flac', '.ogg'].some(ext => name.endsWith(ext))
  ) {
    return 'audio';
  }
  if (
    mime.startsWith('image/') ||
    ['.png', '.jpg', '.jpeg', '.webp'].some(ext => name.endsWith(ext))
  ) {
    return 'image';
  }
  if (
    mime.includes('subrip') ||
    mime.includes('subtitle') ||
    ['.srt', '.vtt', '.ass', '.ssa'].some(ext => name.endsWith(ext))
  ) {
    return 'subtitle';
  }

  return 'other';
}

function formatAssetOption(asset: Asset): string {
  const kind = inferAssetKind(asset);
  const duration = Number(asset.media_info?.duration_seconds || asset.media_info?.duration || 0);
  const size = Number(asset.file_size || 0);
  const meta: string[] = [];
  if (kind !== 'other') {
    meta.push(kind);
  }
  const formattedDuration = formatDuration(Number.isFinite(duration) ? duration : 0);
  if (formattedDuration) {
    meta.push(formattedDuration);
  }
  if (size > 0) {
    meta.push(formatFileSize(size));
  }
  return meta.length > 0 ? `${asset.original_name} (${meta.join(' · ')})` : asset.original_name;
}

function ParamField({
  param,
  value,
  onChange,
}: {
  param: ParamDefinition;
  value: unknown;
  onChange: (val: unknown) => void;
}) {
  const current = value ?? param.default;

  return (
    <div style={{ marginBottom: 12 }}>
      <label style={labelStyle}>
        {param.name.replace(/_/g, ' ')}
        {param.required && <span style={{ color: '#ef4444' }}> *</span>}
      </label>

      {param.param_type === 'select' && param.options ? (
        <select
          value={String(current || '')}
          onChange={e => onChange(e.target.value)}
          style={inputStyle}
        >
          {param.options.map(opt => (
            <option key={opt} value={opt}>{opt}</option>
          ))}
        </select>
      ) : param.param_type === 'boolean' ? (
        <label style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <input
            type="checkbox"
            checked={Boolean(current)}
            onChange={e => onChange(e.target.checked)}
          />
          <span>{current ? 'Yes' : 'No'}</span>
        </label>
      ) : param.param_type === 'number' ? (
        <input
          type="number"
          value={current !== undefined && current !== null ? Number(current) : ''}
          onChange={e => onChange(e.target.value === '' ? undefined : Number(e.target.value))}
          min={param.min_value ?? undefined}
          max={param.max_value ?? undefined}
          step={param.max_value && param.max_value <= 1 ? 0.01 : 1}
          style={inputStyle}
        />
      ) : (
        <input
          type="text"
          value={String(current || '')}
          onChange={e => onChange(e.target.value)}
          placeholder={param.description}
          style={inputStyle}
        />
      )}

      {param.description && (
        <div style={{ fontSize: 11, color: '#475569', marginTop: 2 }}>{param.description}</div>
      )}
    </div>
  );
}

const emptyStyle: CSSProperties = {
  width: 280,
  backgroundColor: '#0f172a',
  borderLeft: '1px solid #1e293b',
  padding: 16,
  color: '#64748b',
  fontSize: 13,
};

const panelStyle: CSSProperties = {
  width: 280,
  backgroundColor: '#0f172a',
  borderLeft: '1px solid #1e293b',
  overflowY: 'auto',
  padding: 16,
  color: '#e2e8f0',
  fontSize: 13,
};

const labelStyle: CSSProperties = {
  display: 'block',
  fontSize: 11,
  color: '#64748b',
  marginBottom: 4,
};

const inputStyle: CSSProperties = {
  width: '100%',
  padding: '6px 8px',
  backgroundColor: '#1e293b',
  border: '1px solid #334155',
  borderRadius: 4,
  color: '#e2e8f0',
  fontSize: 13,
  outline: 'none',
};

const cardStyle: CSSProperties = {
  marginBottom: 16,
  padding: 12,
  borderRadius: 8,
  backgroundColor: '#111827',
  border: '1px solid #1f2937',
};

const primaryButtonStyle: CSSProperties = {
  width: '100%',
  padding: '8px 10px',
  backgroundColor: '#1d4ed8',
  border: 'none',
  borderRadius: 6,
  color: '#eff6ff',
  fontSize: 12,
  cursor: 'pointer',
};

const smallButtonStyle: CSSProperties = {
  padding: '6px 10px',
  backgroundColor: '#0f172a',
  color: '#cbd5e1',
  border: '1px solid #334155',
  borderRadius: 6,
  cursor: 'pointer',
  fontSize: 11,
};

const thumbnailWrapStyle: CSSProperties = {
  width: 96,
  aspectRatio: '16 / 9',
  borderRadius: 6,
  overflow: 'hidden',
  backgroundColor: '#1e293b',
  border: '1px solid #334155',
};

const summaryCardStyle: CSSProperties = {
  padding: 10,
  borderRadius: 6,
  border: '1px solid #334155',
  backgroundColor: '#0f172a',
};

const libraryListStyle: CSSProperties = {
  display: 'grid',
  gap: 6,
  maxHeight: 140,
  overflowY: 'auto',
  padding: 8,
  borderRadius: 6,
  border: '1px solid #334155',
  backgroundColor: '#0f172a',
};

const libraryRowStyle: CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  fontSize: 12,
  color: '#cbd5e1',
};
