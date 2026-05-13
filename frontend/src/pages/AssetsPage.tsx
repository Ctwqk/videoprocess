import { useState, useEffect, useRef } from 'react';
import apiClient from '../api/client';
import type { Asset } from '../api/types';
import { formatFileSize } from '../utils/fileSize';

export default function AssetsPage() {
  const [assets, setAssets] = useState<Asset[]>([]);
  const [loading, setLoading] = useState(true);
  const [uploading, setUploading] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const fetchAssets = () => {
    apiClient.get('/assets').then(res => {
      setAssets(res.data.items);
      setLoading(false);
    }).catch(() => setLoading(false));
  };

  useEffect(() => { fetchAssets(); }, []);

  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploading(true);
    try {
      const formData = new FormData();
      formData.append('file', file);
      await apiClient.post('/assets/upload', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
        timeout: 300000,
      });
      fetchAssets();
    } catch (err) {
      alert('Upload failed');
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = '';
    }
  };

  const handleDelete = async (id: string) => {
    if (!confirm('Delete this asset?')) return;
    await apiClient.delete(`/assets/${id}`);
    fetchAssets();
  };

  const handleDownload = (id: string) => {
    window.open(`/api/v1/assets/${id}/download`, '_blank');
  };

  return (
    <div
      style={{
        padding: 24,
        color: '#e2e8f0',
        overflowY: 'auto',
        height: '100%',
        backgroundColor: '#020617',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 16 }}>
        <h1 style={{ fontSize: 20, fontWeight: 700, margin: 0 }}>Assets</h1>
        <label
          style={{
            padding: '6px 16px',
            backgroundColor: '#2563eb',
            color: '#fff',
            borderRadius: 6,
            cursor: uploading ? 'wait' : 'pointer',
            fontSize: 13,
            fontWeight: 500,
          }}
        >
          {uploading ? 'Uploading...' : 'Upload File'}
          <input
            ref={fileInputRef}
            type="file"
            onChange={handleUpload}
            style={{ display: 'none' }}
            accept="video/*,audio/*,image/*,.srt,.vtt,.ass,.ssa"
          />
        </label>
      </div>

      {loading ? (
        <div style={{ color: '#94a3b8' }}>Loading...</div>
      ) : assets.length === 0 ? (
        <div style={{ color: '#94a3b8' }}>No assets uploaded yet.</div>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #334155', color: '#94a3b8', textAlign: 'left' }}>
              <th style={{ padding: '8px 12px' }}>Name</th>
              <th style={{ padding: '8px 12px' }}>Type</th>
              <th style={{ padding: '8px 12px' }}>Size</th>
              <th style={{ padding: '8px 12px' }}>Uploaded</th>
              <th style={{ padding: '8px 12px' }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {assets.map(asset => (
              <tr key={asset.id} style={{ borderBottom: '1px solid #1e293b' }}>
                <td style={{ padding: '8px 12px' }}>{asset.original_name}</td>
                <td style={{ padding: '8px 12px', color: '#94a3b8' }}>{asset.mime_type || '-'}</td>
                <td style={{ padding: '8px 12px', color: '#94a3b8' }}>{formatFileSize(asset.file_size)}</td>
                <td style={{ padding: '8px 12px', color: '#94a3b8' }}>
                  {new Date(asset.uploaded_at).toLocaleString()}
                </td>
                <td style={{ padding: '8px 12px' }}>
                  <button onClick={() => handleDownload(asset.id)}
                    style={{ color: '#3b82f6', background: 'none', border: 'none', cursor: 'pointer', marginRight: 12, fontSize: 13 }}>
                    Download
                  </button>
                  <button onClick={() => handleDelete(asset.id)}
                    style={{ color: '#ef4444', background: 'none', border: 'none', cursor: 'pointer', fontSize: 13 }}>
                    Delete
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
