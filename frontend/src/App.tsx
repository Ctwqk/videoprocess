import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import AppShell from './components/layout/AppShell';
import EditorPage from './pages/EditorPage';
import JobsPage from './pages/JobsPage';
import JobDetailPage from './pages/JobDetailPage';
import AssetsPage from './pages/AssetsPage';
import TemplatesPage from './pages/TemplatesPage';

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<AppShell />}>
          <Route path="/" element={<Navigate to="/editor" replace />} />
          <Route path="/editor" element={<EditorPage />} />
          <Route path="/editor/:pipelineId" element={<EditorPage />} />
          <Route path="/jobs" element={<JobsPage />} />
          <Route path="/jobs/:jobId" element={<JobDetailPage />} />
          <Route path="/assets" element={<AssetsPage />} />
          <Route path="/templates" element={<TemplatesPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App;
