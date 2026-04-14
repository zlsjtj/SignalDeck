import { Navigate, Route, Routes } from 'react-router-dom';
import { Suspense, lazy, useEffect } from 'react';

import { AppLayout } from '@/components/layout/AppLayout';
import { useAppStore } from '@/store/appStore';

const DashboardPage = lazy(() => import('@/pages/DashboardPage').then(({ DashboardPage }) => ({ default: DashboardPage })));
const GettingStartedPage = lazy(() => import('@/pages/GettingStartedPage').then(({ GettingStartedPage }) => ({ default: GettingStartedPage })));
const MarketStructurePage = lazy(() => import('@/pages/MarketStructurePage').then(({ MarketStructurePage }) => ({ default: MarketStructurePage })));
const StrategiesPage = lazy(() => import('@/pages/StrategiesPage').then(({ StrategiesPage }) => ({ default: StrategiesPage })));
const StrategyCreateWizardPage = lazy(() => import('@/pages/StrategyCreateWizardPage').then(({ StrategyCreateWizardPage }) => ({ default: StrategyCreateWizardPage })));
const StrategyDetailPage = lazy(() => import('@/pages/StrategyDetailPage').then(({ StrategyDetailPage }) => ({ default: StrategyDetailPage })));
const BacktestsPage = lazy(() => import('@/pages/BacktestsPage').then(({ BacktestsPage }) => ({ default: BacktestsPage })));
const BacktestDetailPage = lazy(() => import('@/pages/BacktestDetailPage').then(({ BacktestDetailPage }) => ({ default: BacktestDetailPage })));
const LivePage = lazy(() => import('@/pages/LivePage').then(({ LivePage }) => ({ default: LivePage })));
const RiskPage = lazy(() => import('@/pages/RiskPage').then(({ RiskPage }) => ({ default: RiskPage })));
const LogsPage = lazy(() => import('@/pages/LogsPage').then(({ LogsPage }) => ({ default: LogsPage })));
const NotFoundPage = lazy(() => import('@/pages/NotFoundPage').then(({ NotFoundPage }) => ({ default: NotFoundPage })));
const LoginPage = lazy(() => import('@/pages/LoginPage').then(({ LoginPage }) => ({ default: LoginPage })));

export default function App() {
  const isAuthenticated = useAppStore((s) => s.isAuthenticated);
  const isGuest = useAppStore((s) => s.isGuest);
  const restoreAuth = useAppStore((s) => s.restoreAuth);
  const canAccessApp = isAuthenticated || isGuest;

  useEffect(() => {
    void restoreAuth();
  }, [restoreAuth]);

  return (
    <Suspense fallback={null}>
      <Routes>
        <Route
          path="/login"
          element={canAccessApp ? <Navigate to="/dashboard" replace /> : <LoginPage />}
        />
        <Route
          path="/"
          element={canAccessApp ? <AppLayout /> : <Navigate to="/login" replace />}
        >
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="dashboard" element={<DashboardPage />} />
          <Route path="getting-started" element={<GettingStartedPage />} />
          <Route path="market-structure" element={<MarketStructurePage />} />
          <Route path="strategies" element={<StrategiesPage />} />
          <Route path="strategies/new" element={<StrategyCreateWizardPage />} />
          <Route path="strategies/:id" element={<StrategyDetailPage />} />
          <Route path="backtests" element={<BacktestsPage />} />
          <Route path="backtests/:id" element={<BacktestDetailPage />} />
          <Route path="live" element={<LivePage />} />
          <Route path="risk" element={<RiskPage />} />
          <Route path="logs" element={<LogsPage />} />
          <Route path="*" element={<NotFoundPage />} />
        </Route>
        <Route path="*" element={<Navigate to={canAccessApp ? '/dashboard' : '/login'} replace />} />
      </Routes>
    </Suspense>
  );
}
