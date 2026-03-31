import { Routes } from '@angular/router';
import { authGuard } from './core/guards/auth.guard';

export const routes: Routes = [
  {
    path: 'login',
    loadComponent: () => import('./pages/login/login.component').then(m => m.LoginComponent),
  },
  {
    path: '',
    loadComponent: () => import('./pages/layout/layout.component').then(m => m.LayoutComponent),
    canActivate: [authGuard],
    children: [
      {
        path: '',
        loadComponent: () => import('./pages/executive-summary/executive-summary.component').then(m => m.ExecutiveSummaryComponent),
      },
      {
        path: 'forecast',
        loadComponent: () => import('./pages/forecast-dashboard/forecast-dashboard.component').then(m => m.ForecastDashboardComponent),
        children: [
          { path: '', loadComponent: () => import('./pages/forecast-dashboard/tabs/forecast-overview/forecast-overview.component').then(m => m.ForecastOverviewComponent) },
          { path: 'demand-type', loadComponent: () => import('./pages/forecast-dashboard/tabs/demand-type/demand-type.component').then(m => m.DemandTypeComponent) },
          { path: 'bu-performance', loadComponent: () => import('./pages/forecast-dashboard/tabs/bu-performance/bu-performance.component').then(m => m.BuPerformanceComponent) },
          { path: 'geographic', loadComponent: () => import('./pages/forecast-dashboard/tabs/geographic/geographic.component').then(m => m.GeographicComponent) },
          { path: 'skills', loadComponent: () => import('./pages/forecast-dashboard/tabs/skill-distribution/skill-distribution.component').then(m => m.SkillDistributionComponent) },
          { path: 'grades', loadComponent: () => import('./pages/forecast-dashboard/tabs/grade-distribution/grade-distribution.component').then(m => m.GradeDistributionComponent) },
          { path: 'gap', loadComponent: () => import('./pages/forecast-dashboard/tabs/demand-supply-gap/demand-supply-gap.component').then(m => m.DemandSupplyGapComponent) },
        ],
      },
      // Scenario Planning disabled
      // { path: 'scenarios', loadComponent: () => import('./pages/scenario-planning/scenario-planning.component').then(m => m.ScenarioPlanningComponent) },
      {
        path: 'feedback',
        loadComponent: () => import('./pages/forecast-feedback/forecast-feedback.component').then(m => m.ForecastFeedbackComponent),
      },
      {
        path: 'taxonomy',
        loadComponent: () => import('./pages/skill-taxonomy/skill-taxonomy.component').then(m => m.SkillTaxonomyComponent),
      },
      {
        path: 'tasks',
        loadComponent: () => import('./pages/my-tasks/my-tasks.component').then(m => m.MyTasksComponent),
      },
      {
        path: 'alerts',
        loadComponent: () => import('./pages/my-alerts/my-alerts.component').then(m => m.MyAlertsComponent),
      },
    ],
  },
  { path: '**', redirectTo: '' },
];
