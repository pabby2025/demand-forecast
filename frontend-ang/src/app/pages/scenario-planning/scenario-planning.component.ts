import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { BaseChartDirective } from 'ng2-charts';
import { ChartConfiguration } from 'chart.js';
import { ApiService } from '../../core/services/api.service';
import { Scenario, ScenarioDrivers } from '../../core/models';
import { StatusBadgeComponent } from '../../shared/components/status-badge/status-badge.component';
import { LoadingSkeletonComponent } from '../../shared/components/loading-skeleton/loading-skeleton.component';
import { ErrorCardComponent } from '../../shared/components/error-card/error-card.component';

const DEFAULT_DRIVERS: ScenarioDrivers = {
  bu_level_growth_pct: 5,
  industry_level_market_spend_pct: 3,
  win_rate_strategic_pct: 2,
  growth_strategic_pct: 5,
};

@Component({
  selector: 'app-scenario-planning',
  standalone: true,
  imports: [CommonModule, FormsModule, BaseChartDirective, StatusBadgeComponent, LoadingSkeletonComponent, ErrorCardComponent],
  templateUrl: './scenario-planning.component.html',
})
export class ScenarioPlanningComponent implements OnInit {
  loading = true; error = false;
  scenarios: Scenario[] = [];
  view: 'list' | 'new' | 'edit' = 'list';
  editTarget: Scenario | null = null;

  // Form fields
  formName = '';
  formDesc = '';
  formDrivers: ScenarioDrivers = { ...DEFAULT_DRIVERS };

  driverLabels: Record<keyof ScenarioDrivers, string> = {
    bu_level_growth_pct: 'BU Level Growth (%)',
    industry_level_market_spend_pct: 'Industry Market Spend (%)',
    win_rate_strategic_pct: 'Win Rate — Strategic (%)',
    growth_strategic_pct: 'Strategic Growth Target (%)',
  };
  driverKeys: (keyof ScenarioDrivers)[] = [
    'bu_level_growth_pct',
    'industry_level_market_spend_pct',
    'win_rate_strategic_pct',
    'growth_strategic_pct',
  ];

  comparisonChart: ChartConfiguration<'line'>['data'] = { labels: [], datasets: [] };
  chartOptions = { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { font: { size: 9 } } } }, scales: { x: { ticks: { font: { size: 10 } } }, y: { ticks: { font: { size: 10 }, callback: (v: number | string) => Number(v).toLocaleString() } } } };

  constructor(private apiService: ApiService) {}

  ngOnInit(): void { this.load(); }

  load(): void {
    this.loading = true; this.error = false;
    this.apiService.getScenarios().subscribe({
      next: (d: unknown) => { this.scenarios = (d as { scenarios: Scenario[] }).scenarios ?? []; this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  get impact() {
    const d = this.formDrivers;
    const delta_pct =
      d.bu_level_growth_pct * 0.4 +
      d.industry_level_market_spend_pct * 0.3 +
      d.win_rate_strategic_pct * 0.2 +
      d.growth_strategic_pct * 0.1;
    const base = 47420;
    const delta = Math.round(base * delta_pct / 100);
    return { delta, delta_pct: Math.round(delta_pct * 100) / 100, new_total: base + delta };
  }

  updateChart(): void {
    const pct = this.impact.delta_pct / 100;
    const baselines = [7800, 8100, 7950, 8300, 7700, 7570];
    const months = ['M0', 'M1', 'M2', 'M3', 'M4', 'M5'];
    this.comparisonChart = {
      labels: months,
      datasets: [
        { data: baselines, borderColor: '#1B2559', borderWidth: 2, fill: false, label: 'Baseline', pointRadius: 0 },
        { data: baselines.map((b) => Math.round(b * (1 + pct))), borderColor: '#00BCD4', borderWidth: 2, borderDash: [4, 2], fill: false, label: 'Scenario', pointRadius: 0 },
      ],
    };
  }

  openNew(): void {
    this.formName = ''; this.formDesc = '';
    this.formDrivers = { ...DEFAULT_DRIVERS };
    this.updateChart();
    this.view = 'new';
  }

  openEdit(scn: Scenario): void {
    this.editTarget = scn;
    this.formName = scn.name; this.formDesc = scn.description;
    this.formDrivers = { ...scn.drivers };
    this.updateChart();
    this.view = 'edit';
  }

  onDriverChange(): void { this.updateChart(); }

  submitNew(): void {
    this.apiService.createScenario({
      name: this.formName,
      description: this.formDesc,
      drivers: this.formDrivers,
      status: 'Draft',
      created_by: 'Current User',
      filters: {},
    }).subscribe({
      next: () => { this.load(); this.view = 'list'; },
    });
  }

  approve(): void {
    if (!this.editTarget) return;
    this.apiService.updateScenario(this.editTarget.id, { status: 'Finalized' }).subscribe({ next: () => { this.load(); this.view = 'list'; } });
  }

  reject(): void {
    if (!this.editTarget) return;
    this.apiService.updateScenario(this.editTarget.id, { status: 'Rejected' }).subscribe({ next: () => { this.load(); this.view = 'list'; } });
  }

  isReadOnly(): boolean {
    return this.editTarget?.status === 'In Review' || this.editTarget?.status === 'Finalized';
  }

  deltaClass(v: number): string { return v >= 0 ? 'text-green-600' : 'text-red-500'; }
}
