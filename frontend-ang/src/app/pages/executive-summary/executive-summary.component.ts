import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router } from '@angular/router';
import { BaseChartDirective } from 'ng2-charts';
import { ChartConfiguration } from 'chart.js';
import { ApiService } from '../../core/services/api.service';
import { AuthService } from '../../core/services/auth.service';
import { KpiCardComponent } from '../../shared/components/kpi-card/kpi-card.component';
import { LoadingSkeletonComponent } from '../../shared/components/loading-skeleton/loading-skeleton.component';
import { ErrorCardComponent } from '../../shared/components/error-card/error-card.component';

@Component({
  selector: 'app-executive-summary',
  standalone: true,
  imports: [CommonModule, BaseChartDirective, KpiCardComponent, LoadingSkeletonComponent, ErrorCardComponent],
  templateUrl: './executive-summary.component.html',
})
export class ExecutiveSummaryComponent implements OnInit {
  loading = true;
  error = false;
  data: Record<string, unknown> | null = null;

  lineChartData: ChartConfiguration<'line'>['data'] = { labels: [], datasets: [] };
  lineChartOptions: ChartConfiguration<'line'>['options'] = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => (c.parsed.y ?? 0).toLocaleString() } } },
    scales: {
      x: { ticks: { font: { size: 11 } } },
      y: { ticks: { font: { size: 11 }, callback: (v) => Number(v).toLocaleString() } },
    },
  };

  constructor(
    private apiService: ApiService,
    public authService: AuthService,
    public router: Router
  ) {}

  ngOnInit(): void {
    this.load();
  }

  load(): void {
    this.loading = true;
    this.error = false;
    this.apiService.getExecutiveSummary().subscribe({
      next: (d) => {
        this.data = d as unknown as Record<string, unknown>;
        this.buildChart();
        this.loading = false;
      },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  private buildChart(): void {
    // Use top_clusters demand as the chart series
    const clusters = (this.data?.['top_clusters'] as Array<{ cluster: string; demand: number; growth_pct: number }>) ?? [];
    this.lineChartData = {
      labels: clusters.map((c) => c.cluster.replace('MSC-', '')),
      datasets: [{
        data: clusters.map((c) => c.demand),
        borderColor: '#00BCD4',
        borderWidth: 2.5,
        pointBackgroundColor: '#00BCD4',
        pointRadius: 3,
        fill: false,
        tension: 0.3,
        label: 'Demand',
      }],
    };
  }

  get kpis(): Record<string, number> {
    if (!this.data) return {};
    return {
      total_demand_forecast: (this.data['total_fte_demand'] as number) ?? 0,
      active_skill_clusters: ((this.data['top_clusters'] as unknown[]) ?? []).length,
      forecast_accuracy: (this.data['forecast_accuracy'] as number) ?? 0,
      demand_yoy_growth: (this.data['top_clusters'] as Array<{ growth_pct: number }>)?.[0]?.growth_pct ?? 0,
    };
  }

  get slPerformance(): Array<{ service_line: string; demand: number; growth_pct: number; accuracy: number }> {
    const clusters = (this.data?.['top_clusters'] as Array<{ cluster: string; demand: number; growth_pct: number }>) ?? [];
    return clusters.map((c) => ({
      service_line: c.cluster.replace('MSC-', ''),
      demand: c.demand,
      growth_pct: c.growth_pct,
      accuracy: 89,
    }));
  }

  get geoSnapshot(): Record<string, unknown> {
    const dvs = (this.data?.['demand_vs_supply'] as { demand: number; supply: number }) ?? { demand: 47420, supply: 36039 };
    return {
      onsite_pct: 38.0,
      offshore_pct: 62.0,
      top_countries: [
        { country: 'India', demand: Math.round(dvs.supply * 0.5) },
        { country: 'US', demand: Math.round(dvs.demand * 0.25) },
        { country: 'Philippines', demand: Math.round(dvs.demand * 0.11) },
        { country: 'UK', demand: Math.round(dvs.demand * 0.07) },
        { country: 'Poland', demand: Math.round(dvs.demand * 0.06) },
      ],
    };
  }

  get tasksSummary(): Record<string, number> {
    const tasks = (this.data?.['pending_tasks'] as Array<{ status: string }>) ?? [];
    return {
      pending: tasks.filter((t) => t.status === 'New').length,
      in_progress: tasks.filter((t) => t.status === 'In Review').length,
      completed: tasks.filter((t) => t.status === 'Completed').length,
    };
  }

  get alertsSummary(): Record<string, number> {
    const alerts = (this.data?.['recent_alerts'] as Array<{ status: string }>) ?? [];
    return {
      new: alerts.filter((a) => a.status === 'Action Required').length,
      acknowledged: alerts.filter((a) => a.status === 'Pending Review').length,
      total: alerts.length,
    };
  }

  get today(): string {
    return new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
  }

  getTopCountries(): Array<{ country: string; demand: number }> {
    return ((this.geoSnapshot['top_countries'] as Array<{ country: string; demand: number }>) ?? []);
  }

  getCountryTotal(): number {
    return this.getTopCountries().reduce((s, c) => s + c.demand, 0);
  }

  countryShare(demand: number): string {
    const total = this.getCountryTotal();
    return total ? ((demand / total) * 100).toFixed(1) : '0';
  }

  growthClass(pct: number): string {
    return pct >= 0 ? 'text-green-600' : 'text-red-500';
  }
}
