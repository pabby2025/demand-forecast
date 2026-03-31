import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { BaseChartDirective } from 'ng2-charts';
import { ChartConfiguration } from 'chart.js';
import { Subscription } from 'rxjs';
import { switchMap } from 'rxjs/operators';
import { ApiService } from '../../../../core/services/api.service';
import { FilterService } from '../../../../core/services/filter.service';
import { KpiCardComponent } from '../../../../shared/components/kpi-card/kpi-card.component';
import { DataGridComponent, GridColumn } from '../../../../shared/components/data-grid/data-grid.component';
import { LoadingSkeletonComponent } from '../../../../shared/components/loading-skeleton/loading-skeleton.component';
import { ErrorCardComponent } from '../../../../shared/components/error-card/error-card.component';

@Component({
  selector: 'app-forecast-overview',
  standalone: true,
  imports: [CommonModule, BaseChartDirective, KpiCardComponent, DataGridComponent, LoadingSkeletonComponent, ErrorCardComponent],
  templateUrl: './forecast-overview.component.html',
})
export class ForecastOverviewComponent implements OnInit, OnDestroy {
  loading = true;
  error = false;
  data: Record<string, unknown> | null = null;
  private sub?: Subscription;

  columns: GridColumn[] = [
    { key: 'practice_area', header: 'Practice Area' },
    { key: 'location', header: 'Location' },
    { key: 'cluster', header: 'Skill Cluster' },
    { key: 'so_grade', header: 'Grade' },
    { key: 'jan', header: 'Jan', type: 'number' },
    { key: 'feb', header: 'Feb', type: 'number' },
    { key: 'mar', header: 'Mar', type: 'number' },
    { key: 'apr', header: 'Apr', type: 'number' },
    { key: 'may', header: 'May', type: 'number' },
    { key: 'jun', header: 'Jun', type: 'number' },
    { key: 'q01', header: 'Q1', type: 'number' },
    { key: 'q02', header: 'Q2', type: 'number' },
  ];

  monthlyChart: ChartConfiguration<'line'>['data'] = { labels: [], datasets: [] };
  weeklyChart: ChartConfiguration<'line'>['data'] = { labels: [], datasets: [] };
  quarterlyChart: ChartConfiguration<'bar'>['data'] = { labels: [], datasets: [] };

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  commonOptions: any = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { labels: { font: { size: 10 } } }, tooltip: { callbacks: { label: (c: { parsed: { y: number } }) => c.parsed.y.toLocaleString() } } },
    scales: { x: { ticks: { font: { size: 10 } } }, y: { ticks: { font: { size: 10 }, callback: (v: number | string) => Number(v).toLocaleString() } } },
  };

  constructor(private apiService: ApiService, private filterService: FilterService) {}

  ngOnInit(): void {
    this.sub = this.filterService.filters$.pipe(
      switchMap((filters) => {
        this.loading = true;
        return this.apiService.getForecastOverview(filters);
      })
    ).subscribe({
      next: (d) => { this.data = d as unknown as Record<string, unknown>; this.buildCharts(); this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  ngOnDestroy(): void { this.sub?.unsubscribe(); }

  load(): void {
    this.error = false;
    this.loading = true;
    this.apiService.getForecastOverview(this.filterService.filters).subscribe({
      next: (d) => { this.data = d as unknown as Record<string, unknown>; this.buildCharts(); this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  private buildCharts(): void {
    // trend_monthly: { month, fte_demand, growth_rate_pct }
    const monthly = (this.data?.['trend_monthly'] as Array<{ month: string; fte_demand: number; growth_rate_pct: number }>) ?? [];
    this.monthlyChart = {
      labels: monthly.map((t) => t.month),
      datasets: [
        { data: monthly.map((t) => t.fte_demand), borderColor: '#00BCD4', borderWidth: 2, fill: false, tension: 0.3, label: 'FTE Demand', pointRadius: 3 },
        { data: monthly.map((t) => t.growth_rate_pct), borderColor: '#7C3AED', borderWidth: 2, borderDash: [4, 2], fill: false, tension: 0.3, label: 'Growth %', pointRadius: 0 },
      ],
    };
    // trend_weekly: { week, fte_demand, growth_rate_pct }
    const weekly = (this.data?.['trend_weekly'] as Array<{ week: string; fte_demand: number }>) ?? [];
    this.weeklyChart = {
      labels: weekly.map((t) => t.week),
      datasets: [{ data: weekly.map((t) => t.fte_demand), borderColor: '#00BCD4', backgroundColor: 'rgba(0,188,212,0.1)', fill: true, tension: 0.3, label: 'FTE Demand', pointRadius: 0 }],
    };
    // trend_quarterly: { quarter, fte_demand, growth_rate_pct }
    const quarterly = (this.data?.['trend_quarterly'] as Array<{ quarter: string; fte_demand: number; growth_rate_pct: number }>) ?? [];
    this.quarterlyChart = {
      labels: quarterly.map((t) => t.quarter),
      datasets: [
        { data: quarterly.map((t) => t.fte_demand), backgroundColor: '#1B2559', label: 'FTE Demand', borderRadius: 4 },
        { data: quarterly.map((t) => t.growth_rate_pct), backgroundColor: '#00BCD4', label: 'Growth %', borderRadius: 4 },
      ],
    };
  }

  get kpis(): Record<string, number> {
    const k = (this.data?.['kpis'] as Record<string, unknown>) ?? {};
    const dgr = (k['demand_growth_rate'] as Record<string, Record<string, number>>) ?? {};
    return {
      total_forecast_fte: (k['total_forecast_fte'] as number) ?? 0,
      mom_change: dgr['mom']?.['current'] ?? 0,
      yoy_growth: dgr['qoq']?.['current'] ?? 0,
      avg_cancellation_pct: (k['avg_cancellation_pct'] as number) ?? 0,
    };
  }
  get grid(): Record<string, unknown>[] { return (this.data?.['grid'] as Record<string, unknown>[]) ?? []; }
}
