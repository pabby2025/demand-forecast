import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { BaseChartDirective } from 'ng2-charts';
import { ChartConfiguration } from 'chart.js';
import { Subscription } from 'rxjs';
import { switchMap } from 'rxjs/operators';
import { ApiService } from '../../../../core/services/api.service';
import { FilterService } from '../../../../core/services/filter.service';
import { DataGridComponent, GridColumn } from '../../../../shared/components/data-grid/data-grid.component';
import { LoadingSkeletonComponent } from '../../../../shared/components/loading-skeleton/loading-skeleton.component';
import { ErrorCardComponent } from '../../../../shared/components/error-card/error-card.component';

@Component({
  selector: 'app-bu-performance',
  standalone: true,
  imports: [CommonModule, BaseChartDirective, DataGridComponent, LoadingSkeletonComponent, ErrorCardComponent],
  template: `
    <app-loading-skeleton *ngIf="loading" type="page"></app-loading-skeleton>
    <app-error-card *ngIf="error && !loading" (onRetry)="load()"></app-error-card>
    <div *ngIf="!loading && !error && data" class="space-y-5">
      <h2 class="text-lg font-bold text-gray-900">Business Unit Performance</h2>
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">Top 5 BUs by Demand</h3>
          <div style="height:200px"><canvas baseChart [data]="barChart" [type]="'bar'" [options]="barOptions"></canvas></div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">Demand Trend by BU</h3>
          <div style="height:200px"><canvas baseChart [data]="trendChart" [type]="'line'" [options]="lineOptions"></canvas></div>
        </div>
      </div>
      <app-data-grid [data]="grid" [columns]="columns" [pageSize]="10" downloadFilename="bu-performance.csv"></app-data-grid>
    </div>
  `,
})
export class BuPerformanceComponent implements OnInit, OnDestroy {
  loading = true; error = false; data: Record<string, unknown> | null = null;
  private sub?: Subscription;

  barChart: ChartConfiguration<'bar'>['data'] = { labels: [], datasets: [] };
  trendChart: ChartConfiguration<'line'>['data'] = { labels: [], datasets: [] };

  barOptions = { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { ticks: { font: { size: 9 } } }, y: { ticks: { font: { size: 10 } } } } };
  lineOptions = { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { font: { size: 9 } } } }, scales: { x: { ticks: { font: { size: 9 } } }, y: { ticks: { font: { size: 10 } } } } };

  columns: GridColumn[] = [
    { key: 'business_unit', header: 'Business Unit' }, { key: 'practice_area', header: 'Practice Area' },
    { key: 'cluster', header: 'Skill Cluster' }, { key: 'so_grade', header: 'Grade' },
    { key: 'jan', header: 'Jan', type: 'number' }, { key: 'feb', header: 'Feb', type: 'number' },
    { key: 'mar', header: 'Mar', type: 'number' }, { key: 'apr', header: 'Apr', type: 'number' },
    { key: 'may', header: 'May', type: 'number' }, { key: 'jun', header: 'Jun', type: 'number' },
  ];

  constructor(private apiService: ApiService, private filterService: FilterService) {}

  ngOnInit(): void {
    this.sub = this.filterService.filters$.pipe(switchMap((f) => { this.loading = true; return this.apiService.getBUPerformance(f); }))
      .subscribe({ next: (d) => { this.data = d as unknown as Record<string, unknown>; this.buildCharts(); this.loading = false; }, error: () => { this.error = true; this.loading = false; } });
  }

  ngOnDestroy(): void { this.sub?.unsubscribe(); }

  load(): void {
    this.error = false; this.loading = true;
    this.apiService.getBUPerformance(this.filterService.filters).subscribe({ next: (d) => { this.data = d as unknown as Record<string, unknown>; this.buildCharts(); this.loading = false; }, error: () => { this.error = true; this.loading = false; } });
  }

  private buildCharts(): void {
    const colors = ['#1B2559', '#00BCD4', '#7C3AED', '#F59E0B', '#EF4444'];
    // kpis.top_bus_by_demand: [{ name, pct }]
    const kpis = (this.data?.['kpis'] as Record<string, unknown>) ?? {};
    const topBus = (kpis['top_bus_by_demand'] as Array<{ name: string; pct: number }>) ?? [];
    this.barChart = {
      labels: topBus.map((b) => b.name),
      datasets: [{ data: topBus.map((b) => b.pct), backgroundColor: '#1B2559', label: 'Demand Share %', borderRadius: 4 }],
    };
    // trend_bu_demand: [{ month, BU1: n, BU2: n, ... }]
    const trend = (this.data?.['trend_bu_demand'] as Array<Record<string, string | number>>) ?? [];
    const keys = trend.length ? Object.keys(trend[0]).filter((k) => k !== 'month') : [];
    this.trendChart = {
      labels: trend.map((t) => String(t['month'])),
      datasets: keys.map((k, i) => ({ data: trend.map((t) => Number(t[k])), borderColor: colors[i % colors.length], borderWidth: 2, fill: false, tension: 0.3, label: k, pointRadius: 0 })),
    };
  }

  get grid(): Record<string, unknown>[] { return (this.data?.['grid_bu_demand'] as Record<string, unknown>[]) ?? []; }
}
