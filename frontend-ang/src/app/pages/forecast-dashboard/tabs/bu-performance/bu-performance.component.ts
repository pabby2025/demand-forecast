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

type D = Record<string, unknown>;

@Component({
  selector: 'app-bu-performance',
  standalone: true,
  imports: [CommonModule, BaseChartDirective, DataGridComponent, LoadingSkeletonComponent, ErrorCardComponent],
  template: `
    <app-loading-skeleton *ngIf="loading" type="page"></app-loading-skeleton>
    <app-error-card *ngIf="error && !loading" (onRetry)="load()"></app-error-card>

    <div *ngIf="!loading && !error && data" class="space-y-5">
      <!-- KPI Row -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <!-- Top BUs by Demand -->
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Top BUs by Demand</h3>
          <div class="space-y-2">
            <div *ngFor="let b of topByDemand" class="flex items-center gap-3 text-xs">
              <span class="text-gray-600 font-medium w-28 truncate flex-shrink-0" [title]="b['name']">{{ b['name'] }}</span>
              <div class="flex-1 bg-gray-100 rounded-full h-2">
                <div class="h-2 rounded-full bg-[#1B2559]" [style.width.%]="b['pct']"></div>
              </div>
              <span class="font-bold text-gray-800 w-8 text-right">{{ b['pct'] }}%</span>
            </div>
          </div>
        </div>

        <!-- Top BUs by Growth -->
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Top BUs by Growth Rate</h3>
          <div class="space-y-2">
            <div *ngFor="let b of topByGrowth" class="flex items-center justify-between text-xs">
              <div class="flex items-center gap-2">
                <span class="text-gray-600 font-medium w-28 truncate" [title]="b['name']">{{ b['name'] }}</span>
                <svg *ngIf="asNum(b['growth_pct']) >= 0" class="w-3 h-3 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18" stroke-width="2"/></svg>
                <svg *ngIf="asNum(b['growth_pct']) < 0" class="w-3 h-3 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><polyline points="23 18 13.5 8.5 8.5 13.5 1 6" stroke-width="2"/></svg>
              </div>
              <span [class]="asNum(b['growth_pct']) >= 0 ? 'font-bold text-green-600' : 'font-bold text-red-500'">
                {{ asNum(b['growth_pct']) >= 0 ? '+' : '' }}{{ b['growth_pct'] }}%
              </span>
            </div>
          </div>
        </div>
      </div>

      <!-- Charts Row -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">BU Demand (Stacked Bar)</h3>
          <div style="height:220px"><canvas baseChart [data]="barChart" [type]="'bar'" [options]="barOptions"></canvas></div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">BU Growth Rates Trend</h3>
          <div style="height:220px"><canvas baseChart [data]="trendChart" [type]="'line'" [options]="lineOptions"></canvas></div>
        </div>
      </div>

      <!-- Grid 1: BU Demand -->
      <div>
        <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2 px-1">BU Wise Demand</h3>
        <app-data-grid [data]="demandGrid" [columns]="demandColumns" [pageSize]="10" downloadFilename="bu-demand.csv"></app-data-grid>
      </div>

      <!-- Grid 2: BU Growth Rates -->
      <div>
        <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2 px-1">BU Wise Growth Rates</h3>
        <app-data-grid [data]="growthGrid" [columns]="growthColumns" [pageSize]="10" downloadFilename="bu-growth.csv"></app-data-grid>
      </div>
    </div>
  `,
})
export class BuPerformanceComponent implements OnInit, OnDestroy {
  loading = true; error = false; data: D | null = null;
  private sub?: Subscription;

  barChart: ChartConfiguration<'bar'>['data'] = { labels: [], datasets: [] };
  trendChart: ChartConfiguration<'line'>['data'] = { labels: [], datasets: [] };

  barOptions = {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { labels: { font: { size: 9 }, boxWidth: 10 } } },
    scales: { x: { stacked: true, ticks: { font: { size: 9 } } }, y: { stacked: true, ticks: { font: { size: 9 } } } },
  };
  lineOptions = {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { labels: { font: { size: 9 }, boxWidth: 10 } } },
    scales: { x: { ticks: { font: { size: 9 } } }, y: { ticks: { font: { size: 9 } } } },
  };

  private readonly COLORS = ['#1B2559', '#00BCD4', '#7C3AED', '#F59E0B', '#EF4444'];

  demandColumns: GridColumn[] = [
    { key: 'business_unit', header: 'Business Unit' }, { key: 'practice_area', header: 'Practice Area' },
    { key: 'cluster', header: 'Skill Cluster' }, { key: 'so_grade', header: 'Grade' },
    { key: 'jan', header: 'Jan', type: 'number' }, { key: 'feb', header: 'Feb', type: 'number' },
    { key: 'mar', header: 'Mar', type: 'number' }, { key: 'apr', header: 'Apr', type: 'number' },
    { key: 'may', header: 'May', type: 'number' }, { key: 'jun', header: 'Jun', type: 'number' },
  ];
  growthColumns: GridColumn[] = [
    { key: 'business_unit', header: 'Business Unit' }, { key: 'practice_area', header: 'Practice Area' },
    { key: 'cluster', header: 'Skill Cluster' }, { key: 'so_grade', header: 'Grade' },
    { key: 'jan', header: 'Jan', type: 'number' }, { key: 'feb', header: 'Feb', type: 'number' },
    { key: 'mar', header: 'Mar', type: 'number' }, { key: 'apr', header: 'Apr', type: 'number' },
    { key: 'may', header: 'May', type: 'number' }, { key: 'jun', header: 'Jun', type: 'number' },
  ];

  constructor(private apiService: ApiService, private filterService: FilterService) {}

  ngOnInit(): void {
    this.sub = this.filterService.filters$.pipe(
      switchMap((f) => { this.loading = true; this.error = false; return this.apiService.getBUPerformance(f); })
    ).subscribe({
      next: (d) => { this.data = d as unknown as D; this.buildCharts(); this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  ngOnDestroy(): void { this.sub?.unsubscribe(); }

  load(): void {
    this.error = false; this.loading = true;
    this.apiService.getBUPerformance(this.filterService.filters).subscribe({
      next: (d) => { this.data = d as unknown as D; this.buildCharts(); this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  private buildCharts(): void {
    const kpis = (this.data?.['kpis'] as D) ?? {};
    const trend = (this.data?.['trend_bu_demand'] as Array<D>) ?? [];
    const keys = trend.length ? Object.keys(trend[0]).filter((k) => k !== 'month') : [];

    this.barChart = {
      labels: trend.map((t) => String(t['month'])),
      datasets: keys.map((k, i) => ({
        data: trend.map((t) => Number(t[k])),
        backgroundColor: this.COLORS[i % this.COLORS.length],
        label: k,
        borderWidth: 0,
      })),
    };

    this.trendChart = {
      labels: trend.map((t) => String(t['month'])),
      datasets: keys.map((k, i) => ({
        data: trend.map((t) => Number(t[k])),
        borderColor: this.COLORS[i % this.COLORS.length],
        borderWidth: 2, fill: false, tension: 0.3, label: k, pointRadius: 0,
      })),
    };
  }

  get topByDemand(): D[] {
    const kpis = (this.data?.['kpis'] as D) ?? {};
    return (kpis['top_bus_by_demand'] as D[]) ?? [];
  }
  get topByGrowth(): D[] {
    const kpis = (this.data?.['kpis'] as D) ?? {};
    return (kpis['top_bus_by_growth'] as D[]) ?? [];
  }
  get demandGrid(): D[] { return (this.data?.['grid_bu_demand'] as D[]) ?? []; }
  get growthGrid(): D[] { return (this.data?.['grid_bu_demand'] as D[]) ?? []; }
  asNum(v: unknown): number { return Number(v) || 0; }
}
