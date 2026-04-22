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
  selector: 'app-geographic',
  standalone: true,
  imports: [CommonModule, BaseChartDirective, DataGridComponent, LoadingSkeletonComponent, ErrorCardComponent],
  template: `
    <app-loading-skeleton *ngIf="loading" type="page"></app-loading-skeleton>
    <app-error-card *ngIf="error && !loading" (onRetry)="load()"></app-error-card>
    <div *ngIf="!loading && !error && data" class="space-y-5">
      <h2 class="text-lg font-bold text-gray-900">Geographic Distribution</h2>
      <div class="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-4 text-center">
          <div class="text-xs text-gray-500 uppercase font-medium">Total</div>
          <div class="text-2xl font-bold text-gray-900 mt-1">{{ summary['total'] | number }}</div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-4 text-center">
          <div class="text-xs text-gray-500 uppercase font-medium">Onsite</div>
          <div class="text-2xl font-bold text-[#1B2559] mt-1">{{ summary['onsite_pct'] }}%</div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-4 text-center">
          <div class="text-xs text-gray-500 uppercase font-medium">Offshore</div>
          <div class="text-2xl font-bold text-[#00BCD4] mt-1">{{ summary['offshore_pct'] }}%</div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-4 text-center">
          <div class="text-xs text-gray-500 uppercase font-medium">Countries</div>
          <div class="text-2xl font-bold text-gray-900 mt-1">{{ countryData.length }}</div>
        </div>
      </div>
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">Onsite vs Offshore</h3>
          <div style="height:200px"><canvas baseChart [data]="donutChart" [type]="'doughnut'" [options]="donutOptions"></canvas></div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">Mix Trend</h3>
          <div style="height:200px"><canvas baseChart [data]="trendChart" [type]="'bar'" [options]="barOptions"></canvas></div>
        </div>
      </div>
      <app-data-grid [data]="countryData" [columns]="columns" [pageSize]="10" downloadFilename="geographic.csv"></app-data-grid>
    </div>
  `,
})
export class GeographicComponent implements OnInit, OnDestroy {
  loading = true; error = false; data: Record<string, unknown> | null = null;
  private sub?: Subscription;
  donutChart: ChartConfiguration<'doughnut'>['data'] = { labels: [], datasets: [] };
  trendChart: ChartConfiguration<'bar'>['data'] = { labels: [], datasets: [] };
  donutOptions: ChartConfiguration<'doughnut'>['options'] = { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { font: { size: 10 } } } } };
  barOptions = { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { font: { size: 10 } } } }, scales: { x: { stacked: true, ticks: { font: { size: 10 } } }, y: { stacked: true, ticks: { font: { size: 10 } } } } };
  columns: GridColumn[] = [
    { key: 'country', header: 'Country' },
    { key: 'jan', header: 'Jan', type: 'number' }, { key: 'feb', header: 'Feb', type: 'number' },
    { key: 'mar', header: 'Mar', type: 'number' }, { key: 'apr', header: 'Apr', type: 'number' },
    { key: 'may', header: 'May', type: 'number' }, { key: 'jun', header: 'Jun', type: 'number' },
  ];

  constructor(private apiService: ApiService, private filterService: FilterService) {}
  ngOnInit(): void {
    this.sub = this.filterService.filters$.pipe(switchMap((f) => { this.loading = true; return this.apiService.getGeographic(f); }))
      .subscribe({ next: (d) => { this.data = d as unknown as Record<string, unknown>; this.buildCharts(); this.loading = false; }, error: () => { this.error = true; this.loading = false; } });
  }
  ngOnDestroy(): void { this.sub?.unsubscribe(); }
  load(): void {
    this.error = false; this.loading = true;
    this.apiService.getGeographic(this.filterService.filters).subscribe({ next: (d) => { this.data = d as unknown as Record<string, unknown>; this.buildCharts(); this.loading = false; }, error: () => { this.error = true; this.loading = false; } });
  }
  private buildCharts(): void {
    // kpis.onsite_offshore: { onsite_pct, offshore_pct }
    const kpis = (this.data?.['kpis'] as Record<string, Record<string, number>>) ?? {};
    const oo = kpis['onsite_offshore'] ?? {};
    this.donutChart = {
      labels: ['Offshore', 'Onsite'],
      datasets: [{ data: [oo['offshore_pct'] ?? 0, oo['onsite_pct'] ?? 0], backgroundColor: ['#1B2559', '#00BCD4'] }],
    };
    // trend_mix: { month, onsite_offsite, offshore_mix }
    const trend = (this.data?.['trend_mix'] as Array<{ month: string; onsite_offsite: number; offshore_mix: number }>) ?? [];
    this.trendChart = {
      labels: trend.map((t) => t.month),
      datasets: [
        { data: trend.map((t) => t.onsite_offsite), backgroundColor: '#00BCD4', label: 'Onsite' },
        { data: trend.map((t) => t.offshore_mix), backgroundColor: '#1B2559', label: 'Offshore' },
      ],
    };
  }
  get summary(): Record<string, number> {
    const kpis = (this.data?.['kpis'] as Record<string, Record<string, number>>) ?? {};
    const oo = kpis['onsite_offshore'] ?? {};
    return { total: 47420, onsite_pct: oo['onsite_pct'] ?? 38, offshore_pct: oo['offshore_pct'] ?? 62 };
  }
  get countryData(): Record<string, unknown>[] { return (this.data?.['countrywise_demand'] as Record<string, unknown>[]) ?? []; }
}
