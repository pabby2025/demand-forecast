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
  selector: 'app-grade-distribution',
  standalone: true,
  imports: [CommonModule, BaseChartDirective, DataGridComponent, LoadingSkeletonComponent, ErrorCardComponent],
  template: `
    <app-loading-skeleton *ngIf="loading" type="page"></app-loading-skeleton>
    <app-error-card *ngIf="error && !loading" (onRetry)="load()"></app-error-card>
    <div *ngIf="!loading && !error && data" class="space-y-5">
      <h2 class="text-lg font-bold text-gray-900">Grade Distribution</h2>
      <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">Grade Mix</h3>
          <div style="height:200px"><canvas baseChart [data]="donutChart" [type]="'doughnut'" [options]="donutOptions"></canvas></div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-4 text-center flex flex-col items-center justify-center">
          <div class="text-xs text-gray-500 uppercase font-medium mb-2">Short-Fuse Total</div>
          <div class="text-4xl font-bold text-red-500">{{ shortfuseTotal | number }}</div>
          <div class="text-xs text-gray-400 mt-1">demands &lt; 30 days</div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">Grade Trend</h3>
          <div style="height:200px"><canvas baseChart [data]="trendChart" [type]="'bar'" [options]="barOptions"></canvas></div>
        </div>
      </div>
      <app-data-grid [data]="gradeData" [columns]="columns" [pageSize]="10" downloadFilename="grade-distribution.csv"></app-data-grid>
    </div>
  `,
})
export class GradeDistributionComponent implements OnInit, OnDestroy {
  loading = true; error = false; data: Record<string, unknown> | null = null;
  private sub?: Subscription;
  donutChart: ChartConfiguration<'doughnut'>['data'] = { labels: [], datasets: [] };
  trendChart: ChartConfiguration<'bar'>['data'] = { labels: [], datasets: [] };
  donutOptions: ChartConfiguration<'doughnut'>['options'] = { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { font: { size: 10 } } } } };
  barOptions = { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { font: { size: 9 } } } }, scales: { x: { stacked: true, ticks: { font: { size: 10 } } }, y: { stacked: true, ticks: { font: { size: 10 } } } } };
  columns: GridColumn[] = [
    { key: 'grade', header: 'Grade' }, { key: 'label', header: 'Label' },
    { key: 'count', header: 'Count', type: 'number' }, { key: 'pct', header: 'Pct %' },
    { key: 'shortfuse', header: 'Short-Fuse', type: 'number' }, { key: 'shortfuse_pct', header: 'SF %' },
    { key: 'billable_pct', header: 'Billable %' },
  ];

  constructor(private apiService: ApiService, private filterService: FilterService) {}
  ngOnInit(): void {
    this.sub = this.filterService.filters$.pipe(switchMap((f) => { this.loading = true; return this.apiService.getGradeDistribution(f); }))
      .subscribe({ next: (d) => { this.data = d as unknown as Record<string, unknown>; this.buildCharts(); this.loading = false; }, error: () => { this.error = true; this.loading = false; } });
  }
  ngOnDestroy(): void { this.sub?.unsubscribe(); }
  load(): void {
    this.error = false; this.loading = true;
    this.apiService.getGradeDistribution(this.filterService.filters).subscribe({ next: (d) => { this.data = d as unknown as Record<string, unknown>; this.buildCharts(); this.loading = false; }, error: () => { this.error = true; this.loading = false; } });
  }
  private buildCharts(): void {
    const donut = (this.data?.['donut'] as Array<{ name: string; value: number }>) ?? [];
    this.donutChart = { labels: donut.map((d) => d.name), datasets: [{ data: donut.map((d) => d.value), backgroundColor: ['#1B2559', '#00BCD4', '#7C3AED', '#F59E0B', '#EF4444', '#10B981'] }] };
    const trend = (this.data?.['grade_trend'] as Array<Record<string, string | number>>) ?? [];
    const keys = trend.length ? Object.keys(trend[0]).filter((k) => k !== 'month') : [];
    this.trendChart = { labels: trend.map((t) => String(t['month'])), datasets: keys.map((k, i) => ({ data: trend.map((t) => Number(t[k])), backgroundColor: ['#1B2559', '#00BCD4', '#7C3AED', '#F59E0B', '#EF4444', '#10B981'][i % 6], label: k })) };
  }
  get gradeData(): Record<string, unknown>[] { return (this.data?.['grade_data'] as Record<string, unknown>[]) ?? []; }
  get shortfuseTotal(): number { return (this.data?.['shortfuse_total'] as number) ?? 0; }
}
