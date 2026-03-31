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
  selector: 'app-skill-distribution',
  standalone: true,
  imports: [CommonModule, BaseChartDirective, DataGridComponent, LoadingSkeletonComponent, ErrorCardComponent],
  template: `
    <app-loading-skeleton *ngIf="loading" type="page"></app-loading-skeleton>
    <app-error-card *ngIf="error && !loading" (onRetry)="load()"></app-error-card>
    <div *ngIf="!loading && !error && data" class="space-y-5">
      <h2 class="text-lg font-bold text-gray-900">Skill Distribution</h2>
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">Demand by Cluster</h3>
          <div style="height:220px"><canvas baseChart [data]="barChart" [type]="'bar'" [options]="barOptions"></canvas></div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">Stability Trend</h3>
          <div style="height:220px"><canvas baseChart [data]="trendChart" [type]="'line'" [options]="lineOptions"></canvas></div>
        </div>
      </div>
      <app-data-grid [data]="clusterData" [columns]="columns" [pageSize]="9" downloadFilename="skill-distribution.csv"></app-data-grid>
    </div>
  `,
})
export class SkillDistributionComponent implements OnInit, OnDestroy {
  loading = true; error = false; data: Record<string, unknown> | null = null;
  private sub?: Subscription;
  barChart: ChartConfiguration<'bar'>['data'] = { labels: [], datasets: [] };
  trendChart: ChartConfiguration<'line'>['data'] = { labels: [], datasets: [] };
  barOptions = { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { ticks: { font: { size: 8 }, maxRotation: 45 } }, y: { ticks: { font: { size: 10 } } } } };
  lineOptions = { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { font: { size: 9 } } } }, scales: { x: { ticks: { font: { size: 9 } } }, y: { ticks: { font: { size: 10 } } } } };
  columns: GridColumn[] = [
    { key: 'cluster', header: 'Cluster' }, { key: 'so_grade', header: 'Grade' },
    { key: 'stability', header: 'Stability' }, { key: 'leaf_skill', header: 'Lead Skill' },
    { key: 'jan', header: 'Jan', type: 'number' }, { key: 'feb', header: 'Feb', type: 'number' },
    { key: 'mar', header: 'Mar', type: 'number' },
  ];

  constructor(private apiService: ApiService, private filterService: FilterService) {}
  ngOnInit(): void {
    this.sub = this.filterService.filters$.pipe(switchMap((f) => { this.loading = true; return this.apiService.getSkillDistribution(f); }))
      .subscribe({ next: (d) => { this.data = d as unknown as Record<string, unknown>; this.buildCharts(); this.loading = false; }, error: () => { this.error = true; this.loading = false; } });
  }
  ngOnDestroy(): void { this.sub?.unsubscribe(); }
  load(): void {
    this.error = false; this.loading = true;
    this.apiService.getSkillDistribution(this.filterService.filters).subscribe({ next: (d) => { this.data = d as unknown as Record<string, unknown>; this.buildCharts(); this.loading = false; }, error: () => { this.error = true; this.loading = false; } });
  }
  private buildCharts(): void {
    // Build bar chart from kpis.top_clusters_in_practice
    const kpis = this.data?.['kpis'] as Record<string, unknown> | undefined;
    const topClusters = (kpis?.['top_clusters_in_practice'] as Array<{ name: string; pct: number }>) ?? [];
    this.barChart = { labels: topClusters.map((b) => b.name), datasets: [{ data: topClusters.map((b) => b.pct), backgroundColor: '#1B2559', label: 'Demand %', borderRadius: 4 }] };
    // Stability trend: { month, stable, volatile }
    const trend = (this.data?.['stability_trend'] as Array<Record<string, string | number>>) ?? [];
    const keys = trend.length ? Object.keys(trend[0]).filter((k) => k !== 'month') : [];
    this.trendChart = { labels: trend.map((t) => String(t['month'])), datasets: keys.slice(0, 5).map((k, i) => ({ data: trend.map((t) => Number(t[k])), borderColor: ['#1B2559', '#00BCD4', '#7C3AED', '#F59E0B', '#EF4444'][i], borderWidth: 2, fill: false, tension: 0.3, label: k, pointRadius: 0 })) };
  }
  get clusterData(): Record<string, unknown>[] { return (this.data?.['grid_stability'] as Record<string, unknown>[]) ?? []; }
}
