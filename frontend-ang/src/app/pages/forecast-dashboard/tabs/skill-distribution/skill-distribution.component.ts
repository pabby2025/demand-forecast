import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { BaseChartDirective } from 'ng2-charts';
import { ChartConfiguration } from 'chart.js';
import { Subscription } from 'rxjs';
import { switchMap } from 'rxjs/operators';
import { ApiService } from '../../../../core/services/api.service';
import { FilterService } from '../../../../core/services/filter.service';
import { DataGridComponent, GridColumn } from '../../../../shared/components/data-grid/data-grid.component';
import { HeatmapComponent, HeatmapRow } from '../../../../shared/components/heatmap/heatmap.component';
import { LoadingSkeletonComponent } from '../../../../shared/components/loading-skeleton/loading-skeleton.component';
import { ErrorCardComponent } from '../../../../shared/components/error-card/error-card.component';

type D = Record<string, unknown>;

@Component({
  selector: 'app-skill-distribution',
  standalone: true,
  imports: [CommonModule, BaseChartDirective, DataGridComponent, HeatmapComponent, LoadingSkeletonComponent, ErrorCardComponent],
  template: `
    <app-loading-skeleton *ngIf="loading" type="page"></app-loading-skeleton>
    <app-error-card *ngIf="error && !loading" (onRetry)="load()"></app-error-card>

    <div *ngIf="!loading && !error && data" class="space-y-5">
      <!-- KPI Row: 4 Cards -->
      <div class="grid grid-cols-1 lg:grid-cols-4 gap-4">
        <!-- Top Clusters in Practice -->
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Top Clusters (Practice)</h3>
          <div class="space-y-2">
            <div *ngFor="let c of topInPractice" class="flex items-center justify-between gap-2 text-xs">
              <span class="text-gray-600 truncate max-w-[120px]" [title]="c['name']">{{ c['name'] }}</span>
              <div class="flex items-center gap-1.5 flex-shrink-0">
                <div class="w-16 bg-gray-100 rounded-full h-1.5">
                  <div class="h-1.5 rounded-full bg-[#1B2559]" [style.width.%]="c['pct']"></div>
                </div>
                <span class="font-semibold text-gray-800">{{ c['pct'] }}%</span>
              </div>
            </div>
          </div>
        </div>

        <!-- Top Clusters in SL -->
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Top Clusters (in SL)</h3>
          <div class="space-y-2">
            <div *ngFor="let c of topInSl" class="flex items-center justify-between gap-2 text-xs">
              <span class="text-gray-600 truncate max-w-[120px]" [title]="c['name']">{{ c['name'] }}</span>
              <div class="flex items-center gap-1.5 flex-shrink-0">
                <div class="w-16 bg-gray-100 rounded-full h-1.5">
                  <div class="h-1.5 rounded-full bg-[#00BCD4]" [style.width.%]="c['pct']"></div>
                </div>
                <span class="font-semibold text-gray-800">{{ c['pct'] }}%</span>
              </div>
            </div>
          </div>
        </div>

        <!-- Stable vs Volatile Donut -->
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Stable vs Volatile</h3>
          <div style="height:160px"><canvas baseChart [data]="stableDonut" [type]="'doughnut'" [options]="donutOpts"></canvas></div>
        </div>

        <!-- Top Drivers List -->
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Top Drivers</h3>
          <ul class="space-y-2">
            <li *ngFor="let d of topDrivers; let i = index" class="flex gap-2 text-xs text-gray-600">
              <span class="flex-shrink-0 w-4 h-4 rounded-full bg-[#1B2559] text-white flex items-center justify-center font-bold text-[9px]">{{ i + 1 }}</span>
              <span>{{ d }}</span>
            </li>
          </ul>
        </div>
      </div>

      <!-- Charts Row -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <!-- Short Fuse Heatmap -->
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <div class="flex items-center justify-between mb-3">
            <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider">Short Fuse Demand Heatmap</h3>
            <span class="text-xs text-gray-400">Total: <span class="font-semibold text-gray-700">{{ sfTotal | number }}</span></span>
          </div>
          <app-heatmap [data]="sfHeatmapRows" [months]="heatmapMonths" rowLabel="Cluster"></app-heatmap>
        </div>

        <!-- Stable/Volatile Trend Line -->
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">% Stable &amp; Volatile Trend</h3>
          <div style="height:220px"><canvas baseChart [data]="stabilityTrendChart" [type]="'line'" [options]="lineOpts"></canvas></div>
        </div>
      </div>

      <!-- Grid 1: Skill Cluster Demand Overview -->
      <div>
        <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2 px-1">Skill Cluster Demand Overview</h3>
        <app-data-grid [data]="clusterDemandGrid" [columns]="clusterColumns" [pageSize]="10" downloadFilename="skill-cluster-demand.csv"></app-data-grid>
      </div>

      <!-- Grid 2: Demand Stability Analysis -->
      <div>
        <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2 px-1">Demand Stability Analysis</h3>
        <app-data-grid [data]="stabilityGrid" [columns]="stabilityColumns" [pageSize]="10" downloadFilename="skill-stability.csv"></app-data-grid>
      </div>
    </div>
  `,
})
export class SkillDistributionComponent implements OnInit, OnDestroy {
  loading = true; error = false; data: D | null = null;
  private sub?: Subscription;

  stableDonut: ChartConfiguration<'doughnut'>['data'] = { labels: [], datasets: [] };
  stabilityTrendChart: ChartConfiguration<'line'>['data'] = { labels: [], datasets: [] };

  donutOpts: ChartConfiguration<'doughnut'>['options'] = {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { position: 'bottom', labels: { font: { size: 9 }, boxWidth: 10, padding: 6 } } },
    cutout: '65%',
  };
  lineOpts = {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { labels: { font: { size: 9 }, boxWidth: 10 } } },
    scales: { x: { ticks: { font: { size: 9 } } }, y: { min: 0, max: 100, ticks: { font: { size: 9 }, callback: (v: number | string) => `${v}%` } } },
  };

  sfHeatmapRows: HeatmapRow[] = [];
  heatmapMonths = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'];
  sfTotal = 0;

  clusterColumns: GridColumn[] = [
    { key: 'practice_area', header: 'Practice Area' }, { key: 'location', header: 'Location' },
    { key: 'cluster', header: 'Skill Cluster' }, { key: 'so_grade', header: 'Grade' },
    { key: 'leaf_skill', header: 'Leaf Skill' },
    { key: 'jan', header: 'Jan', type: 'number' }, { key: 'feb', header: 'Feb', type: 'number' },
    { key: 'mar', header: 'Mar', type: 'number' }, { key: 'apr', header: 'Apr', type: 'number' },
    { key: 'may', header: 'May', type: 'number' }, { key: 'jun', header: 'Jun', type: 'number' },
  ];
  stabilityColumns: GridColumn[] = [
    { key: 'practice_area', header: 'Practice Area' }, { key: 'location', header: 'Location' },
    { key: 'cluster', header: 'Skill Cluster' }, { key: 'so_grade', header: 'Grade' },
    { key: 'stability', header: 'Demand Type' },
    { key: 'jan', header: 'Jan', type: 'number' }, { key: 'feb', header: 'Feb', type: 'number' },
    { key: 'mar', header: 'Mar', type: 'number' }, { key: 'apr', header: 'Apr', type: 'number' },
    { key: 'may', header: 'May', type: 'number' }, { key: 'jun', header: 'Jun', type: 'number' },
  ];

  constructor(private apiService: ApiService, private filterService: FilterService) {}

  ngOnInit(): void {
    this.sub = this.filterService.filters$.pipe(
      switchMap((f) => { this.loading = true; this.error = false; return this.apiService.getSkillDistribution(f); })
    ).subscribe({
      next: (d) => { this.data = d as unknown as D; this.buildCharts(); this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  ngOnDestroy(): void { this.sub?.unsubscribe(); }

  load(): void {
    this.error = false; this.loading = true;
    this.apiService.getSkillDistribution(this.filterService.filters).subscribe({
      next: (d) => { this.data = d as unknown as D; this.buildCharts(); this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  private buildCharts(): void {
    const kpis = (this.data?.['kpis'] as D) ?? {};
    const svd = (kpis['stable_vs_volatile'] as { stable_pct: number; volatile_pct: number }) ?? { stable_pct: 70, volatile_pct: 30 };
    this.stableDonut = {
      labels: ['Stable', 'Volatile'],
      datasets: [{ data: [svd.stable_pct, svd.volatile_pct], backgroundColor: ['#10B981', '#F59E0B'], borderWidth: 0 }],
    };

    const trend = (this.data?.['stability_trend'] as Array<{ month: string; stable: number; volatile: number }>) ?? [];
    this.stabilityTrendChart = {
      labels: trend.map((t) => t.month),
      datasets: [
        { data: trend.map((t) => t.stable), borderColor: '#10B981', borderWidth: 2, fill: false, tension: 0.3, label: 'Stable %', pointRadius: 0 },
        { data: trend.map((t) => t.volatile), borderColor: '#F59E0B', borderWidth: 2, fill: false, tension: 0.3, label: 'Volatile %', pointRadius: 0 },
      ],
    };

    const sf = (this.data?.['short_fuse_heatmap'] as { total: number; clusters: Array<{ name: string; monthly: number[] }> }) ?? { total: 0, clusters: [] };
    this.sfTotal = sf.total;
    this.sfHeatmapRows = sf.clusters.map((c) => ({ name: c.name, monthly: c.monthly }));
  }

  get topInPractice(): D[] {
    const kpis = (this.data?.['kpis'] as D) ?? {};
    return (kpis['top_clusters_in_practice'] as D[]) ?? [];
  }
  get topInSl(): D[] {
    const kpis = (this.data?.['kpis'] as D) ?? {};
    return (kpis['top_clusters_in_sl'] as D[]) ?? [];
  }
  get topDrivers(): string[] {
    const kpis = (this.data?.['kpis'] as D) ?? {};
    return (kpis['top_drivers'] as string[]) ?? [];
  }
  get clusterDemandGrid(): D[] { return (this.data?.['grid_cluster_demand'] as D[]) ?? []; }
  get stabilityGrid(): D[] { return (this.data?.['grid_stability'] as D[]) ?? []; }
}
