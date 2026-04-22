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
  selector: 'app-demand-supply-gap',
  standalone: true,
  imports: [CommonModule, BaseChartDirective, DataGridComponent, HeatmapComponent, LoadingSkeletonComponent, ErrorCardComponent],
  template: `
    <app-loading-skeleton *ngIf="loading" type="page"></app-loading-skeleton>
    <app-error-card *ngIf="error && !loading" (onRetry)="load()"></app-error-card>

    <div *ngIf="!loading && !error && data" class="space-y-5">
      <!-- KPI Row -->
      <div class="grid grid-cols-3 gap-4">
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <div class="flex items-start justify-between mb-2">
            <div>
              <div class="text-xs font-semibold text-gray-700">Fulfillment Gap</div>
              <div class="text-xs text-gray-400">Supply vs Demand</div>
            </div>
            <svg class="w-4 h-4 text-gray-300" fill="none" stroke="currentColor" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" stroke-width="2"/><line x1="12" y1="8" x2="12" y2="12" stroke-width="2"/><line x1="12" y1="16" x2="12.01" y2="16" stroke-width="2"/></svg>
          </div>
          <div class="text-4xl font-bold text-[#1B2559] mt-1">{{ kpis['fulfillment_gap_pct'] }}<span class="text-lg font-semibold text-gray-400 ml-1">%</span></div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <div class="flex items-start justify-between mb-2">
            <div>
              <div class="text-xs font-semibold text-gray-700">Critical Skill Shortage</div>
              <div class="text-xs text-gray-400">Micro Clusters with &gt;10 unfulfilled FTE</div>
            </div>
            <svg class="w-4 h-4 text-gray-300" fill="none" stroke="currentColor" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" stroke-width="2"/><line x1="12" y1="8" x2="12" y2="12" stroke-width="2"/><line x1="12" y1="16" x2="12.01" y2="16" stroke-width="2"/></svg>
          </div>
          <div class="flex items-end gap-2 mt-1">
            <div class="text-4xl font-bold text-[#1B2559]">{{ kpis['critical_skill_shortage'] }}</div>
            <div class="text-xs text-gray-400 mb-1">Skill Micro Clusters</div>
          </div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <div class="flex items-start justify-between mb-2">
            <div>
              <div class="text-xs font-semibold text-gray-700">Fulfillment Time</div>
              <div class="text-xs text-gray-400">Days to fulfil staff demand</div>
            </div>
            <svg class="w-4 h-4 text-gray-300" fill="none" stroke="currentColor" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" stroke-width="2"/><line x1="12" y1="8" x2="12" y2="12" stroke-width="2"/><line x1="12" y1="16" x2="12.01" y2="16" stroke-width="2"/></svg>
          </div>
          <div class="flex items-end gap-2 mt-1">
            <div class="text-4xl font-bold text-[#1B2559]">{{ kpis['fulfillment_time_days'] }}</div>
            <div class="text-xs text-gray-400 mb-1">days</div>
          </div>
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
          <app-heatmap [data]="sfHeatmapRows" [months]="heatmapMonths" rowLabel="Cluster" colorScheme="blue"></app-heatmap>
        </div>

        <!-- Demand vs Supply Trend -->
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Demand &amp; Supply Gap Trend</h3>
          <div style="height:220px"><canvas baseChart [data]="trendChart" [type]="'line'" [options]="lineOptions"></canvas></div>
        </div>
      </div>

      <!-- Grid 1: Short Fuse Demand -->
      <div>
        <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2 px-1">Short Fuse Demand</h3>
        <app-data-grid [data]="sfGrid" [columns]="sfColumns" [pageSize]="9" downloadFilename="shortfuse-demand.csv"></app-data-grid>
      </div>

      <!-- Grid 2: Demand & Supply Gap -->
      <div>
        <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2 px-1">Demand &amp; Supply Gap by Cluster</h3>
        <app-data-grid [data]="gapGrid" [columns]="gapColumns" [pageSize]="9" downloadFilename="demand-supply-gap.csv"></app-data-grid>
      </div>
    </div>
  `,
})
export class DemandSupplyGapComponent implements OnInit, OnDestroy {
  loading = true; error = false; data: D | null = null;
  private sub?: Subscription;

  trendChart: ChartConfiguration<'line'>['data'] = { labels: [], datasets: [] };
  lineOptions = {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { labels: { font: { size: 9 }, boxWidth: 10 } } },
    scales: { x: { ticks: { font: { size: 9 } } }, y: { ticks: { font: { size: 9 }, callback: (v: number | string) => Number(v).toLocaleString() } } },
  };

  sfHeatmapRows: HeatmapRow[] = [];
  heatmapMonths = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'];
  sfTotal = 0;

  sfColumns: GridColumn[] = [
    { key: 'practice_area', header: 'Practice Area' }, { key: 'location', header: 'Location' },
    { key: 'cluster', header: 'Skill Cluster' }, { key: 'so_grade', header: 'Grade' },
    { key: 'jan', header: 'Jan', type: 'number' }, { key: 'feb', header: 'Feb', type: 'number' },
    { key: 'mar', header: 'Mar', type: 'number' }, { key: 'q01', header: 'Q 01', type: 'number', isQuarterly: true },
    { key: 'apr', header: 'Apr', type: 'number' }, { key: 'may', header: 'May', type: 'number' },
    { key: 'jun', header: 'Jun', type: 'number' }, { key: 'q02', header: 'Q 02', type: 'number', isQuarterly: true },
  ];
  gapColumns: GridColumn[] = [
    { key: 'practice_area', header: 'Practice Area' }, { key: 'location', header: 'Location' },
    { key: 'cluster', header: 'Skill Cluster' }, { key: 'so_grade', header: 'Grade' },
    { key: 'jan_demand', header: 'Jan Demand', type: 'number' }, { key: 'jan_supply', header: 'Jan Supply', type: 'number' },
    { key: 'feb_demand', header: 'Feb Demand', type: 'number' }, { key: 'feb_supply', header: 'Feb Supply', type: 'number' },
    { key: 'mar_demand', header: 'Mar Demand', type: 'number' }, { key: 'mar_supply', header: 'Mar Supply', type: 'number' },
    { key: 'q01_demand', header: 'Q 01 Demand', type: 'number', isQuarterly: true }, { key: 'q01_supply', header: 'Q 01 Supply', type: 'number', isQuarterly: true },
    { key: 'gap', header: 'Total Gap', type: 'number' }, { key: 'gap_pct', header: 'Gap %' },
  ];

  constructor(private apiService: ApiService, private filterService: FilterService) {}

  ngOnInit(): void {
    this.sub = this.filterService.filters$.pipe(
      switchMap((f) => { this.loading = true; this.error = false; return this.apiService.getDemandSupplyGap(f); })
    ).subscribe({
      next: (d) => { this.data = d as unknown as D; this.buildCharts(); this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  ngOnDestroy(): void { this.sub?.unsubscribe(); }

  load(): void {
    this.error = false; this.loading = true;
    this.apiService.getDemandSupplyGap(this.filterService.filters).subscribe({
      next: (d) => { this.data = d as unknown as D; this.buildCharts(); this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  private buildCharts(): void {
    const trend = (this.data?.['demand_supply_trend'] as Array<{ month: string; demand_fte: number; supply_fte: number }>) ?? [];
    this.trendChart = {
      labels: trend.map((t) => t.month),
      datasets: [
        { data: trend.map((t) => t.demand_fte), borderColor: '#1B2559', borderWidth: 2, fill: false, tension: 0.3, label: 'Demand FTE', pointRadius: 0 },
        { data: trend.map((t) => t.supply_fte), borderColor: '#00BCD4', borderWidth: 2, fill: false, tension: 0.3, label: 'Supply FTE', pointRadius: 0 },
        { data: trend.map((t) => t.demand_fte - t.supply_fte), borderColor: '#EF4444', borderWidth: 2, borderDash: [4, 2], fill: false, tension: 0.3, label: 'Gap', pointRadius: 0 },
      ],
    };

    const heatmap = (this.data?.['heatmap'] as { total: number; clusters: Array<{ name: string; monthly: number[] }> }) ?? { total: 0, clusters: [] };
    this.sfTotal = heatmap.total;
    this.sfHeatmapRows = heatmap.clusters.map((c) => ({ name: c.name, monthly: c.monthly }));
  }

  get kpis(): Record<string, number> { return (this.data?.['kpis'] as Record<string, number>) ?? {}; }
  get sfGrid(): D[] { return (this.data?.['grid_short_fuse'] as D[]) ?? []; }
  get gapGrid(): D[] { return (this.data?.['grid_gap'] as D[]) ?? []; }
}
