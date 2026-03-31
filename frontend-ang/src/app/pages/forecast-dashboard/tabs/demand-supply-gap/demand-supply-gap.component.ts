import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { BaseChartDirective } from 'ng2-charts';
import { ChartConfiguration } from 'chart.js';
import { Subscription } from 'rxjs';
import { switchMap } from 'rxjs/operators';
import { ApiService } from '../../../../core/services/api.service';
import { FilterService } from '../../../../core/services/filter.service';
import { DataGridComponent, GridColumn } from '../../../../shared/components/data-grid/data-grid.component';
import { StatusBadgeComponent } from '../../../../shared/components/status-badge/status-badge.component';
import { LoadingSkeletonComponent } from '../../../../shared/components/loading-skeleton/loading-skeleton.component';
import { ErrorCardComponent } from '../../../../shared/components/error-card/error-card.component';

@Component({
  selector: 'app-demand-supply-gap',
  standalone: true,
  imports: [CommonModule, BaseChartDirective, DataGridComponent, StatusBadgeComponent, LoadingSkeletonComponent, ErrorCardComponent],
  template: `
    <app-loading-skeleton *ngIf="loading" type="page"></app-loading-skeleton>
    <app-error-card *ngIf="error && !loading" (onRetry)="load()"></app-error-card>
    <div *ngIf="!loading && !error && data" class="space-y-5">
      <h2 class="text-lg font-bold text-gray-900">Demand-Supply Gap</h2>
      <div class="grid grid-cols-2 lg:grid-cols-3 gap-4">
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-4 text-center">
          <div class="text-xs text-gray-500 uppercase font-medium">Fulfillment Gap %</div>
          <div class="text-2xl font-bold text-red-500 mt-1">{{ kpis['fulfillment_gap_pct'] }}%</div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-4 text-center">
          <div class="text-xs text-gray-500 uppercase font-medium">Critical Skill Shortage</div>
          <div class="text-2xl font-bold text-orange-500 mt-1">{{ kpis['critical_skill_shortage'] }}</div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-4 text-center">
          <div class="text-xs text-gray-500 uppercase font-medium">Avg Fulfillment Days</div>
          <div class="text-2xl font-bold text-[#1B2559] mt-1">{{ kpis['fulfillment_time_days'] }}</div>
        </div>
      </div>
      <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
        <h3 class="text-sm font-semibold text-gray-700 mb-3">Demand vs Supply Trend</h3>
        <div style="height:200px"><canvas baseChart [data]="trendChart" [type]="'line'" [options]="lineOptions"></canvas></div>
      </div>
      <app-data-grid [data]="gapGrid" [columns]="columns" [pageSize]="9" downloadFilename="demand-supply-gap.csv"></app-data-grid>
    </div>
  `,
})
export class DemandSupplyGapComponent implements OnInit, OnDestroy {
  loading = true; error = false; data: Record<string, unknown> | null = null;
  private sub?: Subscription;
  trendChart: ChartConfiguration<'line'>['data'] = { labels: [], datasets: [] };
  lineOptions = { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { font: { size: 10 } } } }, scales: { x: { ticks: { font: { size: 10 } } }, y: { ticks: { font: { size: 10 } } } } };
  columns: GridColumn[] = [
    { key: 'cluster', header: 'Skill Cluster' }, { key: 'so_grade', header: 'Grade' },
    { key: 'gap', header: 'Gap', type: 'number' }, { key: 'gap_pct', header: 'Gap %' },
    { key: 'status', header: 'Status', type: 'badge' },
  ];

  constructor(private apiService: ApiService, private filterService: FilterService) {}
  ngOnInit(): void {
    this.sub = this.filterService.filters$.pipe(switchMap((f) => { this.loading = true; return this.apiService.getDemandSupplyGap(f); }))
      .subscribe({ next: (d) => { this.data = d as unknown as Record<string, unknown>; this.buildCharts(); this.loading = false; }, error: () => { this.error = true; this.loading = false; } });
  }
  ngOnDestroy(): void { this.sub?.unsubscribe(); }
  load(): void {
    this.error = false; this.loading = true;
    this.apiService.getDemandSupplyGap(this.filterService.filters).subscribe({ next: (d) => { this.data = d as unknown as Record<string, unknown>; this.buildCharts(); this.loading = false; }, error: () => { this.error = true; this.loading = false; } });
  }
  private buildCharts(): void {
    // demand_supply_trend: { month, demand_fte, supply_fte }
    const trend = (this.data?.['demand_supply_trend'] as Array<{ month: string; demand_fte: number; supply_fte: number }>) ?? [];
    this.trendChart = {
      labels: trend.map((t) => t.month),
      datasets: [
        { data: trend.map((t) => t.demand_fte), borderColor: '#1B2559', borderWidth: 2, fill: false, tension: 0.3, label: 'Demand', pointRadius: 0 },
        { data: trend.map((t) => t.supply_fte), borderColor: '#00BCD4', borderWidth: 2, fill: false, tension: 0.3, label: 'Supply', pointRadius: 0 },
        { data: trend.map((t) => t.demand_fte - t.supply_fte), borderColor: '#EF4444', borderWidth: 2, borderDash: [4, 2], fill: false, tension: 0.3, label: 'Gap', pointRadius: 0 },
      ],
    };
  }
  get kpis(): Record<string, number> { return (this.data?.['kpis'] as Record<string, number>) ?? {}; }
  get gapGrid(): Record<string, unknown>[] { return (this.data?.['grid_gap'] as Record<string, unknown>[]) ?? []; }
}
