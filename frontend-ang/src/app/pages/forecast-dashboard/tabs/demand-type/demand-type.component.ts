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
  selector: 'app-demand-type',
  standalone: true,
  imports: [CommonModule, BaseChartDirective, DataGridComponent, LoadingSkeletonComponent, ErrorCardComponent],
  template: `
    <app-loading-skeleton *ngIf="loading" type="page"></app-loading-skeleton>
    <app-error-card *ngIf="error && !loading" (onRetry)="load()"></app-error-card>

    <div *ngIf="!loading && !error && data" class="space-y-5">
      <div class="flex items-center justify-between">
        <h2 class="text-lg font-bold text-gray-900">Demand Type Breakdown</h2>
      </div>
      <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">New vs Backfill</h3>
          <div style="height:200px"><canvas baseChart [data]="donutChart" [type]="'doughnut'" [options]="donutOptions"></canvas></div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">Billability Mix</h3>
          <div style="height:200px"><canvas baseChart [data]="billabilityChart" [type]="'doughnut'" [options]="donutOptions"></canvas></div>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">Billability Trend</h3>
          <div style="height:200px"><canvas baseChart [data]="trendChart" [type]="'bar'" [options]="barOptions"></canvas></div>
        </div>
      </div>
      <app-data-grid [data]="typeGrid" [columns]="typeColumns" [pageSize]="10" downloadFilename="demand-type.csv"></app-data-grid>
      <app-data-grid [data]="buGrid" [columns]="buColumns" [pageSize]="10" downloadFilename="demand-type-bu.csv"></app-data-grid>
    </div>
  `,
})
export class DemandTypeComponent implements OnInit, OnDestroy {
  loading = true; error = false; data: Record<string, unknown> | null = null;
  private sub?: Subscription;

  donutChart: ChartConfiguration<'doughnut'>['data'] = { labels: [], datasets: [] };
  billabilityChart: ChartConfiguration<'doughnut'>['data'] = { labels: [], datasets: [] };
  trendChart: ChartConfiguration<'bar'>['data'] = { labels: [], datasets: [] };

  donutOptions: ChartConfiguration<'doughnut'>['options'] = {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { position: 'bottom', labels: { font: { size: 10 } } } },
  };
  barOptions = { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { font: { size: 10 } } } }, scales: { x: { ticks: { font: { size: 10 } } }, y: { stacked: true, ticks: { font: { size: 10 } } } } };

  typeColumns: GridColumn[] = [
    { key: 'demand_type', header: 'Demand Type' }, { key: 'practice_area', header: 'Practice Area' },
    { key: 'cluster', header: 'Skill Cluster' }, { key: 'so_grade', header: 'Grade' },
    { key: 'jan', header: 'Jan', type: 'number' }, { key: 'feb', header: 'Feb', type: 'number' },
    { key: 'mar', header: 'Mar', type: 'number' }, { key: 'q01', header: 'Q1', type: 'number' },
  ];
  buColumns: GridColumn[] = [
    { key: 'billability_type', header: 'Billability' }, { key: 'practice_area', header: 'Practice Area' },
    { key: 'cluster', header: 'Skill Cluster' }, { key: 'so_grade', header: 'Grade' },
    { key: 'jan', header: 'Jan', type: 'number' }, { key: 'feb', header: 'Feb', type: 'number' },
    { key: 'mar', header: 'Mar', type: 'number' },
  ];

  constructor(private apiService: ApiService, private filterService: FilterService) {}

  ngOnInit(): void {
    this.sub = this.filterService.filters$.pipe(switchMap((f) => { this.loading = true; return this.apiService.getDemandType(f); }))
      .subscribe({ next: (d) => { this.data = d as unknown as Record<string, unknown>; this.buildCharts(); this.loading = false; }, error: () => { this.error = true; this.loading = false; } });
  }

  ngOnDestroy(): void { this.sub?.unsubscribe(); }

  load(): void {
    this.error = false; this.loading = true;
    this.apiService.getDemandType(this.filterService.filters).subscribe({ next: (d) => { this.data = d as unknown as Record<string, unknown>; this.buildCharts(); this.loading = false; }, error: () => { this.error = true; this.loading = false; } });
  }

  private buildCharts(): void {
    // kpis.new_vs_backfill: { new_pct, backfill_pct }
    const kpis = (this.data?.['kpis'] as Record<string, Record<string, number>>) ?? {};
    const nvb = kpis['new_vs_backfill'] ?? {};
    this.donutChart = {
      labels: ['New Demand', 'Backfill'],
      datasets: [{ data: [nvb['new_pct'] ?? 0, nvb['backfill_pct'] ?? 0], backgroundColor: ['#1B2559', '#00BCD4'] }],
    };
    // kpis.contract_type_mix: { t_and_m_pct, fixed_price_pct, transaction_based_pct }
    const ctm = kpis['contract_type_mix'] ?? {};
    this.billabilityChart = {
      labels: ['BFD (T&M)', 'BTB (Fixed)', 'BTM (Txn)'],
      datasets: [{ data: [ctm['t_and_m_pct'] ?? 0, ctm['fixed_price_pct'] ?? 0, ctm['transaction_based_pct'] ?? 0], backgroundColor: ['#00BCD4', '#7C3AED', '#F59E0B'] }],
    };
    // trend_billability: { month, bfd, btb, btm }
    const trend = (this.data?.['trend_billability'] as Array<{ month: string; bfd: number; btb: number; btm: number }>) ?? [];
    this.trendChart = {
      labels: trend.map((t) => t.month),
      datasets: [
        { data: trend.map((t) => t.bfd), backgroundColor: '#00BCD4', label: 'BFD' },
        { data: trend.map((t) => t.btb), backgroundColor: '#7C3AED', label: 'BTB' },
        { data: trend.map((t) => t.btm), backgroundColor: '#F59E0B', label: 'BTM' },
      ],
    };
  }

  get typeGrid(): Record<string, unknown>[] { return (this.data?.['grid_new_vs_backfill'] as Record<string, unknown>[]) ?? []; }
  get buGrid(): Record<string, unknown>[] { return (this.data?.['grid_billability'] as Record<string, unknown>[]) ?? []; }
}
