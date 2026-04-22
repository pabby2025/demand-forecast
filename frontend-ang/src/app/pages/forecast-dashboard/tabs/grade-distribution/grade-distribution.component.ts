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
  selector: 'app-grade-distribution',
  standalone: true,
  imports: [CommonModule, BaseChartDirective, DataGridComponent, HeatmapComponent, LoadingSkeletonComponent, ErrorCardComponent],
  templateUrl: './grade-distribution.component.html',
})
export class GradeDistributionComponent implements OnInit, OnDestroy {
  loading = true; error = false; data: D | null = null;
  private sub?: Subscription;

  donutChart: ChartConfiguration<'doughnut'>['data'] = { labels: [], datasets: [] };
  trendChart: ChartConfiguration<'bar'>['data'] = { labels: [], datasets: [] };

  donutOptions: ChartConfiguration<'doughnut'>['options'] = {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { position: 'bottom', labels: { font: { size: 10 }, boxWidth: 10, padding: 6 } } },
    cutout: '65%',
  };
  barOptions = {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { labels: { font: { size: 9 }, boxWidth: 10 } } },
    scales: { x: { stacked: true, ticks: { font: { size: 9 } } }, y: { stacked: true, ticks: { font: { size: 9 } } } },
  };

  gradeHeatmapRows: HeatmapRow[] = [];
  gradeHeatmapMonths: string[] = [];

  monthlyColumns: GridColumn[] = [
    { key: 'practice_area', header: 'Practice Area' }, { key: 'location', header: 'Location' },
    { key: 'cluster', header: 'Cluster' }, { key: 'so_grade', header: 'SO Grade' },
    { key: 'jan', header: 'Jan', type: 'number' }, { key: 'feb', header: 'Feb', type: 'number' },
    { key: 'mar', header: 'Mar', type: 'number' },
    { key: 'q01', header: 'Q 01', type: 'number', isQuarterly: true },
    { key: 'apr', header: 'Apr', type: 'number' }, { key: 'may', header: 'May', type: 'number' },
    { key: 'jun', header: 'Jun', type: 'number' },
    { key: 'q02', header: 'Q 02', type: 'number', isQuarterly: true },
  ];

  sfColumns: GridColumn[] = [
    { key: 'practice_area', header: 'Practice Area' }, { key: 'location', header: 'Location' },
    { key: 'cluster', header: 'Cluster' }, { key: 'so_grade', header: 'SO Grade' },
    { key: 'feb', header: 'Feb', type: 'number' }, { key: 'mar', header: 'Mar', type: 'number' },
    { key: 'apr', header: 'Apr', type: 'number' }, { key: 'may', header: 'May', type: 'number' },
    { key: 'jun', header: 'Jun', type: 'number' }, { key: 'jul', header: 'Jul', type: 'number' },
    { key: 'total', header: 'Total', type: 'number' },
  ];

  private readonly GRADE_LABELS: Record<string, string> = {
    SA: 'Senior Analyst', A: 'Analyst', M: 'Manager', SM: 'Senior Manager', AD: 'Associate Director', GenC: 'Graduate',
  };
  private readonly COLORS = ['#1B2559', '#00BCD4', '#7C3AED', '#F59E0B', '#EF4444', '#10B981'];

  constructor(private apiService: ApiService, private filterService: FilterService) {}

  ngOnInit(): void {
    this.sub = this.filterService.filters$.pipe(
      switchMap((f) => { this.loading = true; this.error = false; return this.apiService.getGradeDistribution(f); })
    ).subscribe({
      next: (d) => { this.data = d as unknown as D; this.buildCharts(); this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  ngOnDestroy(): void { this.sub?.unsubscribe(); }

  load(): void {
    this.error = false; this.loading = true;
    this.apiService.getGradeDistribution(this.filterService.filters).subscribe({
      next: (d) => { this.data = d as unknown as D; this.buildCharts(); this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  private buildCharts(): void {
    const donut = (this.data?.['donut'] as Array<{ name: string; value: number }>) ?? [];
    this.donutChart = {
      labels: donut.map((d) => d.name),
      datasets: [{ data: donut.map((d) => d.value), backgroundColor: this.COLORS, borderWidth: 0 }],
    };

    const trend = (this.data?.['grade_trend'] as Array<D>) ?? [];
    const keys = trend.length ? Object.keys(trend[0]).filter((k) => k !== 'month') : [];
    this.trendChart = {
      labels: trend.map((t) => String(t['month'])),
      datasets: keys.map((k, i) => ({
        data: trend.map((t) => Number(t[k])),
        backgroundColor: this.COLORS[i % this.COLORS.length],
        label: k,
        borderWidth: 0,
      })),
    };

    // Grade heatmap: rows = grades, columns = months from grade_trend
    this.gradeHeatmapMonths = trend.map((t) => String(t['month'])).slice(0, 9);
    this.gradeHeatmapRows = keys.map((grade) => ({
      name: grade,
      monthly: trend.slice(0, 9).map((row) => Number(row[grade] ?? 0)),
    }));
  }

  get donutList(): Array<{ name: string; value: number; pct: number }> {
    const donut = (this.data?.['donut'] as Array<{ name: string; value: number }>) ?? [];
    const total = donut.reduce((s, d) => s + d.value, 0) || 1;
    return donut.map((d) => ({ ...d, pct: Math.round((d.value / total) * 100) }));
  }

  get shortfuseReqList(): D[] {
    const kpis = (this.data?.['kpis'] as D) ?? {};
    return (kpis['shortfuse_6months'] as D[]) ?? [];
  }

  get shortfuseTotal(): number { return (this.data?.['shortfuse_total'] as number) ?? 0; }
  get shortfuseChange(): string { return (this.data?.['shortfuse_change'] as string) ?? '+2.3%'; }
  get monthlyGrid(): D[] { return (this.data?.['grid_monthly'] as D[]) ?? []; }
  get shortFuseGrid(): D[] { return (this.data?.['grid_short_fuse'] as D[]) ?? []; }

  gradeLabel(grade: unknown): string { return this.GRADE_LABELS[String(grade)] ?? ''; }
  colorFor(i: number): string { return this.COLORS[i % this.COLORS.length]; }
  asNum(v: unknown): number { return Number(v) || 0; }

  shortfuseBarPct(count: unknown): number {
    const max = Math.max(...this.shortfuseReqList.map((g) => Number(g['count'])), 1);
    return (Number(count) / max) * 100;
  }
}
