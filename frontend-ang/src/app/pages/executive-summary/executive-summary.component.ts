import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router } from '@angular/router';
import { BaseChartDirective } from 'ng2-charts';
import { ChartConfiguration } from 'chart.js';
import { Subscription } from 'rxjs';
import { switchMap } from 'rxjs/operators';
import { ApiService } from '../../core/services/api.service';
import { AuthService } from '../../core/services/auth.service';
import { FilterService } from '../../core/services/filter.service';
import { FilterBarComponent } from '../../shared/components/filter-bar/filter-bar.component';
import { LoadingSkeletonComponent } from '../../shared/components/loading-skeleton/loading-skeleton.component';
import { ErrorCardComponent } from '../../shared/components/error-card/error-card.component';

type D = Record<string, unknown>;

@Component({
  selector: 'app-executive-summary',
  standalone: true,
  imports: [CommonModule, BaseChartDirective, FilterBarComponent, LoadingSkeletonComponent, ErrorCardComponent],
  templateUrl: './executive-summary.component.html',
})
export class ExecutiveSummaryComponent implements OnInit, OnDestroy {
  loading = true;
  error = false;
  data: D | null = null;
  private sub?: Subscription;

  // ── Charts ──────────────────────────────────────────────────────────────────
  donutOpts: ChartConfiguration<'doughnut'>['options'] = {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { position: 'bottom', labels: { font: { size: 9 }, boxWidth: 10, padding: 8 } } },
    cutout: '68%',
  };
  donutOptsMini: ChartConfiguration<'doughnut'>['options'] = {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { display: false } },
    cutout: '70%',
  };

  newBackfillChart: ChartConfiguration<'doughnut'>['data'] = { labels: [], datasets: [] };
  contractChart: ChartConfiguration<'doughnut'>['data'] = { labels: [], datasets: [] };
  onsiteChart: ChartConfiguration<'doughnut'>['data'] = { labels: [], datasets: [] };
  gradeChart: ChartConfiguration<'doughnut'>['data'] = { labels: [], datasets: [] };
  stableChart: ChartConfiguration<'doughnut'>['data'] = { labels: [], datasets: [] };

  constructor(
    private apiService: ApiService,
    private filterService: FilterService,
    public authService: AuthService,
    public router: Router,
  ) {}

  ngOnInit(): void {
    this.sub = this.filterService.filters$.pipe(
      switchMap((f) => { this.loading = true; this.error = false; return this.apiService.getExecutiveSummary(f); })
    ).subscribe({
      next: (d) => { this.data = d as unknown as D; this.buildCharts(); this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  ngOnDestroy(): void { this.sub?.unsubscribe(); }

  load(): void {
    this.error = false; this.loading = true;
    this.apiService.getExecutiveSummary(this.filterService.filters).subscribe({
      next: (d) => { this.data = d as unknown as D; this.buildCharts(); this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  private buildCharts(): void {
    const dt = this.demandType;
    this.newBackfillChart = {
      labels: ['New Demand', 'Backfill'],
      datasets: [{ data: [dt.new_demand_pct, dt.backfill_pct], backgroundColor: ['#1B2559', '#00BCD4'], borderWidth: 0 }],
    };
    this.contractChart = {
      labels: dt.contract_type_mix.map((c: D) => String(c['name'])),
      datasets: [{ data: dt.contract_type_mix.map((c: D) => Number(c['pct'])), backgroundColor: ['#1B2559', '#00BCD4', '#7C3AED'], borderWidth: 0 }],
    };
    const geo = this.geographic;
    this.onsiteChart = {
      labels: ['Onsite', 'Offshore'],
      datasets: [{ data: [geo.onsite_pct, geo.offshore_pct], backgroundColor: ['#1B2559', '#00BCD4'], borderWidth: 0 }],
    };
    const gd = this.gradeDistribution;
    this.gradeChart = {
      labels: gd.grade_donut.map((g: D) => String(g['grade'])),
      datasets: [{ data: gd.grade_donut.map((g: D) => Number(g['count'])), backgroundColor: ['#1B2559', '#00BCD4', '#7C3AED', '#F59E0B', '#EF4444', '#10B981'], borderWidth: 0 }],
    };
    const sd = this.skillDistribution;
    this.stableChart = {
      labels: ['Stable', 'Volatile'],
      datasets: [{ data: [sd.stable_pct, sd.volatile_pct], backgroundColor: ['#10B981', '#F59E0B'], borderWidth: 0 }],
    };
  }

  // ── Data accessors ──────────────────────────────────────────────────────────
  get kpis(): D { return {}; }
  get totalDemand(): number { return (this.data?.['total_fte_demand'] as number) ?? 0; }
  get forecastAccuracy(): number { return (this.data?.['forecast_accuracy'] as number) ?? 0; }
  get avgCancellation(): number { return (this.data?.['avg_cancellation_pct'] as number) ?? 40; }
  get momChange(): number { return (this.data?.['mom_change'] as number) ?? 0; }
  get topPracticeAreas(): D[] { return (this.data?.['top_practice_areas'] as D[]) ?? []; }
  get buPerformance(): { top_by_demand: D[]; top_by_growth: D[] } {
    return (this.data?.['bu_performance'] as { top_by_demand: D[]; top_by_growth: D[] }) ?? { top_by_demand: [], top_by_growth: [] };
  }
  get geographic(): { onsite_pct: number; offshore_pct: number; top_countries: D[] } {
    return (this.data?.['geographic'] as { onsite_pct: number; offshore_pct: number; top_countries: D[] }) ?? { onsite_pct: 38, offshore_pct: 62, top_countries: [] };
  }
  get skillDistribution(): { top_clusters: D[]; top_clusters_in_sl: D[]; stable_pct: number; volatile_pct: number } {
    return (this.data?.['skill_distribution'] as { top_clusters: D[]; top_clusters_in_sl: D[]; stable_pct: number; volatile_pct: number }) ?? { top_clusters: [], top_clusters_in_sl: [], stable_pct: 70, volatile_pct: 30 };
  }
  get gradeDistribution(): { grade_donut: D[]; shortfuse_by_grade: D[]; shortfuse_total: number } {
    return (this.data?.['grade_distribution'] as { grade_donut: D[]; shortfuse_by_grade: D[]; shortfuse_total: number }) ?? { grade_donut: [], shortfuse_by_grade: [], shortfuse_total: 0 };
  }
  get demandType(): { new_demand_pct: number; backfill_pct: number; contract_type_mix: D[] } {
    return (this.data?.['demand_type'] as { new_demand_pct: number; backfill_pct: number; contract_type_mix: D[] }) ?? { new_demand_pct: 65, backfill_pct: 35, contract_type_mix: [] };
  }
  get tasksSummary(): D {
    const tasks = (this.data?.['pending_tasks'] as Array<{ status: string }>) ?? [];
    return { pending: tasks.filter((t) => t.status === 'New').length, in_progress: tasks.filter((t) => t.status === 'In Review').length, completed: tasks.filter((t) => t.status === 'Completed').length };
  }
  get alertsSummary(): D {
    const alerts = (this.data?.['recent_alerts'] as Array<{ status: string }>) ?? [];
    return { new: alerts.filter((a) => a.status === 'Action Required').length, acknowledged: alerts.filter((a) => a.status === 'Pending Review').length, total: alerts.length };
  }
  get today(): string {
    return new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
  }
  growthClass(pct: number): string { return pct >= 0 ? 'text-green-600' : 'text-red-500'; }
  growthSign(pct: number): string { return pct >= 0 ? '+' : ''; }
  asNum(v: unknown): number { return Number(v) || 0; }
}
