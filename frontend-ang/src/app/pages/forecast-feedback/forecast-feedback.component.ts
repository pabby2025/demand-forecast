import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { BaseChartDirective } from 'ng2-charts';
import { ChartConfiguration } from 'chart.js';
import { ApiService } from '../../core/services/api.service';
import { FeedbackItem, FeedbackSubmitRequest } from '../../core/models';
import { DataGridComponent, GridColumn } from '../../shared/components/data-grid/data-grid.component';
import { StatusBadgeComponent } from '../../shared/components/status-badge/status-badge.component';
import { LoadingSkeletonComponent } from '../../shared/components/loading-skeleton/loading-skeleton.component';
import { ErrorCardComponent } from '../../shared/components/error-card/error-card.component';

const SKILL_CLUSTERS = [
  'MSC-.NET-Angular-Azure-C#-Java', 'MSC-Agile-Microsoft_365-PPM-Project_Management',
  'MSC-Git-HTML/CSS-Node_JS-React-TypeScript', 'MSC-AWS-DevOps-Docker_Containers-Jenkins-Terraform',
  'MSC-Java-Kafka-Microservices-Python-Spring_Boot', 'MSC-AWS-Java-MySQL-SQL-Spring_Boot',
  'MSC-Android-React_Native-iOS', 'MSC-API_Development-Git-Java-Shell_Scripting-Software_Testing',
  'MSC-AWS-Java-JavaScript-MySQL-SQL',
];
const MONTH_LABELS = ['Jan 2026', 'Feb 2026', 'Mar 2026', 'Apr 2026', 'May 2026', 'Jun 2026'];

@Component({
  selector: 'app-forecast-feedback',
  standalone: true,
  imports: [CommonModule, FormsModule, BaseChartDirective, DataGridComponent, StatusBadgeComponent, LoadingSkeletonComponent, ErrorCardComponent],
  templateUrl: './forecast-feedback.component.html',
})
export class ForecastFeedbackComponent implements OnInit {
  loading = true; error = false;
  items: FeedbackItem[] = [];
  showModal = false;
  editItem: FeedbackItem | null = null;
  clusters = SKILL_CLUSTERS;
  monthLabels = MONTH_LABELS;

  formMonth = MONTH_LABELS[0];
  formCluster = SKILL_CLUSTERS[0];
  formSystemForecast = 5000;
  formMgmtAdjustment = 5000;
  formReason = '';

  lineChart: ChartConfiguration<'line'>['data'] = { labels: [], datasets: [] };
  chartOptions = { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { font: { size: 10 } } } }, scales: { x: { ticks: { font: { size: 10 }, maxRotation: 10 } }, y: { ticks: { font: { size: 10 }, callback: (v: number | string) => Number(v).toLocaleString() } } } };

  columns: GridColumn[] = [
    { key: 'month', header: 'Month' }, { key: 'cluster', header: 'Cluster' },
    { key: 'system_forecast', header: 'System Forecast', type: 'number' },
    { key: 'mgmt_adjustment', header: 'Mgmt Adjustment', type: 'number' },
    { key: 'reason', header: 'Reason' }, { key: 'status', header: 'Status', type: 'badge' },
  ];

  constructor(private apiService: ApiService) {}
  ngOnInit(): void { this.load(); }

  load(): void {
    this.loading = true; this.error = false;
    this.apiService.getFeedback().subscribe({
      next: (d: unknown) => {
        this.items = (d as { feedback: FeedbackItem[] }).feedback ?? [];
        this.buildChart();
        this.loading = false;
      },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  buildChart(): void {
    const slice = this.items.slice(0, 9);
    this.lineChart = {
      labels: slice.map((f) => f.month),
      datasets: [
        { data: slice.map((f) => f.system_forecast), borderColor: '#1B2559', borderWidth: 2, fill: false, label: 'System Forecast', pointRadius: 0 },
        { data: slice.map((f) => f.mgmt_adjustment), borderColor: '#00BCD4', borderWidth: 2, borderDash: [4, 2], fill: false, label: 'Mgmt Adjustment', pointRadius: 0 },
        { data: slice.map((f) => f.final_forecast), borderColor: '#7C3AED', borderWidth: 1.5, fill: false, label: 'Final Forecast', pointRadius: 0 },
      ],
    };
  }

  get approved(): number { return this.items.filter((i) => i.status === 'Approved').length; }
  get pending(): number { return this.items.filter((i) => i.status === 'Pending').length; }
  get rejected(): number { return this.items.filter((i) => i.status === 'Rejected').length; }
  get deviation(): string {
    if (this.formSystemForecast === 0) return '0';
    return ((this.formMgmtAdjustment - this.formSystemForecast) / this.formSystemForecast * 100).toFixed(1);
  }
  get itemsAsRecords(): Record<string, unknown>[] { return this.items as unknown as Record<string, unknown>[]; }

  openAdd(): void {
    this.editItem = null;
    this.formMonth = MONTH_LABELS[0]; this.formCluster = SKILL_CLUSTERS[0];
    this.formSystemForecast = 5000; this.formMgmtAdjustment = 5000; this.formReason = '';
    this.showModal = true;
  }

  save(): void {
    if (this.editItem) {
      this.apiService.updateFeedback(this.editItem.id, {
        month: this.formMonth, cluster: this.formCluster,
        system_forecast: this.formSystemForecast, mgmt_adjustment: this.formMgmtAdjustment,
        reason: this.formReason,
      }).subscribe({ next: () => { this.showModal = false; this.editItem = null; this.load(); } });
    } else {
      const req: FeedbackSubmitRequest = {
        scenario_inputs: [{ scenario_id: '', variable: this.formCluster, value: this.formMgmtAdjustment, impact_pct: 0 }],
        summary: { total_fte: this.formSystemForecast + this.formMgmtAdjustment, hc_target: 0, variance_from_target: 0, variance_last_cycle_pct: 0, onsite_pct: 0, grade_pct: '', stable_volatile: '', forecast_accuracy_pct: 0 },
        skill_updates: [],
        feedback_text: this.formReason,
        action: 'submit',
      };
      this.apiService.submitFeedback(req).subscribe({
        next: () => { this.showModal = false; this.load(); },
      });
    }
  }

  delete(item: FeedbackItem): void {
    if (confirm('Delete this feedback?')) {
      this.apiService.deleteFeedback(item.id).subscribe({ next: () => this.load() });
    }
  }
}
