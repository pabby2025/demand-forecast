import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { BaseChartDirective } from 'ng2-charts';
import { ChartConfiguration } from 'chart.js';
import { ApiService } from '../../core/services/api.service';
import { FeedbackItem, FeedbackSubmitRequest } from '../../core/models';
import { LoadingSkeletonComponent } from '../../shared/components/loading-skeleton/loading-skeleton.component';
import { ErrorCardComponent } from '../../shared/components/error-card/error-card.component';

type D = Record<string, unknown>;

@Component({
  selector: 'app-forecast-feedback',
  standalone: true,
  imports: [CommonModule, FormsModule, BaseChartDirective, LoadingSkeletonComponent, ErrorCardComponent],
  templateUrl: './forecast-feedback.component.html',
})
export class ForecastFeedbackComponent implements OnInit {
  loading = true; error = false; submitting = false; submitSuccess = false;
  items: FeedbackItem[] = [];
  feedbackText = '';

  // Charts
  barChart: ChartConfiguration<'bar'>['data'] = { labels: [], datasets: [] };
  barOptions = {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { labels: { font: { size: 9 }, boxWidth: 10 } } },
    scales: {
      x: { stacked: true, ticks: { font: { size: 9 } } },
      y: { stacked: true, ticks: { font: { size: 9 }, callback: (v: number | string) => Number(v).toLocaleString() } },
    },
  };

  // Skill updates table state
  skillUpdates: D[] = [
    { type: 'Updated', cluster: 'MSC-AWS-DevOps-Docker_Containers-Jenkins-Terraform', old_skills: 'AWS, Jenkins', new_skills: 'AWS, Jenkins, GitOps' },
    { type: 'Removed', cluster: 'MSC-Java-Kafka-Microservices-Python-Spring_Boot', old_skills: 'Java, Python, Spring Boot', new_skills: 'Java, Spring Boot' },
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
    // Stacked bar: System Generated Baseline + Management Overlay
    this.barChart = {
      labels: slice.map((f) => f.month),
      datasets: [
        {
          data: slice.map((f) => f.system_forecast),
          backgroundColor: '#1B2559',
          label: 'System Generated Baseline',
          borderWidth: 0,
        },
        {
          data: slice.map((f) => f.mgmt_adjustment),
          backgroundColor: '#00BCD4',
          label: 'Management Overlay',
          borderWidth: 0,
        },
      ],
    };
  }

  get scenarioInputs(): D[] {
    // Build from items: group by cluster, compute impact
    return this.items.slice(0, 5).map((f) => ({
      scenario_id: `SC-${f.month?.replace(' ', '')}`,
      variable: f.cluster ?? 'N/A',
      value: f.mgmt_adjustment ?? 0,
      impact_pct: f.system_forecast
        ? Number((((f.mgmt_adjustment - f.system_forecast) / f.system_forecast) * 100).toFixed(1))
        : 0,
    }));
  }

  get summary(): D {
    const totalFte = this.items.reduce((s, i) => s + (i.final_forecast ?? 0), 0);
    const hcTarget = Math.round(totalFte * 0.92);
    return {
      total_fte: totalFte,
      hc_target: hcTarget,
      variance_from_target: totalFte - hcTarget,
      variance_last_cycle_pct: 3.4,
      onsite_pct: 38,
      grade_pct: 'SA 25% / A 35%',
      stable_volatile: '70% / 30%',
      forecast_accuracy_pct: 87.4,
    };
  }

  addSkillUpdate(): void {
    this.skillUpdates = [
      ...this.skillUpdates,
      { type: 'Newly Added', cluster: '', old_skills: '', new_skills: '' },
    ];
  }

  removeSkillUpdate(i: number): void {
    this.skillUpdates = this.skillUpdates.filter((_, idx) => idx !== i);
  }

  submitFeedback(): void {
    if (this.submitting) return;
    this.submitting = true;
    const req: FeedbackSubmitRequest = {
      scenario_inputs: this.scenarioInputs.map((s) => ({
        scenario_id: String(s['scenario_id']),
        variable: String(s['variable']),
        value: Number(s['value']),
        impact_pct: Number(s['impact_pct']),
      })),
      summary: {
        total_fte: Number(this.summary['total_fte']),
        hc_target: Number(this.summary['hc_target']),
        variance_from_target: Number(this.summary['variance_from_target']),
        variance_last_cycle_pct: Number(this.summary['variance_last_cycle_pct']),
        onsite_pct: Number(this.summary['onsite_pct']),
        grade_pct: String(this.summary['grade_pct']),
        stable_volatile: String(this.summary['stable_volatile']),
        forecast_accuracy_pct: Number(this.summary['forecast_accuracy_pct']),
      },
      skill_updates: this.skillUpdates.map((u) => ({
        type: (String(u['type']) || 'Updated') as 'Newly Added' | 'Updated' | 'Removed',
        cluster: String(u['cluster']),
        old_skills: String(u['old_skills']),
        new_skills: String(u['new_skills']),
      })),
      feedback_text: this.feedbackText,
      action: 'submit',
    };
    this.apiService.submitFeedback(req).subscribe({
      next: () => { this.submitting = false; this.submitSuccess = true; setTimeout(() => (this.submitSuccess = false), 3000); this.load(); },
      error: () => { this.submitting = false; },
    });
  }

  changeTypeClass(type: unknown): string {
    const base = 'inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ';
    switch (String(type)) {
      case 'Newly Added': return base + 'bg-green-100 text-green-700';
      case 'Removed': return base + 'bg-red-100 text-red-700';
      case 'Updated': return base + 'bg-blue-100 text-blue-700';
      default: return base + 'bg-gray-100 text-gray-600';
    }
  }

  asNum(v: unknown): number { return Number(v) || 0; }
}
