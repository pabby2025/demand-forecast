import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { BaseChartDirective } from 'ng2-charts';
import { ChartConfiguration } from 'chart.js';

interface Driver {
  key: string;
  label: string;
  value: number;
  min: number;
  max: number;
  step: number;
}

@Component({
  selector: 'app-scenario-planning',
  standalone: true,
  imports: [CommonModule, FormsModule, BaseChartDirective],
  templateUrl: './scenario-planning.component.html',
})
export class ScenarioPlanningComponent implements OnInit {

  drivers: Driver[] = [
    { key: 'bu_growth', label: 'BU Level Growth', value: 65, min: 0, max: 100, step: 1 },
    { key: 'market_spend', label: 'Industry Level Market Spend', value: 65, min: 0, max: 100, step: 1 },
    { key: 'win_rate', label: 'Win Rate % on Strategic Account', value: 6, min: 0, max: 20, step: 0.5 },
    { key: 'growth_strategic', label: 'Growth % on Strategic Account', value: 65, min: 0, max: 100, step: 1 },
  ];

  readonly baseTotal = 380;
  Math = Math;

  get scenarioTotal(): number {
    const pctImpact =
      (this.drivers[0].value * 0.4 +
       this.drivers[1].value * 0.3 +
       this.drivers[2].value * 5 +
       this.drivers[3].value * 0.1) / 100;
    return Math.round(this.baseTotal * (1 + pctImpact * 0.2));
  }
  get netChange(): number { return this.scenarioTotal - this.baseTotal; }

  comparisonChart: ChartConfiguration<'line'>['data'] = { labels: [], datasets: [] };
  chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'bottom' as const, labels: { font: { size: 9 }, boxWidth: 14 } } },
    scales: {
      x: { ticks: { font: { size: 9 } } },
      y: { min: 2000, ticks: { font: { size: 9 }, callback: (v: number | string) => Number(v).toLocaleString() } },
    },
  };

  readonly months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'];
  readonly baselines = [7800, 8100, 7950, 8300, 7700, 7570];

  get comparisonRows(): Array<{ metric: string; values: string[] }> {
    const pct = this.netChange / this.baseTotal;
    const scenario = this.baselines.map((b) => Math.round(b * (1 + pct * 0.05)));
    const adj = scenario.map((s, i) => s - this.baselines[i]);
    return [
      { metric: 'Scenario Forecast', values: scenario.map((v) => v.toLocaleString()) },
      { metric: 'Baseline Forecast', values: this.baselines.map((v) => v.toLocaleString()) },
      { metric: 'Adjustment', values: adj.map((v) => (v >= 0 ? '+' : '') + v.toLocaleString()) },
      { metric: 'Feedback', values: ['', '', '', '', '', ''] },
    ];
  }

  constructor() {}

  ngOnInit(): void { this.buildChart(); }

  buildChart(): void {
    const pct = this.netChange / this.baseTotal;
    const scenario = this.baselines.map((b) => Math.round(b * (1 + pct * 0.05)));
    this.comparisonChart = {
      labels: this.months,
      datasets: [
        { data: scenario, borderColor: '#00BCD4', borderWidth: 2, fill: false, label: 'Scenario', pointRadius: 3, tension: 0.3 } as never,
        { data: this.baselines, borderColor: '#1B2559', borderWidth: 2, fill: false, label: 'Baseline', pointRadius: 3, tension: 0.3 } as never,
      ],
    };
  }

  onDriverChange(): void { this.buildChart(); }

  applyScenario(): void {
    this.buildChart();
  }

  resetDrivers(): void {
    this.drivers[0].value = 65;
    this.drivers[1].value = 65;
    this.drivers[2].value = 6;
    this.drivers[3].value = 65;
    this.buildChart();
  }

  isAdjPos(v: string): boolean { return v.startsWith('+'); }
}
