import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';

const COLOR_MAP: Record<string, string> = {
  teal: 'bg-teal-100 text-teal-600',
  purple: 'bg-purple-100 text-purple-600',
  navy: 'bg-blue-100 text-blue-700',
  orange: 'bg-orange-100 text-orange-600',
};

@Component({
  selector: 'app-kpi-card',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-5 flex flex-col gap-3">
      <div class="flex items-center justify-between">
        <span class="text-xs font-medium text-gray-500 uppercase tracking-wide">{{ title }}</span>
        <div class="flex items-center gap-1">
          <span [class]="'w-8 h-8 rounded-lg flex items-center justify-center ' + iconBg">
            <ng-content select="[slot=icon]"></ng-content>
          </span>
          <button class="text-gray-300 hover:text-gray-500 transition-colors" [title]="'About ' + title">
            <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <circle cx="12" cy="12" r="10" stroke-width="2"/>
              <line x1="12" y1="16" x2="12" y2="12" stroke-width="2"/>
              <line x1="12" y1="8" x2="12.01" y2="8" stroke-width="2"/>
            </svg>
          </button>
        </div>
      </div>
      <div>
        <div class="text-2xl font-bold text-gray-900">{{ displayValue }}</div>
        <div *ngIf="subtitle" class="text-xs text-gray-500 mt-1">{{ subtitle }}</div>
      </div>
      <div *ngIf="trend !== null && trend !== undefined"
           [class]="'flex items-center gap-1 text-xs font-medium ' + (trend >= 0 ? 'text-green-600' : 'text-red-500')">
        <svg *ngIf="trend >= 0" class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/>
        </svg>
        <svg *ngIf="trend < 0" class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <polyline points="23 18 13.5 8.5 8.5 13.5 1 6"/><polyline points="17 18 23 18 23 12"/>
        </svg>
        {{ trend >= 0 ? '+' : '' }}{{ trend.toFixed(1) }}% vs last period
      </div>
    </div>
  `,
})
export class KpiCardComponent {
  @Input() title = '';
  @Input() subtitle?: string;
  @Input() trend?: number | null;
  @Input() color: 'teal' | 'purple' | 'navy' | 'orange' = 'teal';

  @Input() set value(v: string | number) {
    this.displayValue = typeof v === 'number' ? v.toLocaleString() : v;
  }

  displayValue = '';

  get iconBg(): string {
    return COLOR_MAP[this.color] ?? COLOR_MAP['teal'];
  }
}
