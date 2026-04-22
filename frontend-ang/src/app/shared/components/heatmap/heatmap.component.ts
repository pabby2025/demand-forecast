import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';

export interface HeatmapRow { name: string; monthly: number[]; }

@Component({
  selector: 'app-heatmap',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div>
      <div class="overflow-x-auto">
        <table class="min-w-full text-xs border-collapse">
          <thead>
            <tr>
              <th class="sticky left-0 z-10 text-left px-3 py-2 text-gray-500 font-semibold bg-gray-50 border-b border-r border-gray-100 min-w-[130px]">{{ rowLabel }}</th>
              <th *ngFor="let m of months" class="text-center px-3 py-2 text-gray-500 font-semibold bg-gray-50 border-b border-gray-100 min-w-[60px]">{{ m }}</th>
              <th class="text-center px-3 py-2 text-gray-500 font-semibold bg-gray-50 border-b border-l border-gray-100">Total</th>
            </tr>
          </thead>
          <tbody>
            <tr *ngFor="let row of data; let even = even" [class]="even ? 'bg-white' : 'bg-gray-50/50'">
              <td class="sticky left-0 z-10 px-3 py-1.5 font-medium text-gray-700 truncate max-w-[130px] border-r border-gray-100" [class]="even ? 'bg-white' : 'bg-gray-50/50'" [title]="row.name">{{ row.name }}</td>
              <td *ngFor="let v of row.monthly"
                  class="px-1 py-1.5 text-center font-semibold border-b border-gray-50 transition-colors"
                  [style.background-color]="getColor(v)"
                  [style.color]="getTextColor(v)">
                {{ v | number:'1.0-0' }}
              </td>
              <td class="px-3 py-1.5 text-center font-bold text-gray-800 border-l border-gray-100">{{ rowTotal(row) | number:'1.0-0' }}</td>
            </tr>
            <tr *ngIf="data.length === 0">
              <td [attr.colspan]="months.length + 2" class="px-4 py-8 text-center text-gray-400 text-xs">No data available</td>
            </tr>
          </tbody>
        </table>
      </div>
      <!-- Legend -->
      <div class="flex items-center gap-2 mt-3 text-xs text-gray-400">
        <span>Low</span>
        <div class="flex h-2.5 rounded-full overflow-hidden w-32">
          <div *ngFor="let c of legendSteps" class="flex-1" [style.background-color]="c"></div>
        </div>
        <span>High</span>
      </div>
    </div>
  `,
})
export class HeatmapComponent {
  @Input() data: HeatmapRow[] = [];
  @Input() months: string[] = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'];
  @Input() rowLabel = 'Cluster';
  @Input() colorScheme: 'yellow' | 'blue' = 'yellow';

  get legendSteps(): string[] {
    if (this.colorScheme === 'blue') {
      return ['#EFF6FF', '#BFDBFE', '#93C5FD', '#60A5FA', '#3B82F6', '#2563EB', '#1D4ED8', '#1E40AF'];
    }
    return ['#FEF9C3', '#FDE68A', '#FCD34D', '#FBBF24', '#F59E0B', '#D97706', '#B45309', '#92400E'];
  }

  get maxVal(): number {
    const all = this.data.flatMap((r) => r.monthly);
    return Math.max(1, ...all);
  }

  getColor(v: number): string {
    if (!v || v === 0) return '#F9FAFB';
    const ratio = Math.min(v / this.maxVal, 1);
    if (this.colorScheme === 'blue') {
      // Light blue (#EFF6FF) → Deep blue (#1E40AF)
      const r = Math.round(239 - ratio * (239 - 30));
      const g = Math.round(246 - ratio * (246 - 64));
      const b = Math.round(255 - ratio * (255 - 175));
      return `rgb(${r},${g},${b})`;
    }
    // Yellow (#FEF3C7) → Orange (#F59E0B) → Deep orange (#B45309)
    const r = Math.round(254 - ratio * 88);
    const g = Math.round(243 - ratio * 145);
    const b = Math.round(199 - ratio * 185);
    return `rgb(${r},${g},${b})`;
  }

  getTextColor(v: number): string {
    const ratio = Math.min(v / this.maxVal, 1);
    return ratio > 0.55 ? '#fff' : '#374151';
  }

  rowTotal(row: HeatmapRow): number {
    return row.monthly.reduce((s, n) => s + (n || 0), 0);
  }
}
