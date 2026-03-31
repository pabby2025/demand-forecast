import { Component, Input, OnChanges } from '@angular/core';
import { CommonModule } from '@angular/common';
import { StatusBadgeComponent } from '../status-badge/status-badge.component';

export interface GridColumn {
  key: string;
  header: string;
  type?: 'text' | 'number' | 'badge' | 'accuracy';
  size?: number;
  isQuarterly?: boolean;
}

@Component({
  selector: 'app-data-grid',
  standalone: true,
  imports: [CommonModule, StatusBadgeComponent],
  template: `
    <div class="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden">
      <!-- Toolbar -->
      <div class="flex items-center justify-between px-4 py-3 border-b border-gray-100">
        <span class="text-xs text-gray-500">{{ pagedData.length }} of {{ filteredData.length }} records</span>
        <button (click)="downloadCSV()" class="flex items-center gap-1.5 text-xs text-gray-500 hover:text-gray-700 hover:bg-gray-50 px-2 py-1 rounded transition-colors">
          <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" stroke-width="2"/>
            <polyline points="7 10 12 15 17 10" stroke-width="2"/>
            <line x1="12" y1="15" x2="12" y2="3" stroke-width="2"/>
          </svg>
          Download CSV
        </button>
      </div>

      <!-- Table -->
      <div class="overflow-x-auto">
        <table class="min-w-full text-sm">
          <thead>
            <tr class="bg-gray-50 border-b border-gray-100">
              <th *ngFor="let col of columns"
                  (click)="sort(col.key)"
                  [class]="'px-4 py-2.5 text-left text-xs font-semibold whitespace-nowrap cursor-pointer select-none ' + (col.isQuarterly ? 'text-blue-700' : 'text-gray-600')">
                <div class="flex items-center gap-1">
                  {{ col.header }}
                  <svg *ngIf="sortKey === col.key && sortDir === 'asc'" class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><polyline points="18 15 12 9 6 15" stroke-width="2"/></svg>
                  <svg *ngIf="sortKey === col.key && sortDir === 'desc'" class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><polyline points="6 9 12 15 18 9" stroke-width="2"/></svg>
                </div>
              </th>
            </tr>
          </thead>
          <tbody>
            <tr *ngIf="pagedData.length === 0">
              <td [attr.colspan]="columns.length" class="px-4 py-12 text-center text-gray-400 text-sm">
                No data available
              </td>
            </tr>
            <tr *ngFor="let row of pagedData; let i = index"
                [class]="'border-b border-gray-50 transition-colors hover:bg-blue-50/30 ' + (i % 2 === 0 ? 'bg-white' : 'bg-gray-50/30')">
              <td *ngFor="let col of columns" class="px-4 py-2.5 text-gray-700 whitespace-nowrap text-xs">
                <ng-container [ngSwitch]="col.type">
                  <app-status-badge *ngSwitchCase="'badge'" [status]="getCellValue(row, col.key)"></app-status-badge>
                  <span *ngSwitchCase="'accuracy'"
                        [class]="'font-medium ' + getAccuracyClass(+getCellValue(row, col.key))">
                    {{ getCellValue(row, col.key) }}%
                  </span>
                  <span *ngSwitchCase="'number'">{{ formatNumber(getCellValue(row, col.key)) }}</span>
                  <span *ngSwitchDefault>{{ getCellValue(row, col.key) }}</span>
                </ng-container>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Pagination -->
      <div class="flex items-center justify-between px-4 py-3 border-t border-gray-100">
        <span class="text-xs text-gray-500">Page {{ currentPage + 1 }} of {{ pageCount }}</span>
        <div class="flex items-center gap-1">
          <button (click)="prevPage()" [disabled]="currentPage === 0"
                  class="p-1 rounded text-gray-400 hover:text-gray-600 disabled:opacity-30 transition-colors">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><polyline points="15 18 9 12 15 6" stroke-width="2"/></svg>
          </button>
          <button (click)="nextPage()" [disabled]="currentPage >= pageCount - 1"
                  class="p-1 rounded text-gray-400 hover:text-gray-600 disabled:opacity-30 transition-colors">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><polyline points="9 18 15 12 9 6" stroke-width="2"/></svg>
          </button>
        </div>
      </div>
    </div>
  `,
})
export class DataGridComponent implements OnChanges {
  @Input() data: Record<string, unknown>[] = [];
  @Input() columns: GridColumn[] = [];
  @Input() pageSize = 10;
  @Input() downloadFilename = 'export.csv';

  sortKey = '';
  sortDir: 'asc' | 'desc' = 'asc';
  currentPage = 0;

  filteredData: Record<string, unknown>[] = [];
  pagedData: Record<string, unknown>[] = [];

  get pageCount(): number {
    return Math.max(1, Math.ceil(this.filteredData.length / this.pageSize));
  }

  ngOnChanges(): void {
    this.filteredData = [...this.data];
    this.applySortAndPage();
  }

  sort(key: string): void {
    if (this.sortKey === key) {
      this.sortDir = this.sortDir === 'asc' ? 'desc' : 'asc';
    } else {
      this.sortKey = key;
      this.sortDir = 'asc';
    }
    this.applySortAndPage();
  }

  prevPage(): void {
    if (this.currentPage > 0) {
      this.currentPage--;
      this.updatePage();
    }
  }

  nextPage(): void {
    if (this.currentPage < this.pageCount - 1) {
      this.currentPage++;
      this.updatePage();
    }
  }

  private applySortAndPage(): void {
    let sorted = [...this.filteredData];
    if (this.sortKey) {
      sorted.sort((a, b) => {
        const av = a[this.sortKey];
        const bv = b[this.sortKey];
        const cmp = av! < bv! ? -1 : av! > bv! ? 1 : 0;
        return this.sortDir === 'asc' ? cmp : -cmp;
      });
    }
    this.filteredData = sorted;
    this.currentPage = 0;
    this.updatePage();
  }

  private updatePage(): void {
    const start = this.currentPage * this.pageSize;
    this.pagedData = this.filteredData.slice(start, start + this.pageSize);
  }

  getCellValue(row: Record<string, unknown>, key: string): string {
    const v = row[key];
    return v === null || v === undefined ? '' : String(v);
  }

  formatNumber(val: string): string {
    const n = parseFloat(val);
    return isNaN(n) ? val : n.toLocaleString();
  }

  getAccuracyClass(val: number): string {
    if (val >= 90) return 'text-green-600';
    if (val >= 85) return 'text-yellow-600';
    return 'text-red-500';
  }

  downloadCSV(): void {
    if (!this.data.length) return;
    const keys = Object.keys(this.data[0]);
    const header = keys.join(',');
    const rows = this.data.map((row) =>
      keys.map((k) => {
        const v = String(row[k] ?? '');
        return v.includes(',') ? `"${v}"` : v;
      }).join(',')
    );
    const csv = [header, ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = this.downloadFilename;
    a.click();
    URL.revokeObjectURL(url);
  }
}
