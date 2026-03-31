import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-loading-skeleton',
  standalone: true,
  imports: [CommonModule],
  template: `
    <!-- Page skeleton -->
    <div *ngIf="type === 'page'" class="space-y-6">
      <div class="flex items-center justify-between">
        <div class="animate-pulse bg-gray-200 rounded h-8 w-48"></div>
        <div class="animate-pulse bg-gray-200 rounded h-8 w-32"></div>
      </div>
      <div class="grid grid-cols-4 gap-4">
        <div *ngFor="let i of [0,1,2,3]" class="bg-white rounded-xl border border-gray-100 shadow-sm p-5 flex flex-col gap-3">
          <div class="animate-pulse bg-gray-200 rounded h-3 w-24"></div>
          <div class="animate-pulse bg-gray-200 rounded h-8 w-32"></div>
          <div class="animate-pulse bg-gray-200 rounded h-3 w-20"></div>
        </div>
      </div>
      <div class="grid grid-cols-3 gap-4">
        <div *ngFor="let i of [0,1,2]" class="bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <div class="animate-pulse bg-gray-200 rounded h-4 w-32 mb-4"></div>
          <div class="animate-pulse bg-gray-100 rounded" style="height: 180px"></div>
        </div>
      </div>
      <div class="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden">
        <div class="px-4 py-3 border-b border-gray-100">
          <div class="animate-pulse bg-gray-200 rounded h-3 w-20"></div>
        </div>
        <div class="p-4 space-y-3">
          <div *ngFor="let r of [0,1,2,3,4]" class="flex gap-4">
            <div *ngFor="let c of [0,1,2,3,4,5]" class="animate-pulse bg-gray-200 rounded h-4 flex-1"></div>
          </div>
        </div>
      </div>
    </div>

    <!-- Table skeleton -->
    <div *ngIf="type === 'table'" class="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden">
      <div class="px-4 py-3 border-b border-gray-100">
        <div class="animate-pulse bg-gray-200 rounded h-3 w-20"></div>
      </div>
      <div class="p-4 space-y-3">
        <div *ngFor="let r of rowArray" class="flex gap-4">
          <div *ngFor="let c of colArray" class="animate-pulse bg-gray-200 rounded h-4 flex-1"></div>
        </div>
      </div>
    </div>
  `,
})
export class LoadingSkeletonComponent {
  @Input() type: 'page' | 'table' = 'page';
  @Input() rows = 5;
  @Input() cols = 6;

  get rowArray(): number[] {
    return Array.from({ length: this.rows }, (_, i) => i);
  }
  get colArray(): number[] {
    return Array.from({ length: this.cols }, (_, i) => i);
  }
}
