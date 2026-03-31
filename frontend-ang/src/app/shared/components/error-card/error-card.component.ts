import { Component, Input, Output, EventEmitter } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-error-card',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="bg-red-50 border border-red-200 rounded-xl p-6 flex flex-col items-center gap-3 text-center">
      <svg class="w-8 h-8 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <circle cx="12" cy="12" r="10" stroke-width="2"/>
        <line x1="12" y1="8" x2="12" y2="12" stroke-width="2"/>
        <line x1="12" y1="16" x2="12.01" y2="16" stroke-width="2"/>
      </svg>
      <div>
        <p class="font-medium text-red-700 text-sm">Something went wrong</p>
        <p class="text-red-600 text-xs mt-1">{{ message }}</p>
      </div>
      <button
        *ngIf="onRetry.observed"
        (click)="onRetry.emit()"
        class="flex items-center gap-2 px-4 py-2 bg-red-600 text-white text-xs rounded-lg hover:bg-red-700 transition-colors"
      >
        <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/>
          <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/>
        </svg>
        Retry
      </button>
    </div>
  `,
})
export class ErrorCardComponent {
  @Input() message = 'Failed to load data.';
  @Output() onRetry = new EventEmitter<void>();
}
