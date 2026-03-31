import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';

const STATUS_CONFIG: Record<string, { label: string; className: string }> = {
  action_required: { label: 'Action Required', className: 'bg-red-100 text-red-700 border-red-200' },
  new: { label: 'New', className: 'bg-red-100 text-red-700 border-red-200' },
  pending: { label: 'Pending', className: 'bg-yellow-100 text-yellow-700 border-yellow-200' },
  in_review: { label: 'In Review', className: 'bg-yellow-100 text-yellow-700 border-yellow-200' },
  in_progress: { label: 'In Progress', className: 'bg-blue-100 text-blue-700 border-blue-200' },
  finalized: { label: 'Finalized', className: 'bg-green-100 text-green-700 border-green-200' },
  completed: { label: 'Completed', className: 'bg-green-100 text-green-700 border-green-200' },
  approved: { label: 'Approved', className: 'bg-green-100 text-green-700 border-green-200' },
  dismissed: { label: 'Dismissed', className: 'bg-gray-100 text-gray-600 border-gray-200' },
  rejected: { label: 'Rejected', className: 'bg-red-100 text-red-700 border-red-200' },
  draft: { label: 'Draft', className: 'bg-gray-100 text-gray-600 border-gray-200' },
  high: { label: 'High', className: 'bg-red-100 text-red-700 border-red-200' },
  medium: { label: 'Medium', className: 'bg-yellow-100 text-yellow-700 border-yellow-200' },
  low: { label: 'Low', className: 'bg-green-100 text-green-700 border-green-200' },
  critical: { label: 'Critical', className: 'bg-red-100 text-red-700 border-red-200' },
  warning: { label: 'Warning', className: 'bg-yellow-100 text-yellow-700 border-yellow-200' },
  healthy: { label: 'Healthy', className: 'bg-green-100 text-green-700 border-green-200' },
};

@Component({
  selector: 'app-status-badge',
  standalone: true,
  imports: [CommonModule],
  template: `
    <span [class]="'inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ' + config.className">
      {{ config.label }}
    </span>
  `,
})
export class StatusBadgeComponent {
  @Input() set status(val: string) {
    const key = val.toLowerCase().replace(/ /g, '_');
    this.config = STATUS_CONFIG[key] || STATUS_CONFIG[val.toLowerCase()] || {
      label: val,
      className: 'bg-gray-100 text-gray-600 border-gray-200',
    };
  }

  config: { label: string; className: string } = { label: '', className: 'bg-gray-100 text-gray-600 border-gray-200' };
}
