import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ApiService } from '../../core/services/api.service';
import { Task } from '../../core/models';
import { DataGridComponent, GridColumn } from '../../shared/components/data-grid/data-grid.component';
import { StatusBadgeComponent } from '../../shared/components/status-badge/status-badge.component';
import { LoadingSkeletonComponent } from '../../shared/components/loading-skeleton/loading-skeleton.component';
import { ErrorCardComponent } from '../../shared/components/error-card/error-card.component';

const FILTER_TABS = ['All', 'Pending', 'In Progress', 'Completed', 'Action Required'];

@Component({
  selector: 'app-my-tasks',
  standalone: true,
  imports: [CommonModule, DataGridComponent, StatusBadgeComponent, LoadingSkeletonComponent, ErrorCardComponent],
  templateUrl: './my-tasks.component.html',
})
export class MyTasksComponent implements OnInit {
  loading = true; error = false;
  tasks: Task[] = [];
  activeTab = 'All';
  filterTabs = FILTER_TABS;
  selected: Task | null = null;

  columns: GridColumn[] = [
    { key: 'task_id', header: 'ID' }, { key: 'task_type', header: 'Task Type' },
    { key: 'priority', header: 'Priority', type: 'badge' },
    { key: 'due_date', header: 'Due Date' }, { key: 'assigned_by', header: 'Assigned By' },
    { key: 'status', header: 'Status', type: 'badge' },
  ];

  constructor(private apiService: ApiService) {}

  ngOnInit(): void { this.load(); }

  load(): void {
    this.loading = true; this.error = false;
    this.apiService.getTasks().subscribe({
      next: (d: unknown) => { this.tasks = (d as { tasks: Task[] }).tasks ?? []; this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  get filtered(): Task[] {
    if (this.activeTab === 'All') return this.tasks;
    return this.tasks.filter((t) => t.status === this.activeTab);
  }

  get filteredAsRecords(): Record<string, unknown>[] {
    return this.filtered as unknown as Record<string, unknown>[];
  }

  countFor(tab: string): number {
    if (tab === 'All') return this.tasks.length;
    return this.tasks.filter((t) => t.status === tab).length;
  }

  markComplete(task: Task): void {
    this.apiService.updateTask(task.task_id, { status: 'Completed' }).subscribe({
      next: () => this.load(),
    });
  }
}
