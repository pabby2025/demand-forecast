import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { ApiService } from '../../core/services/api.service';
import { Task } from '../../core/models';
import { StatusBadgeComponent } from '../../shared/components/status-badge/status-badge.component';
import { LoadingSkeletonComponent } from '../../shared/components/loading-skeleton/loading-skeleton.component';
import { ErrorCardComponent } from '../../shared/components/error-card/error-card.component';

const FILTER_TABS = ['All', 'New', 'In Review', 'Completed'];

@Component({
  selector: 'app-my-tasks',
  standalone: true,
  imports: [CommonModule, RouterModule, StatusBadgeComponent, LoadingSkeletonComponent, ErrorCardComponent],
  templateUrl: './my-tasks.component.html',
})
export class MyTasksComponent implements OnInit {
  loading = true; error = false;
  tasks: Task[] = [];
  activeTab = 'All';
  filterTabs = FILTER_TABS;
  selected: Task | null = null;
  Math = Math;

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

  countFor(tab: string): number {
    if (tab === 'All') return this.tasks.length;
    return this.tasks.filter((t) => t.status === tab).length;
  }

  markComplete(task: Task): void {
    this.apiService.updateTask(task.task_id, { status: 'Completed' }).subscribe({
      next: () => this.load(),
    });
  }

  statusClass(status: string): string {
    const base = 'inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ';
    switch (status) {
      case 'New': return base + 'bg-red-100 text-red-700';
      case 'In Review': return base + 'bg-blue-100 text-blue-700';
      case 'Completed': return base + 'bg-green-100 text-green-700';
      default: return base + 'bg-gray-100 text-gray-600';
    }
  }
}
