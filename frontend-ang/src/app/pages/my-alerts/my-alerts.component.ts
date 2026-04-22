import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ApiService } from '../../core/services/api.service';
import { Alert } from '../../core/models';
import { StatusBadgeComponent } from '../../shared/components/status-badge/status-badge.component';
import { LoadingSkeletonComponent } from '../../shared/components/loading-skeleton/loading-skeleton.component';
import { ErrorCardComponent } from '../../shared/components/error-card/error-card.component';

const FILTER_TABS = ['All', 'Action Required', 'Pending Review', 'Finalized'];

@Component({
  selector: 'app-my-alerts',
  standalone: true,
  imports: [CommonModule, StatusBadgeComponent, LoadingSkeletonComponent, ErrorCardComponent],
  templateUrl: './my-alerts.component.html',
})
export class MyAlertsComponent implements OnInit {
  loading = true; error = false;
  alerts: Alert[] = []; unread = 0;
  activeTab = 'All';
  filterTabs = FILTER_TABS;

  constructor(private apiService: ApiService) {}
  ngOnInit(): void { this.load(); }

  load(): void {
    this.loading = true; this.error = false;
    this.apiService.getAlerts().subscribe({
      next: (d: unknown) => {
        const resp = d as { alerts: Alert[]; unread: number };
        this.alerts = resp.alerts ?? [];
        this.unread = resp.unread ?? 0;
        this.loading = false;
      },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  get filtered(): Alert[] {
    if (this.activeTab === 'All') return this.alerts;
    return this.alerts.filter((a) => a.status === this.activeTab);
  }

  get newAlerts(): Alert[] { return this.alerts.filter((a) => a.status === 'Action Required').slice(0, 2); }

  countFor(tab: string): number {
    if (tab === 'All') return this.alerts.length;
    return this.alerts.filter((a) => a.status === tab).length;
  }

  acknowledge(alert: Alert): void {
    this.apiService.acknowledgeAlert(alert.alert_id).subscribe({ next: () => this.load() });
  }

  dismiss(alert: Alert): void {
    this.apiService.dismissAlert(alert.alert_id).subscribe({ next: () => this.load() });
  }

  statusClass(status: string): string {
    const base = 'inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ';
    switch (status) {
      case 'Action Required': return base + 'bg-red-100 text-red-700';
      case 'Pending Review': return base + 'bg-blue-100 text-blue-700';
      case 'Finalized': return base + 'bg-green-100 text-green-700';
      case 'High': return base + 'bg-red-100 text-red-700';
      case 'Medium': return base + 'bg-yellow-100 text-yellow-700';
      case 'Low': return base + 'bg-blue-100 text-blue-700';
      default: return base + 'bg-gray-100 text-gray-600';
    }
  }
}
