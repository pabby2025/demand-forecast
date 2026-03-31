import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ApiService } from '../../core/services/api.service';
import { Alert } from '../../core/models';
import { DataGridComponent, GridColumn } from '../../shared/components/data-grid/data-grid.component';
import { StatusBadgeComponent } from '../../shared/components/status-badge/status-badge.component';
import { LoadingSkeletonComponent } from '../../shared/components/loading-skeleton/loading-skeleton.component';
import { ErrorCardComponent } from '../../shared/components/error-card/error-card.component';

const FILTER_TABS = ['All', 'New', 'Acknowledged', 'Dismissed'];

@Component({
  selector: 'app-my-alerts',
  standalone: true,
  imports: [CommonModule, DataGridComponent, StatusBadgeComponent, LoadingSkeletonComponent, ErrorCardComponent],
  templateUrl: './my-alerts.component.html',
})
export class MyAlertsComponent implements OnInit {
  loading = true; error = false;
  alerts: Alert[] = []; unread = 0;
  activeTab = 'All';
  filterTabs = FILTER_TABS;

  columns: GridColumn[] = [
    { key: 'alert_id', header: 'ID' }, { key: 'alert_type', header: 'Alert' },
    { key: 'category', header: 'Category' }, { key: 'severity', header: 'Severity', type: 'badge' },
    { key: 'due_date', header: 'Date' }, { key: 'status', header: 'Status', type: 'badge' },
  ];

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

  get filteredAsRecords(): Record<string, unknown>[] {
    return this.filtered as unknown as Record<string, unknown>[];
  }

  get newAlerts(): Alert[] { return this.alerts.filter((a) => a.status === 'New').slice(0, 2); }

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
}
