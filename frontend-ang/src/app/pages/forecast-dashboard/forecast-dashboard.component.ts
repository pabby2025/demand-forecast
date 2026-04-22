import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, Router, NavigationEnd } from '@angular/router';
import { filter } from 'rxjs/operators';
import { FilterBarComponent } from '../../shared/components/filter-bar/filter-bar.component';

const TABS = [
  { label: 'Forecast Overview', path: '/forecast', subtitle: 'Track FTE demand patterns at multiple time scales to align staffing with forecasted needs.' },
  { label: 'Demand Type Breakdown', path: '/forecast/demand-type', subtitle: 'Analyze demand distribution by type and billability to guide workforce planning decisions.' },
  { label: 'Business Unit Performance', path: '/forecast/bu-performance', subtitle: 'Evaluate demand and growth trends across business units for strategic resource allocation.' },
  { label: 'Geographic Distribution', path: '/forecast/geographic', subtitle: 'Understand demand spread across regions and locations for geo-specific workforce planning.' },
  { label: 'Skill Distribution', path: '/forecast/skills', subtitle: 'Identify top skill clusters and volatility patterns to support targeted talent acquisition.' },
  { label: 'Grade Distribution', path: '/forecast/grades', subtitle: 'Analyze demand distribution across grades to balance workforce structure and short-term needs.' },
  { label: 'Demand-Supply Gap', path: '/forecast/gap', subtitle: 'Tracking demand-supply mismatches to optimize fulfilment and workforce allocation.' },
];

@Component({
  selector: 'app-forecast-dashboard',
  standalone: true,
  imports: [CommonModule, RouterModule, FilterBarComponent],
  template: `
    <!-- Tab Bar (top) -->
    <div class="bg-white border-b border-gray-100 shadow-sm">
      <div class="flex overflow-x-auto px-4">
        <button *ngFor="let tab of tabs"
                (click)="navigate(tab.path)"
                [class]="'px-4 py-3.5 text-xs font-medium whitespace-nowrap border-b-2 transition-colors ' +
                  (isActive(tab.path) ? 'border-[#00BCD4] text-[#1B2559]' : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300')">
          {{ tab.label }}
        </button>
      </div>
    </div>

    <!-- Active Tab Title + Subtitle -->
    <div class="px-6 pt-5 pb-2">
      <h1 class="text-2xl font-bold text-gray-900">{{ activeTab?.label }}</h1>
      <p class="text-sm text-gray-500 mt-0.5">{{ activeTab?.subtitle }}</p>
    </div>

    <!-- Filter bar -->
    <app-filter-bar></app-filter-bar>

    <!-- Tab Content -->
    <div class="px-6 py-4">
      <router-outlet></router-outlet>
    </div>
  `,
})
export class ForecastDashboardComponent {
  tabs = TABS;
  currentPath = '/forecast';

  constructor(private router: Router) {
    this.currentPath = router.url;
    router.events.pipe(filter((e) => e instanceof NavigationEnd)).subscribe((e) => {
      this.currentPath = (e as NavigationEnd).urlAfterRedirects;
    });
  }

  navigate(path: string): void {
    this.router.navigate([path]);
  }

  isActive(path: string): boolean {
    if (path === '/forecast') return this.currentPath === '/forecast' || this.currentPath === '/forecast/';
    return this.currentPath.startsWith(path);
  }

  get activeTab(): typeof TABS[0] | undefined {
    return TABS.find((t) => this.isActive(t.path));
  }
}
