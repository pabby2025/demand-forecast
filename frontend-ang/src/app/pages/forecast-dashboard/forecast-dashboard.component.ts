import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, Router, NavigationEnd } from '@angular/router';
import { filter } from 'rxjs/operators';

const TABS = [
  { label: 'Forecast Overview', path: '/forecast' },
  { label: 'Demand Type', path: '/forecast/demand-type' },
  { label: 'BU Performance', path: '/forecast/bu-performance' },
  { label: 'Geographic', path: '/forecast/geographic' },
  { label: 'Skill Distribution', path: '/forecast/skills' },
  { label: 'Grade Distribution', path: '/forecast/grades' },
  { label: 'Demand-Supply Gap', path: '/forecast/gap' },
];

@Component({
  selector: 'app-forecast-dashboard',
  standalone: true,
  imports: [CommonModule, RouterModule],
  template: `
    <div class="space-y-4">
      <!-- Tab Bar -->
      <div class="bg-white rounded-xl border border-gray-100 shadow-sm">
        <div class="flex overflow-x-auto">
          <button *ngFor="let tab of tabs; let i = index"
                  (click)="navigate(tab.path)"
                  [class]="'px-4 py-3 text-sm font-medium whitespace-nowrap border-b-2 transition-colors ' +
                    (isActive(tab.path) ? 'border-[#00BCD4] text-[#1B2559]' : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300')">
            {{ tab.label }}
          </button>
        </div>
      </div>
      <!-- Tab Content -->
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
}
