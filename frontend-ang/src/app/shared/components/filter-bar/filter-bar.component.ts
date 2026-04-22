import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { FilterService } from '../../../core/services/filter.service';
import { FilterState } from '../../../core/models';

const PRACTICE_AREAS = ['All', 'Technology', 'Financial Services', 'Healthcare', 'Consulting', 'Energy', 'Retail'];
const BUS = ['All', 'Consulting', 'Financial Services', 'Healthcare', 'Technology', 'Retail', 'Manufacturing', 'Energy', 'Telecom', 'Government', 'Media', 'Education', 'Logistics', 'Real Estate', 'Automotive'];
const LOCATIONS = ['All', 'US', 'India', 'UK', 'Philippines', 'Poland', 'Canada', 'Australia', 'Germany', 'Singapore', 'UAE'];
const GRADES = ['All', 'SA', 'A', 'M', 'GenC', 'SM', 'AD'];
const CLUSTERS = [
  'All',
  'MSC-.NET-Angular-Azure-C#-Java',
  'MSC-Agile-Microsoft_365-PPM-Project_Management',
  'MSC-Git-HTML/CSS-Node_JS-React-TypeScript',
  'MSC-AWS-DevOps-Docker_Containers-Jenkins-Terraform',
  'MSC-Java-Kafka-Microservices-Python-Spring_Boot',
  'MSC-AWS-Java-MySQL-SQL-Spring_Boot',
  'MSC-Android-React_Native-iOS',
  'MSC-API_Development-Git-Java-Shell_Scripting-Software_Testing',
  'MSC-AWS-Java-JavaScript-MySQL-SQL',
];
const HORIZONS = ['All', 'M0-M5', 'M0-M2', 'M0-M3', 'Full Year'];

@Component({
  selector: 'app-filter-bar',
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: `
    <div class="bg-white border-b border-gray-200 px-6 py-3 flex items-end gap-5 flex-wrap">

      <div class="flex flex-col gap-1" *ngFor="let f of filterDefs">
        <label class="text-[10px] font-semibold text-gray-500 uppercase tracking-wider">{{ f.label }}</label>
        <div class="relative flex items-center">
          <select
            [ngModel]="getDisplayValue(f.key)"
            (ngModelChange)="onUpdate(f.key, $event)"
            class="appearance-none text-xs text-gray-700 bg-white border border-gray-200 rounded-lg pl-2.5 pr-7 py-1.5 focus:outline-none focus:border-teal-500 focus:ring-1 focus:ring-teal-500 cursor-pointer min-w-[130px]">
            <option *ngFor="let opt of f.options" [value]="opt">{{ opt }}</option>
          </select>
          <svg class="w-3 h-3 text-gray-400 absolute right-1.5 pointer-events-none" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <polyline points="6 9 12 15 18 9" stroke-width="2"/>
          </svg>
        </div>
      </div>

      <div *ngIf="hasActiveFilters" class="flex flex-col justify-end pb-0.5">
        <button (click)="filterService.clearFilters()"
                class="text-xs text-red-500 hover:text-red-700 underline">
          Clear all
        </button>
      </div>
    </div>
  `,
})
export class FilterBarComponent {
  filterDefs = [
    { key: 'practice_area' as keyof FilterState, label: 'Practice Area', options: PRACTICE_AREAS },
    { key: 'bu' as keyof FilterState, label: 'Business Unit', options: BUS },
    { key: 'location' as keyof FilterState, label: 'Location', options: LOCATIONS },
    { key: 'grade' as keyof FilterState, label: 'Grade', options: GRADES },
    { key: 'skill_cluster' as keyof FilterState, label: 'Skill Microcluster', options: CLUSTERS },
    { key: 'forecast_horizon' as keyof FilterState, label: 'Forecast Horizon', options: HORIZONS },
  ];

  constructor(public filterService: FilterService) {}

  get hasActiveFilters(): boolean {
    return this.filterService.hasActiveFilters();
  }

  getDisplayValue(key: keyof FilterState): string {
    return this.filterService.filters[key] || 'All';
  }

  onUpdate(key: keyof FilterState, value: string): void {
    this.filterService.updateFilter(key, value === 'All' ? '' : value);
  }
}
