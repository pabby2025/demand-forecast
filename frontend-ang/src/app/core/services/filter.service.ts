import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { FilterState } from '../models';

export const defaultFilters: FilterState = {
  practice_area: '',
  bu: '',
  location: '',
  grade: '',
  skill_cluster: '',
  forecast_horizon: '',
};

@Injectable({ providedIn: 'root' })
export class FilterService {
  private filtersSubject = new BehaviorSubject<FilterState>({ ...defaultFilters });
  filters$ = this.filtersSubject.asObservable();

  get filters(): FilterState {
    return this.filtersSubject.value;
  }

  setFilters(filters: FilterState): void {
    this.filtersSubject.next(filters);
  }

  updateFilter(key: keyof FilterState, value: string): void {
    this.filtersSubject.next({ ...this.filtersSubject.value, [key]: value });
  }

  clearFilters(): void {
    this.filtersSubject.next({ ...defaultFilters });
  }

  hasActiveFilters(): boolean {
    return Object.values(this.filtersSubject.value).some((v) => v !== '');
  }

  toParams(): Record<string, string> {
    const params: Record<string, string> = {};
    Object.entries(this.filtersSubject.value).forEach(([k, v]) => {
      if (v) params[k] = v;
    });
    return params;
  }
}
