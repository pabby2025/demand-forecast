import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ApiService } from '../../core/services/api.service';
import { TaxonomyCluster, LeafSkill } from '../../core/models';
import { LoadingSkeletonComponent } from '../../shared/components/loading-skeleton/loading-skeleton.component';
import { ErrorCardComponent } from '../../shared/components/error-card/error-card.component';

@Component({
  selector: 'app-skill-taxonomy',
  standalone: true,
  imports: [CommonModule, LoadingSkeletonComponent, ErrorCardComponent],
  templateUrl: './skill-taxonomy.component.html',
})
export class SkillTaxonomyComponent implements OnInit {
  loading = true; error = false;
  clusters: TaxonomyCluster[] = [];
  expandedIds = new Set<string>();

  constructor(private apiService: ApiService) {}
  ngOnInit(): void { this.load(); }

  load(): void {
    this.loading = true; this.error = false;
    this.apiService.getTaxonomy().subscribe({
      next: (d: unknown) => { this.clusters = (d as { clusters: TaxonomyCluster[] }).clusters ?? []; this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  toggleExpand(id: string): void {
    if (this.expandedIds.has(id)) this.expandedIds.delete(id);
    else this.expandedIds.add(id);
  }

  isExpanded(id: string): boolean { return this.expandedIds.has(id); }

  get totalDemands(): number { return this.clusters.reduce((s, c) => s + c.total_demands, 0); }
  get avgStability(): string {
    if (!this.clusters.length) return '0';
    return (this.clusters.reduce((s, c) => s + c.stability, 0) / this.clusters.length).toFixed(2);
  }
  get totalLeafSkills(): number { return this.clusters.reduce((s, c) => s + c.leaf_skills.length, 0); }

  stabilityBarWidth(stability: number): string { return `${stability * 100}%`; }
  stabilityClass(stability: number): string { return stability >= 0.85 ? 'text-green-600' : 'text-yellow-600'; }
  leafBarWidth(weight: number): string { return `${weight * 100}%`; }
}
