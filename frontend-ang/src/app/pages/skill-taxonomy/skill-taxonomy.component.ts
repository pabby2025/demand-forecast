import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ApiService } from '../../core/services/api.service';
import { TaxonomyCluster } from '../../core/models';
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

  constructor(private apiService: ApiService) {}
  ngOnInit(): void { this.load(); }

  load(): void {
    this.loading = true; this.error = false;
    this.apiService.getTaxonomy().subscribe({
      next: (d: unknown) => { this.clusters = (d as { clusters: TaxonomyCluster[] }).clusters ?? []; this.loading = false; },
      error: () => { this.error = true; this.loading = false; },
    });
  }

  leafSkill(cluster: TaxonomyCluster, index: number): string {
    return (cluster.leaf_skills[index] as { skill?: string } | undefined)?.skill ?? '–';
  }

  Math = Math;
  pageSize = 10;
  currentPage = 1;
  get pagedClusters(): TaxonomyCluster[] {
    const start = (this.currentPage - 1) * this.pageSize;
    return this.clusters.slice(start, start + this.pageSize);
  }
  get totalPages(): number { return Math.ceil(this.clusters.length / this.pageSize) || 1; }
  prevPage(): void { if (this.currentPage > 1) this.currentPage--; }
  nextPage(): void { if (this.currentPage < this.totalPages) this.currentPage++; }
}
