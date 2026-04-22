import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, RouterLink, RouterLinkActive } from '@angular/router';
import { Router } from '@angular/router';
import { AuthService } from '../../core/services/auth.service';
const NAV_ITEMS = [
  { path: '/', label: 'Executive Summary', exact: true, icon: 'dashboard' },
  { path: '/forecast', label: 'Forecast Dashboard', exact: false, icon: 'trending' },
  // { path: '/scenarios', label: 'Scenario Planning', exact: false, icon: 'branch' }, // disabled
{ path: '/feedback', label: 'Forecast Feedback', exact: false, icon: 'message' },
  { path: '/taxonomy', label: 'Skill Taxonomy', exact: false, icon: 'network' },
  { path: '/tasks', label: 'My Tasks', exact: false, icon: 'check' },
  { path: '/alerts', label: 'My Alerts', exact: false, icon: 'bell' },
];

@Component({
  selector: 'app-layout',
  standalone: true,
  imports: [CommonModule, RouterModule, RouterLink, RouterLinkActive],
  templateUrl: './layout.component.html',
})
export class LayoutComponent {
  navItems = NAV_ITEMS;

  constructor(public authService: AuthService, private router: Router) {}

  handleLogout(): void {
    this.authService.logout();
    this.router.navigate(['/login']);
  }

  get userInitial(): string {
    return this.authService.user?.name?.charAt(0) ?? 'U';
  }

  get userName(): string {
    return this.authService.user?.name ?? '';
  }

  get userRole(): string {
    return this.authService.user?.role?.replace(/_/g, ' ') ?? '';
  }

  get userEmail(): string {
    return this.authService.user?.email ?? '';
  }
}
