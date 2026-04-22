import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { AuthService } from '../../core/services/auth.service';
import { ApiService } from '../../core/services/api.service';

@Component({
  selector: 'app-change-password',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './change-password.component.html',
})
export class ChangePasswordComponent implements OnInit {
  currentCredential = '';
  newPassword = '';
  confirmPassword = '';
  showCurrent = false;
  showNew = false;
  loading = false;
  error = '';
  success = '';
  isFirstLogin = false;

  constructor(
    public authService: AuthService,
    private apiService: ApiService,
    public router: Router,
    private route: ActivatedRoute
  ) {}

  ngOnInit(): void {
    this.isFirstLogin = this.route.snapshot.queryParamMap.get('first') === '1';
  }

  get email(): string { return this.authService.user?.email ?? ''; }

  handleSubmit(): void {
    this.error = '';
    this.success = '';
    if (!this.currentCredential.trim()) {
      this.error = 'Please enter your current password or access code.'; return;
    }
    if (this.newPassword.length < 8) {
      this.error = 'New password must be at least 8 characters.'; return;
    }
    if (this.newPassword !== this.confirmPassword) {
      this.error = 'New passwords do not match.'; return;
    }
    if (this.newPassword === this.currentCredential) {
      this.error = 'New password must be different from the access code or current password.'; return;
    }

    this.loading = true;
    this.apiService.changePassword(this.email, this.currentCredential.trim(), this.newPassword).subscribe({
      next: () => {
        this.authService.clearMustChangePassword();
        this.success = 'Password changed successfully.';
        this.loading = false;
        // Redirect to home after short delay
        setTimeout(() => this.router.navigate(['/']), 1500);
      },
      error: (err) => {
        this.error = err.error?.detail ?? 'Failed to change password. Please check your current credential.';
        this.loading = false;
      },
    });
  }
}
