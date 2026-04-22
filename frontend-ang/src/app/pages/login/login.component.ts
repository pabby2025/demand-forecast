import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { AuthService } from '../../core/services/auth.service';
import { ApiService } from '../../core/services/api.service';
import { User } from '../../core/models';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './login.component.html',
})
export class LoginComponent {
  email = '';
  credential = '';
  showCredential = false;
  loading = false;
  error = '';

  constructor(
    private authService: AuthService,
    private apiService: ApiService,
    private router: Router
  ) {}

  toggleShowCredential(): void {
    this.showCredential = !this.showCredential;
  }

  handleLogin(): void {
    this.error = '';
    if (!this.email.trim()) { this.error = 'Please enter your email address.'; return; }
    if (!this.credential.trim()) { this.error = 'Please enter your access code or password.'; return; }

    this.loading = true;
    this.apiService.login(this.email.trim(), this.credential.trim()).subscribe({
      next: (data) => {
        this.authService.login(data.user as User, data.token);
        this.loading = false;
        if (data.must_change_password) {
          this.router.navigate(['/change-password'], { queryParams: { first: '1' } });
        } else {
          this.router.navigate(['/']);
        }
      },
      error: (err) => {
        this.error = err.status === 401
          ? 'Invalid email or access code / password. Please try again.'
          : 'Something went wrong. Please try again.';
        this.loading = false;
      },
    });
  }
}
