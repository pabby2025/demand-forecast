import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { AuthService } from '../../core/services/auth.service';
import { ApiService } from '../../core/services/api.service';
import { User } from '../../core/models';

const PRESETS = [
  { label: 'Login as SL COO', email: 'sl.coo@company.com', password: 'password' },
  { label: 'Login as Market COO', email: 'market.coo@company.com', password: 'password' },
  { label: 'Login as CFT Planner', email: 'cft.planner@company.com', password: 'password' },
];

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './login.component.html',
})
export class LoginComponent {
  email = '';
  password = '';
  loading = false;
  error = '';
  presets = PRESETS;

  constructor(
    private authService: AuthService,
    private apiService: ApiService,
    private router: Router
  ) {}

  async handleLogin(preset?: { email: string; password: string }): Promise<void> {
    const creds = preset ?? { email: this.email, password: this.password };
    if (!creds.email || !creds.password) {
      this.error = 'Please enter email and password.';
      return;
    }
    this.loading = true;
    this.error = '';
    this.apiService.login(creds.email, creds.password).subscribe({
      next: (data: { user: unknown; token: string }) => {
        this.authService.login(data.user as User, data.token);
        this.router.navigate(['/']);
        this.loading = false;
      },
      error: () => {
        this.error = 'Invalid credentials. Please try again.';
        this.loading = false;
      },
    });
  }
}
