import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { User } from '../models';

@Injectable({ providedIn: 'root' })
export class AuthService {
  private userSubject = new BehaviorSubject<User | null>(this.loadUser());
  user$ = this.userSubject.asObservable();

  get user(): User | null {
    return this.userSubject.value;
  }

  private loadUser(): User | null {
    const stored = localStorage.getItem('dp_user');
    return stored ? (JSON.parse(stored) as User) : null;
  }

  login(userData: User, token: string): void {
    localStorage.setItem('dp_user', JSON.stringify(userData));
    localStorage.setItem('dp_token', token);
    this.userSubject.next(userData);
  }

  logout(): void {
    localStorage.removeItem('dp_user');
    localStorage.removeItem('dp_token');
    this.userSubject.next(null);
  }

  isLoggedIn(): boolean {
    return this.userSubject.value !== null;
  }
}
