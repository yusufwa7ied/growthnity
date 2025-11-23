import { HttpClient } from '@angular/common/http';
import { Injectable, signal } from '@angular/core';
import { Router } from '@angular/router';
import { catchError, map, Observable, switchMap, tap, throwError } from 'rxjs';
import { environment } from '../../../environments/environment';
import { DashboardContext, LoginCredentials, LoginResponse, User } from '../models/user.model';

export interface TokenResponse {
  access: string;
  refresh: string;
}

@Injectable({
  providedIn: 'root'
})
export class AuthService {
  private readonly API_BASE_URL = environment.apiUrl;
  private readonly TOKEN_KEY = 'auth_token';
  private readonly REFRESH_TOKEN_KEY = 'refresh_token';
  private readonly USER_KEY = 'logged_user';
  private readonly IDLE_TIMEOUT = 15 * 60 * 1000; // 15 minutes idle timeout
  private idleTimer: any;

  // Signal for reactive authentication state
  private isAuthenticatedSignal = signal<boolean>(this.hasToken());
  public isAuthenticated = this.isAuthenticatedSignal.asReadonly();

  private currentUserSignal = signal<User | null>(this.getStoredUser());
  public currentUser = this.currentUserSignal.asReadonly();

  constructor(
    private http: HttpClient,
    private router: Router
  ) {
    this.initializeIdleTimer();
  }

  login(credentials: LoginCredentials): Observable<{ success: boolean; error?: string }> {
    return this.http.post<TokenResponse>(`${this.API_BASE_URL}/login/`, credentials).pipe(
      tap(response => {
        if (response.access) {
          this.setToken(response.access);
          this.setRefreshToken(response.refresh);
          this.isAuthenticatedSignal.set(true);
          this.resetIdleTimer();
        }
      }),
      switchMap(response => {
        // Wait for user context before completing login
        return this.getDashboardContext(response.access).pipe(
          tap(context => {
            const user: User = {
              username: credentials.username,
              role: context.role,
              partner_id: context.partner_id,
              department: context.department
            };
            this.setUser(user);
            this.currentUserSignal.set(user);
          }),
          map(() => ({ success: true }))
        );
      }),
      catchError((error) => {
        this.clearAuth();
        let errorMessage = 'Login failed. Please try again.';

        if (error.status === 401 || error.status === 400) {
          errorMessage = 'Invalid credentials. Please try again.';
        } else if (error.status === 0) {
          errorMessage = 'Server timeout. Try again.';
        }

        return throwError(() => ({ success: false, error: errorMessage }));
      })
    );
  }

  /**
   * Refresh the access token using the refresh token
   */
  refreshToken(): Observable<TokenResponse> {
    const refreshToken = this.getRefreshToken();
    if (!refreshToken) {
      return throwError(() => new Error('No refresh token available'));
    }

    return this.http.post<TokenResponse>(`${this.API_BASE_URL}/token/refresh/`, { refresh: refreshToken }).pipe(
      tap(response => {
        if (response.access) {
          this.setToken(response.access);
          if (response.refresh) {
            this.setRefreshToken(response.refresh);
          }
          this.resetIdleTimer();
        }
      }),
      catchError((error) => {
        // Refresh token invalid or expired - force logout
        this.clearAuth();
        this.router.navigate(['/login'], { replaceUrl: true });
        return throwError(() => error);
      })
    );
  }

  /**
   * Initialize idle timer for auto-logout
   */
  private initializeIdleTimer(): void {
    // Listen for user activity
    if (typeof window !== 'undefined') {
      ['mousedown', 'keydown', 'scroll', 'touchstart', 'click'].forEach(event => {
        document.addEventListener(event, () => this.resetIdleTimer(), true);
      });
    }
  }

  /**
   * Reset the idle timer
   */
  private resetIdleTimer(): void {
    if (!this.isAuthenticatedUser()) {
      return; // Don't set timer if not authenticated
    }

    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
    }

    this.idleTimer = setTimeout(() => {
      console.warn('User idle timeout - logging out');
      this.logout();
    }, this.IDLE_TIMEOUT);
  }

  private getDashboardContext(token: string): Observable<DashboardContext> {
    return this.http.get<DashboardContext>(`${this.API_BASE_URL}/dashboard/context/`, {
      headers: { Authorization: `Bearer ${token}` }
    });
  }

  logout(): void {
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
    }
    this.clearAuth();
    // Navigate to login without reload for smoother UX
    this.router.navigate(['/login'], { replaceUrl: true });
  }

  /**
   * Called by the HTTP interceptor when session expires (401/403)
   * Clears auth state without page reload to maintain responsive UI
   */
  handleSessionExpired(): void {
    this.clearAuth();
    this.router.navigate(['/login'], { replaceUrl: true });
  }

  private setToken(token: string): void {
    localStorage.setItem(this.TOKEN_KEY, token);
  }

  getToken(): string | null {
    return localStorage.getItem(this.TOKEN_KEY);
  }

  private setRefreshToken(token: string): void {
    localStorage.setItem(this.REFRESH_TOKEN_KEY, token);
  }

  private getRefreshToken(): string | null {
    return localStorage.getItem(this.REFRESH_TOKEN_KEY);
  }

  private setUser(user: User): void {
    localStorage.setItem(this.USER_KEY, JSON.stringify(user));
  }

  private getStoredUser(): User | null {
    const userJson = localStorage.getItem(this.USER_KEY);
    return userJson ? JSON.parse(userJson) : null;
  }

  private hasToken(): boolean {
    return !!this.getToken();
  }

  /**
   * Check if user is currently authenticated
   * Used by auth guard to determine route access
   */
  isAuthenticatedUser(): boolean {
    return this.isAuthenticated();
  }

  private clearAuth(): void {
    localStorage.removeItem(this.TOKEN_KEY);
    localStorage.removeItem(this.REFRESH_TOKEN_KEY);
    localStorage.removeItem(this.USER_KEY);
    this.isAuthenticatedSignal.set(false);
    this.currentUserSignal.set(null);
  }
}
