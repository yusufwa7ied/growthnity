import { HttpClient } from '@angular/common/http';
import { Injectable, signal } from '@angular/core';
import { Router } from '@angular/router';
import { catchError, map, Observable, switchMap, tap, throwError } from 'rxjs';
import { environment } from '../../../environments/environment';
import { DashboardContext, LoginCredentials, LoginResponse, User } from '../models/user.model';

@Injectable({
  providedIn: 'root'
})
export class AuthService {
  private readonly API_BASE_URL = environment.apiUrl;
  private readonly TOKEN_KEY = 'auth_token';
  private readonly USER_KEY = 'logged_user';

  // Signal for reactive authentication state
  private isAuthenticatedSignal = signal<boolean>(this.hasToken());
  public isAuthenticated = this.isAuthenticatedSignal.asReadonly();

  private currentUserSignal = signal<User | null>(this.getStoredUser());
  public currentUser = this.currentUserSignal.asReadonly();

  constructor(
    private http: HttpClient,
    private router: Router
  ) { }

  login(credentials: LoginCredentials): Observable<{ success: boolean; error?: string }> {
    console.log('🌐 AuthService: Calling login API...');
    return this.http.post<LoginResponse>(`${this.API_BASE_URL}/login/`, credentials).pipe(
      tap(response => {
        console.log('🌐 AuthService: Token received');
        if (response.access) {
          this.setToken(response.access);
          this.isAuthenticatedSignal.set(true);
        }
      }),
      switchMap(response => {
        // Wait for user context before completing login
        console.log('🌐 AuthService: Fetching dashboard context...');
        return this.getDashboardContext(response.access).pipe(
          tap(context => {
            console.log('🌐 AuthService: Context received:', context.role);
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
        console.log('🌐 AuthService: Login error:', error.status, error.message);
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

  private getDashboardContext(token: string): Observable<DashboardContext> {
    return this.http.get<DashboardContext>(`${this.API_BASE_URL}/dashboard/context/`, {
      headers: { Authorization: `Bearer ${token}` }
    });
  }

  logout(): void {
    this.clearAuth();
    // Navigate and force a full page reload to clear component state
    this.router.navigate(['/login']).then(() => {
      // This ensures clean state on next login
      window.location.reload();
    });
  }

  /**
   * Called by the HTTP interceptor when session expires (401/403)
   * Clears auth state without page reload to maintain responsive UI
   */
  handleSessionExpired(): void {
    this.clearAuth();
  }

  private setToken(token: string): void {
    localStorage.setItem(this.TOKEN_KEY, token);
  }

  getToken(): string | null {
    return localStorage.getItem(this.TOKEN_KEY);
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

  private clearAuth(): void {
    localStorage.removeItem(this.TOKEN_KEY);
    localStorage.removeItem(this.USER_KEY);
    this.isAuthenticatedSignal.set(false);
    this.currentUserSignal.set(null);
  }
}
