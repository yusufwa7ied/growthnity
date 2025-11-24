import { HttpErrorResponse, HttpInterceptorFn } from '@angular/common/http';
import { inject } from '@angular/core';
import { Router } from '@angular/router';
import { catchError, retry, switchMap, throwError, timer } from 'rxjs';
import { AuthService } from '../services/auth.service';

export const authInterceptor: HttpInterceptorFn = (req, next) => {
  const token = localStorage.getItem('auth_token');
  const router = inject(Router);
  const authService = inject(AuthService);

  // Clone request with auth token if available
  const clonedRequest = token
    ? req.clone({
      setHeaders: {
        Authorization: `Bearer ${token}`
      }
    })
    : req;

  return next(clonedRequest).pipe(
    // Retry on network errors (not on 4xx/5xx errors)
    retry({
      count: 2,
      delay: (error, retryCount) => {
        // Only retry on network errors (status 0) or server errors (5xx)
        if (error instanceof HttpErrorResponse &&
          (error.status === 0 || error.status >= 500)) {
          // Exponential backoff: 1s, 2s
          return timer(1000 * retryCount);
        }
        // Don't retry on client errors
        throw error;
      }
    }),
    catchError((error: HttpErrorResponse) => {
      // Handle authentication errors - try to refresh token
      if (error.status === 401) {
        // Skip refresh for login endpoint - just logout
        if (req.url.includes('/login/')) {
          authService.handleSessionExpired();
          if (!router.url.includes('/login')) {
            router.navigate(['/login']);
          }
          return throwError(() => error);
        }

        // For token/refresh/ endpoint, don't try to refresh again - just logout
        if (req.url.includes('/token/refresh/')) {
          authService.handleSessionExpired();
          if (!router.url.includes('/login')) {
            router.navigate(['/login']);
          }
          return throwError(() => error);
        }

        // Try to refresh the token
        return authService.refreshToken().pipe(
          switchMap((response) => {
            // Token refreshed successfully, retry the original request
            const newToken = response.access;
            const retryRequest = req.clone({
              setHeaders: {
                Authorization: `Bearer ${newToken}`
              }
            });
            return next(retryRequest);
          }),
          catchError((refreshError) => {
            // Refresh failed, force logout
            authService.handleSessionExpired();
            if (!router.url.includes('/login')) {
              router.navigate(['/login']);
            }
            return throwError(() => error);
          })
        );
      }

      if (error.status === 403) {
        authService.handleSessionExpired();
        if (!router.url.includes('/login')) {
          router.navigate(['/login']);
        }
      }

      // Handle network errors with user-friendly message
      if (error.status === 0) {
        console.error('Network error. Please check your connection.');
      }

      return throwError(() => error);
    })
  );
};
