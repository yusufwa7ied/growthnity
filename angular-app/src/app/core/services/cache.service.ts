import { Injectable } from '@angular/core';
import { Observable, of, Subject } from 'rxjs';
import { shareReplay, tap } from 'rxjs/operators';

interface CacheEntry<T> {
    data: T;
    timestamp: number;
    ttl: number; // Time to live in milliseconds
}

@Injectable({
    providedIn: 'root'
})
export class CacheService {
    private cache = new Map<string, CacheEntry<any>>();
    private requests = new Map<string, Observable<any>>();
    private cacheInvalidated$ = new Subject<string>();

    constructor() { }

    /**
     * Get cached data or execute the request if cache is invalid/expired
     */
    get<T>(key: string, request: Observable<T>, ttl: number = 5 * 60 * 1000): Observable<T> {
        // Check if data is cached and not expired
        const cached = this.cache.get(key);
        if (cached && Date.now() - cached.timestamp < cached.ttl) {
            return of(cached.data as T);
        }

        // Check if request is already in flight to avoid duplicates
        if (this.requests.has(key)) {
            return this.requests.get(key)!;
        }

        // Execute request and cache result
        const cachedRequest$ = request.pipe(
            tap(data => {
                this.cache.set(key, {
                    data,
                    timestamp: Date.now(),
                    ttl
                });
                this.requests.delete(key);
            }),
            shareReplay(1)
        );

        this.requests.set(key, cachedRequest$);
        return cachedRequest$;
    }

    /**
     * Set cache directly
     */
    set<T>(key: string, data: T, ttl: number = 5 * 60 * 1000): void {
        this.cache.set(key, {
            data,
            timestamp: Date.now(),
            ttl
        });
    }

    /**
     * Invalidate cache entry
     */
    invalidate(key: string): void {
        this.cache.delete(key);
        this.requests.delete(key);
        this.cacheInvalidated$.next(key);
    }

    /**
     * Invalidate all cache entries
     */
    invalidateAll(): void {
        this.cache.clear();
        this.requests.clear();
    }

    /**
     * Observable for cache invalidation events
     */
    onInvalidated(key: string): Observable<string> {
        return this.cacheInvalidated$.asObservable();
    }
}
