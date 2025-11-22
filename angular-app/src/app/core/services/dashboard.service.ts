import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from '../../../environments/environment';
import {
  Advertiser,
  Coupon,
  DashboardContext,
  DashboardFilters,
  FilterOptions,
  GraphData,
  KPIData,
  PaginatedTableResponse,
  Partner,
  PieChartData
} from '../models/dashboard.model';
import { CacheService } from './cache.service';

@Injectable({
  providedIn: 'root'
})
export class DashboardService {
  private readonly API_BASE_URL = environment.apiUrl;

  constructor(
    private http: HttpClient,
    private cacheService: CacheService
  ) { }

  // Get dashboard context (role, permissions, assignments)
  getDashboardContext(): Observable<DashboardContext> {
    return this.cacheService.get(
      'dashboard-context',
      this.http.get<DashboardContext>(`${this.API_BASE_URL}/dashboard/context/`),
      10 * 60 * 1000 // 10 minutes cache
    );
  }

  // Get all filter options (advertisers, partners, coupons) based on user permissions
  // Cache for 10 minutes since filter options don't change often
  getFilterOptions(): Observable<FilterOptions> {
    return this.cacheService.get(
      'filter-options',
      this.http.get<FilterOptions>(`${this.API_BASE_URL}/dashboard/filter-options/`),
      10 * 60 * 1000 // 10 minutes cache
    );
  }

  // Get KPI summary data
  getKPIs(filters: DashboardFilters): Observable<KPIData> {
    let params = this.buildParams(filters);
    return this.http.get<KPIData>(`${this.API_BASE_URL}/dashboard/kpis/`, { params });
  }

  // Get graph data
  getGraphData(filters: DashboardFilters): Observable<GraphData> {
    let params = this.buildParams(filters);
    return this.http.get<GraphData>(`${this.API_BASE_URL}/dashboard/graph-data/`, { params });
  }

  // Get table data with pagination
  getTableData(filters: DashboardFilters, page: number = 1, pageSize: number = 50): Observable<PaginatedTableResponse> {
    let params = this.buildParams(filters);
    params = params.set('page', page.toString());
    params = params.set('page_size', pageSize.toString());
    return this.http.get<PaginatedTableResponse>(`${this.API_BASE_URL}/dashboard/performance-table/`, { params });
  }

  // Get advertisers list
  getAdvertisers(): Observable<Advertiser[]> {
    return this.cacheService.get(
      'advertisers-list',
      this.http.get<Advertiser[]>(`${this.API_BASE_URL}/advertisers/`),
      10 * 60 * 1000
    );
  }

  // Get partners list
  getPartners(): Observable<Partner[]> {
    return this.cacheService.get(
      'partners-list',
      this.http.get<Partner[]>(`${this.API_BASE_URL}/partners/`),
      10 * 60 * 1000
    );
  }

  // Get coupons list
  getCoupons(): Observable<Coupon[]> {
    return this.cacheService.get(
      'coupons-list',
      this.http.get<Coupon[]>(`${this.API_BASE_URL}/coupons/`),
      10 * 60 * 1000
    );
  }

  // Get pie chart data for all campaigns (not paginated)
  getPieChartData(filters: DashboardFilters): Observable<PieChartData[]> {
    const params = this.buildParams(filters);
    return this.http.get<PieChartData[]>(`${this.API_BASE_URL}/dashboard/pie-chart-data/`, { params });
  }

  // Helper to build HTTP params from filters
  private buildParams(filters: DashboardFilters): HttpParams {
    let params = new HttpParams();

    if (filters.partner_type) {
      params = params.set('partner_type', filters.partner_type);
    }
    if (filters.advertiser_id) {
      const advIds = Array.isArray(filters.advertiser_id) ? filters.advertiser_id : [filters.advertiser_id];
      advIds.forEach(id => params = params.append('advertiser_id', id.toString()));
    }
    if (filters.partner_id) {
      const partnerIds = Array.isArray(filters.partner_id) ? filters.partner_id : [filters.partner_id];
      partnerIds.forEach(id => params = params.append('partner_id', id.toString()));
    }
    if (filters.coupon_code) {
      const codes = Array.isArray(filters.coupon_code) ? filters.coupon_code : [filters.coupon_code];
      codes.forEach(code => params = params.append('coupon_code', code));
    }
    if (filters.date_from) {
      params = params.set('date_from', filters.date_from);
    }
    if (filters.date_to) {
      params = params.set('date_to', filters.date_to);
    }

    return params;
  }
}
