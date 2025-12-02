import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { shareReplay } from 'rxjs/operators';
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

  // Cache for in-flight requests to prevent duplicate API calls
  private kpiCache: Map<string, Observable<KPIData>> = new Map();
  private tableDataCache: Map<string, Observable<PaginatedTableResponse>> = new Map();
  private graphDataCache: Map<string, Observable<GraphData>> = new Map();
  private pieChartCache: Map<string, Observable<PieChartData[]>> = new Map();

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
  getFilterOptions(filters?: DashboardFilters): Observable<FilterOptions> {
    // Build cache key including team_member_id filter
    const teamMemberId = filters?.team_member_id ? 
      (Array.isArray(filters.team_member_id) ? filters.team_member_id.join(',') : filters.team_member_id) : '';
    const cacheKey = `filter-options:${teamMemberId}`;
    
    // Build params - only include team_member_id for filtering
    let params = new HttpParams();
    if (filters?.team_member_id) {
      const teamMemberIds = Array.isArray(filters.team_member_id) ? filters.team_member_id : [filters.team_member_id];
      teamMemberIds.forEach(id => params = params.append('team_member_id', id.toString()));
    }
    
    return this.cacheService.get(
      cacheKey,
      this.http.get<FilterOptions>(`${this.API_BASE_URL}/dashboard/filter-options/`, { params }),
      10 * 60 * 1000 // 10 minutes cache
    );
  }

  // Get KPI summary data
  // Uses request deduplication - multiple subscribers to same request share response
  getKPIs(filters: DashboardFilters): Observable<KPIData> {
    const cacheKey = this.buildCacheKey('kpis', filters);

    if (this.kpiCache.has(cacheKey)) {
      return this.kpiCache.get(cacheKey)!;
    }

    const request = this.http.get<KPIData>(`${this.API_BASE_URL}/dashboard/kpis/`, {
      params: this.buildParams(filters)
    }).pipe(
      shareReplay(1) // Share single request among subscribers
    );

    this.kpiCache.set(cacheKey, request);
    // Clear cache after 30 seconds to allow fresh data
    setTimeout(() => this.kpiCache.delete(cacheKey), 30000);

    return request;
  }

  // Get graph data
  // Uses request deduplication
  getGraphData(filters: DashboardFilters): Observable<GraphData> {
    const cacheKey = this.buildCacheKey('graph-data', filters);

    if (this.graphDataCache.has(cacheKey)) {
      return this.graphDataCache.get(cacheKey)!;
    }

    const request = this.http.get<GraphData>(`${this.API_BASE_URL}/dashboard/graph-data/`, {
      params: this.buildParams(filters)
    }).pipe(
      shareReplay(1)
    );

    this.graphDataCache.set(cacheKey, request);
    setTimeout(() => this.graphDataCache.delete(cacheKey), 30000);

    return request;
  }

  // Get table data with pagination
  // Uses request deduplication
  getTableData(filters: DashboardFilters, page: number = 1, pageSize: number = 50): Observable<PaginatedTableResponse> {
    const cacheKey = this.buildCacheKey(`table-data-p${page}`, filters);

    if (this.tableDataCache.has(cacheKey)) {
      return this.tableDataCache.get(cacheKey)!;
    }

    let params = this.buildParams(filters);
    params = params.set('page', page.toString());
    params = params.set('page_size', pageSize.toString());

    const request = this.http.get<PaginatedTableResponse>(`${this.API_BASE_URL}/dashboard/performance-table/`, { params }).pipe(
      shareReplay(1)
    );

    this.tableDataCache.set(cacheKey, request);
    setTimeout(() => this.tableDataCache.delete(cacheKey), 30000);

    return request;
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

  // Get partners filtered by partner_type
  getPartnersByType(partnerType: string): Observable<Partner[]> {
    const params = new HttpParams().set('partner_type', partnerType);
    return this.http.get<Partner[]>(`${this.API_BASE_URL}/partners/`, { params });
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
  // Uses request deduplication
  getPieChartData(filters: DashboardFilters): Observable<PieChartData[]> {
    const cacheKey = this.buildCacheKey('pie-chart-data', filters);

    if (this.pieChartCache.has(cacheKey)) {
      return this.pieChartCache.get(cacheKey)!;
    }

    const request = this.http.get<PieChartData[]>(`${this.API_BASE_URL}/dashboard/pie-chart-data/`, {
      params: this.buildParams(filters)
    }).pipe(
      shareReplay(1)
    );

    this.pieChartCache.set(cacheKey, request);
    setTimeout(() => this.pieChartCache.delete(cacheKey), 30000);

    return request;
  }

  // Get team members list
  getTeamMembers(): Observable<any[]> {
    return this.http.get<any[]>(`${this.API_BASE_URL}/team-members/`);
  }

  // Get team members filtered by department (partner_type)
  getTeamMembersByDepartment(partnerType: string): Observable<any[]> {
    const params = new HttpParams().set('department', partnerType);
    return this.http.get<any[]>(`${this.API_BASE_URL}/team-members/`, { params });
  }

  // Get advertiser detail summary for modal
  getAdvertiserDetailSummary(advertiserId: number, filters: DashboardFilters): Observable<any> {
    let params = new HttpParams().set('advertiser_id', advertiserId.toString());

    if (filters.date_from) {
      params = params.set('date_from', filters.date_from);
    }
    if (filters.date_to) {
      params = params.set('date_to', filters.date_to);
    }

    return this.http.get<any>(`${this.API_BASE_URL}/dashboard/advertiser-detail-summary/`, { params });
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
    if (filters.team_member_id) {
      const teamMemberIds = Array.isArray(filters.team_member_id) ? filters.team_member_id : [filters.team_member_id];
      teamMemberIds.forEach(id => params = params.append('team_member_id', id.toString()));
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

  // Helper to build cache key from endpoint and filters
  private buildCacheKey(endpoint: string, filters: DashboardFilters): string {
    const filterParts = [
      filters.partner_type || '',
      Array.isArray(filters.advertiser_id) ? filters.advertiser_id.join(',') : (filters.advertiser_id || ''),
      Array.isArray(filters.partner_id) ? filters.partner_id.join(',') : (filters.partner_id || ''),
      Array.isArray(filters.team_member_id) ? filters.team_member_id.join(',') : (filters.team_member_id || ''),
      Array.isArray(filters.coupon_code) ? filters.coupon_code.join(',') : (filters.coupon_code || ''),
      filters.date_from || '',
      filters.date_to || ''
    ].join('|');
    return `${endpoint}:${filterParts}`;
  }
}
