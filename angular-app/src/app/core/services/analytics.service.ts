import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from '../../../environments/environment';

export interface PerformanceAnalytics {
    month: string;
    days_in_month: number;
    days_elapsed: number;
    days_remaining: number;

    mtd: {
        orders: number;
        revenue: number;
        profit: number;
        payout: number;
        spend: number;
    };

    targets: {
        orders: number | null;
        revenue: number | null;
        profit: number | null;
        spend: number | null;
    };

    achievement_pct: {
        orders: number | null;
        revenue: number | null;
        profit: number | null;
        spend: number | null;
    };

    run_rate: {
        projected_orders: number;
        projected_revenue: number;
        projected_profit: number;
        projected_spend: number;
        orders_pct: number;
        revenue_pct: number;
        profit_pct: number;
    };

    pacing: {
        expected_progress_pct: number;
        orders_pacing: number;
        revenue_pacing: number;
        profit_pacing: number;
        status: 'Ahead' | 'On Track' | 'Behind';
    };

    daily: {
        today_orders: number;
        today_revenue: number;
        today_profit: number;
        required_daily_orders: number;
        required_daily_revenue: number;
        required_daily_profit: number;
        orders_achievement_pct: number;
        revenue_achievement_pct: number;
    };

    roi: {
        roas: number;
        roi_pct: number;
        cpa: number;
    };

    efficiency: {
        avg_order_value: number;
        profit_margin_pct: number;
    };

    department_breakdown?: DepartmentBreakdown[];
    is_department_restricted?: boolean;
    simplified_analytics?: SimplifiedAnalytics;
}

export interface SimplifiedAnalytics {
    is_department_restricted: boolean;
    earnings: {
        total_payout: number;
        total_orders: number;
        avg_commission: number;
        sales_volume: number;
    };
    yesterday: {
        payout: number;
        orders: number;
        sales: number;
    };
    growth: {
        payout_vs_last_month_pct: number;
        orders_vs_last_month_pct: number;
        commission_vs_last_month_pct: number;
    };
    run_rate: {
        projected_payout: number;
        projected_orders: number;
        vs_last_month_pct: number;
        status: 'Ahead' | 'On Track' | 'Behind';
        last_month_payout: number;
    };
}

export interface DepartmentBreakdown {
    code: string;
    name: string;
    orders: number;
    revenue: number;
    profit: number;
    payout: number;
    targets: {
        orders: number | null;
        revenue: number | null;
        profit: number | null;
    };
    achievement: {
        orders_pct: number | null;
        revenue_pct: number | null;
        profit_pct: number | null;
    };
}

@Injectable({
    providedIn: 'root'
})
export class AnalyticsService {
    private apiUrl = `${environment.apiUrl}/analytics/performance/`;

    constructor(private http: HttpClient) { }

    getPerformanceAnalytics(
        advertiserIds?: number | number[],
        partnerIds?: number | number[],
        partnerType?: string,
        month?: string
    ): Observable<PerformanceAnalytics> {
        let params = new HttpParams();

        // Support both single value and array for advertiserIds
        if (advertiserIds) {
            const ids = Array.isArray(advertiserIds) ? advertiserIds : [advertiserIds];
            ids.forEach(id => {
                params = params.append('advertiser_id', id.toString());
            });
        }

        // Support both single value and array for partnerIds
        if (partnerIds) {
            const ids = Array.isArray(partnerIds) ? partnerIds : [partnerIds];
            ids.forEach(id => {
                params = params.append('partner_id', id.toString());
            });
        }

        if (partnerType) {
            params = params.set('partner_type', partnerType);
        }
        if (month) {
            params = params.set('month', month);
        }

        return this.http.get<PerformanceAnalytics>(this.apiUrl, { params });
    }
}