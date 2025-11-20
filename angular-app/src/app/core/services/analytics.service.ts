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
        orders: number;
        revenue: number;
        profit: number;
        spend: number;
    };

    achievement_pct: {
        orders: number;
        revenue: number;
        profit: number;
        spend: number;
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
}

export interface DepartmentBreakdown {
    code: string;
    name: string;
    orders: number;
    revenue: number;
    profit: number;
    payout: number;
    targets: {
        orders: number;
        revenue: number;
        profit: number;
    };
    achievement: {
        orders_pct: number;
        revenue_pct: number;
        profit_pct: number;
    };
}

@Injectable({
    providedIn: 'root'
})
export class AnalyticsService {
    private apiUrl = `${environment.apiUrl}/analytics/performance/`;

    constructor(private http: HttpClient) { }

    getPerformanceAnalytics(
        advertiserId?: number,
        partnerId?: number,
        partnerType?: string,
        month?: string
    ): Observable<PerformanceAnalytics> {
        let params = new HttpParams();

        if (advertiserId) {
            params = params.set('advertiser_id', advertiserId.toString());
        }
        if (partnerId) {
            params = params.set('partner_id', partnerId.toString());
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