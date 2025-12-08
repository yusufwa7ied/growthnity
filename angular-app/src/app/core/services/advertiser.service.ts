import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from '../../../environments/environment';

export interface PartnerPayout {
    id?: number;
    partner_id: number;
    partner_name?: string;
    ftu_payout: number | null;
    rtu_payout: number | null;
    ftu_fixed_bonus?: number | null;
    rtu_fixed_bonus?: number | null;
    exchange_rate?: number | null;
    currency?: string;
    rate_type: string;
    condition?: string;
    start_date?: string | null;
    end_date?: string | null;
}

export interface Advertiser {
    id: number;
    name: string;
    attribution: string;
    rev_rate_type: string;
    rev_ftu_rate: number | null;
    rev_rtu_rate: number | null;
    rev_ftu_fixed_bonus?: number | null;
    rev_rtu_fixed_bonus?: number | null;
    currency: string;
    exchange_rate: number | null;
    default_payout_rate_type: string;
    default_ftu_payout: number | null;
    default_rtu_payout: number | null;
    default_ftu_fixed_bonus?: number | null;
    default_rtu_fixed_bonus?: number | null;
    partner_payouts: PartnerPayout[];
    total_partners?: number;
    active_partners?: number;
    total_coupons?: number;
    active_coupons?: number;
    stats?: {
        coupon_count: number;
        partner_count: number;
    };
}

export interface AdvertiserFormData {
    name: string;
    attribution: string;
    rev_rate_type: string;
    rev_ftu_rate: number | null;
    rev_rtu_rate: number | null;
    rev_ftu_fixed_bonus?: number | null;
    rev_rtu_fixed_bonus?: number | null;
    currency: string;
    exchange_rate: number | null;
    default_payout_rate_type: string;
    default_ftu_payout: number | null;
    default_rtu_payout: number | null;
    default_ftu_fixed_bonus?: number | null;
    default_rtu_fixed_bonus?: number | null;
    partner_payouts: PartnerPayout[];
}

@Injectable({
    providedIn: 'root'
})
export class AdvertiserService {
    private apiUrl = `${environment.apiUrl}/admin/advertisers/`;

    constructor(private http: HttpClient) { }

    getAdvertisers(): Observable<Advertiser[]> {
        return this.http.get<Advertiser[]>(this.apiUrl);
    }

    createAdvertiser(data: AdvertiserFormData): Observable<Advertiser> {
        return this.http.post<Advertiser>(`${this.apiUrl}create/`, data);
    }

    updateAdvertiser(id: number, data: AdvertiserFormData): Observable<Advertiser> {
        return this.http.put<Advertiser>(`${this.apiUrl}${id}/`, data);
    }

    deleteAdvertiser(id: number): Observable<any> {
        return this.http.delete(`${this.apiUrl}${id}/delete/`);
    }

    getPartners(): Observable<any[]> {
        return this.http.get<any[]>(`${environment.apiUrl}/partners/`);
    }

    getCoupons(): Observable<any[]> {
        return this.http.get<any[]>(`${environment.apiUrl}/coupons/`);
    }

    getCancellationRates(advertiserId: number): Observable<any[]> {
        return this.http.get<any[]>(`${this.apiUrl}${advertiserId}/cancellation-rates/`);
    }

    createCancellationRate(advertiserId: number, rate: any): Observable<any> {
        return this.http.post<any>(`${this.apiUrl}${advertiserId}/cancellation-rates/create/`, rate);
    }
}
