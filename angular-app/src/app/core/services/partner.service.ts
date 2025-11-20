import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from '../../../environments/environment';

export interface SpecialPayoutInfo {
    advertiser: string;
    advertiser_id: number;
    ftu_payout: number | null;
    rtu_payout: number | null;
    ftu_fixed_bonus: number | null;
    rtu_fixed_bonus: number | null;
    rate_type: string;
}

export interface Partner {
    id: number;
    name: string;
    partner_type: string;
    email: string;
    phone: string;
    has_special_payouts?: boolean;
    special_payouts_count?: number;
    special_payouts?: SpecialPayoutInfo[];
}

export interface PartnerFormData {
    name: string;
    partner_type: string;
    email?: string;
    phone?: string;
}

export interface PartnerPayout {
    id?: number;
    advertiser: string;
    advertiser_id: number;
    partner: string;
    partner_id: number | null;
    ftu_payout: number | null;
    rtu_payout: number | null;
    ftu_fixed_bonus: number | null;
    rtu_fixed_bonus: number | null;
    exchange_rate: number | null;
    currency: string;
    rate_type: string;
    condition: string;
    start_date: string | null;
    end_date: string | null;
}

export interface PartnerPayoutFormData {
    advertiser: number;
    partner: number | null;
    ftu_payout: number | null;
    rtu_payout: number | null;
    ftu_fixed_bonus: number | null;
    rtu_fixed_bonus: number | null;
    exchange_rate: number | null;
    currency: string;
    rate_type: string;
    condition?: string;
    start_date: string | null;
    end_date: string | null;
}

@Injectable({
    providedIn: 'root'
})
export class PartnerService {
    private apiUrl = `${environment.apiUrl}/admin/partners/`;
    private payoutsUrl = `${environment.apiUrl}/payouts/`;

    constructor(private http: HttpClient) { }

    getPartners(): Observable<Partner[]> {
        return this.http.get<Partner[]>(this.apiUrl);
    }

    createPartner(data: PartnerFormData): Observable<Partner> {
        return this.http.post<Partner>(this.apiUrl, data);
    }

    updatePartner(id: number, data: PartnerFormData): Observable<Partner> {
        return this.http.put<Partner>(`${this.apiUrl}${id}/`, data);
    }

    deletePartner(id: number): Observable<any> {
        return this.http.delete(`${this.apiUrl}${id}/`);
    }

    getPartnerPayouts(partnerId?: number, advertiserId?: number): Observable<PartnerPayout[]> {
        let url = this.payoutsUrl;
        const params: string[] = [];

        if (partnerId) params.push(`partner_id=${partnerId}`);
        if (advertiserId) params.push(`advertiser_id=${advertiserId}`);

        if (params.length > 0) {
            url += '?' + params.join('&');
        }

        return this.http.get<PartnerPayout[]>(url);
    }

    createPartnerPayout(data: PartnerPayoutFormData): Observable<any> {
        return this.http.post(this.payoutsUrl, data);
    }

    updatePartnerPayout(id: number, data: PartnerPayoutFormData): Observable<any> {
        return this.http.put(`${this.payoutsUrl}${id}/`, data);
    }

    deletePartnerPayout(id: number): Observable<any> {
        return this.http.delete(`${this.payoutsUrl}${id}/`);
    }
}
