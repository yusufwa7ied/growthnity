import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from '../../../environments/environment';

export interface MediaBuyerSpend {
    id?: number;
    date: string;
    advertiser_id: number;
    advertiser_name?: string;
    partner_id: number;
    partner_name?: string;
    coupon_id?: number | null;
    coupon_code?: string | null;
    platform: string;
    amount_spent: number;
    currency: string;
    created?: boolean;
}

export interface Platform {
    label: string;
    value: string;
}

export interface SpendFilters {
    date_from?: string;
    date_to?: string;
    advertiser_id?: number;
    partner_id?: number;
}

@Injectable({
    providedIn: 'root'
})
export class MediaBuyerSpendService {
    private apiUrl = `${environment.apiUrl}/media-buyer-spend/`;

    constructor(private http: HttpClient) { }

    getSpendRecords(filters?: SpendFilters): Observable<MediaBuyerSpend[]> {
        let params = new HttpParams();

        if (filters) {
            if (filters.date_from) params = params.set('date_from', filters.date_from);
            if (filters.date_to) params = params.set('date_to', filters.date_to);
            if (filters.advertiser_id) params = params.set('advertiser_id', filters.advertiser_id.toString());
            if (filters.partner_id) params = params.set('partner_id', filters.partner_id.toString());
        }

        return this.http.get<MediaBuyerSpend[]>(this.apiUrl, { params });
    }

    createSpendRecord(data: MediaBuyerSpend): Observable<MediaBuyerSpend> {
        return this.http.post<MediaBuyerSpend>(this.apiUrl, data);
    }

    deleteSpendRecord(id: number): Observable<any> {
        return this.http.delete(`${this.apiUrl}${id}/delete/`);
    }
}
