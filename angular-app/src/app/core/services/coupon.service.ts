import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from '../../../environments/environment';
import { Coupon, CouponFormData } from '../models/coupon.model';

@Injectable({
    providedIn: 'root'
})
export class CouponService {
    private apiUrl = `${environment.apiUrl}/coupons/`;

    constructor(private http: HttpClient) { }

    getCoupons(): Observable<Coupon[]> {
        return this.http.get<Coupon[]>(this.apiUrl);
    }

    createCoupon(data: CouponFormData): Observable<any> {
        return this.http.post(this.apiUrl, data);
    }

    updateCoupon(code: string, data: CouponFormData, advertiserId?: number): Observable<any> {
        let url = `${this.apiUrl}${code}/`;
        if (advertiserId) {
            url += `?advertiser_id=${advertiserId}`;
        }
        return this.http.patch(url, data);
    }

    getAdvertisers(): Observable<any[]> {
        return this.http.get<any[]>(`${environment.apiUrl}/advertisers/`);
    }

    getPartners(): Observable<any[]> {
        return this.http.get<any[]>(`${environment.apiUrl}/partners/`);
    }

    getCouponHistory(code: string): Observable<any[]> {
        return this.http.get<any[]>(`${this.apiUrl}${code}/history/`);
    }
}
