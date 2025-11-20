import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from '../../../environments/environment';
import { DepartmentTarget } from '../models/targets.model';

@Injectable({
  providedIn: 'root'
})
export class TargetsService {
  private apiUrl = `${environment.apiUrl}/targets/`;

  constructor(private http: HttpClient) { }

  getTargets(filters?: { advertiser_id?: number; partner_type?: string; month?: string }): Observable<DepartmentTarget[]> {
    let params = new HttpParams();
    if (filters?.advertiser_id) params = params.set('advertiser_id', filters.advertiser_id.toString());
    if (filters?.partner_type) params = params.set('partner_type', filters.partner_type);
    if (filters?.month) params = params.set('month', filters.month);

    return this.http.get<DepartmentTarget[]>(this.apiUrl, { params });
  }

  getTarget(id: number): Observable<DepartmentTarget> {
    return this.http.get<DepartmentTarget>(`${this.apiUrl}${id}/`);
  }

  createTarget(target: DepartmentTarget): Observable<DepartmentTarget> {
    return this.http.post<DepartmentTarget>(this.apiUrl, target);
  }

  updateTarget(id: number, target: Partial<DepartmentTarget>): Observable<DepartmentTarget> {
    return this.http.put<DepartmentTarget>(`${this.apiUrl}${id}/`, target);
  }

  deleteTarget(id: number): Observable<any> {
    return this.http.delete(`${this.apiUrl}${id}/`);
  }
}
