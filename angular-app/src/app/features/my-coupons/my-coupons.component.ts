import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Button } from 'primeng/button';
import { Card } from 'primeng/card';
import { DatePicker } from 'primeng/datepicker';
import { InputText } from 'primeng/inputtext';
import { PaginatorModule } from 'primeng/paginator';
import { Select } from 'primeng/select';
import { TableModule } from 'primeng/table';
import { Advertiser } from '../../core/models/dashboard.model';
import { DashboardService } from '../../core/services/dashboard.service';
import { CouponPerformance, PartnerService } from '../../core/services/partner.service';
import { FooterComponent } from '../../shared/components/footer/footer.component';
import { MainHeaderComponent } from '../../shared/components/main-header/main-header.component';
import { TrimDecimalsPipe } from '../../shared/pipes/trim-decimals.pipe';

@Component({
    selector: 'app-my-coupons',
    standalone: true,
    imports: [
        CommonModule,
        FormsModule,
        Card,
        Select,
        DatePicker,
        InputText,
        TableModule,
        PaginatorModule,
        Button,
        MainHeaderComponent,
        FooterComponent,
        TrimDecimalsPipe
    ],
    templateUrl: './my-coupons.component.html',
    styleUrl: './my-coupons.component.css'
})
export class MyCouponsComponent implements OnInit {
    coupons: CouponPerformance[] = [];
    advertisers: Advertiser[] = [];
    loading = false;

    // Filters
    dateFrom: Date | null = null;
    dateTo: Date | null = null;
    selectedAdvertiser: Advertiser | null = null;
    searchTerm = '';
    searchTimeout: any;

    // Pagination
    currentPage = 1;
    totalRecords = 0;
    rowsPerPage = 20;

    // Summary
    totalCoupons = 0;
    totalOrders = 0;
    totalSales = 0;
    totalGrossPayout = 0;
    totalNetPayout = 0;

    constructor(
        private partnerService: PartnerService,
        private dashboardService: DashboardService
    ) { }

    ngOnInit() {
        this.loadAdvertisers();
        this.loadCoupons();
    }

    loadAdvertisers() {
        this.dashboardService.getAdvertisers().subscribe({
            next: (advertisers) => {
                this.advertisers = advertisers;
            },
            error: (error) => {
                console.error('Error loading advertisers:', error);
            }
        });
    }

    loadCoupons() {
        this.loading = true;

        const dateFrom = this.dateFrom ? this.formatDate(this.dateFrom) : undefined;
        const dateTo = this.dateTo ? this.formatDate(this.dateTo) : undefined;
        const advertiserId = this.selectedAdvertiser?.id;
        const search = this.searchTerm || undefined;

        this.partnerService.getMyCoupons(dateFrom, dateTo, advertiserId, search, this.currentPage).subscribe({
            next: (response) => {
                this.coupons = response.results;
                this.totalRecords = response.count;
                this.calculateSummary();
                this.loading = false;
            },
            error: (error) => {
                console.error('Error loading coupons:', error);
                this.loading = false;
            }
        });
    }

    calculateSummary() {
        this.totalCoupons = this.coupons.length;
        this.totalOrders = this.coupons.reduce((sum, c) => sum + c.orders, 0);
        this.totalSales = this.coupons.reduce((sum, c) => sum + c.sales, 0);
        this.totalGrossPayout = this.coupons.reduce((sum, c) => sum + c.gross_payout, 0);
        this.totalNetPayout = this.coupons.reduce((sum, c) => sum + c.net_payout, 0);
    }

    onPageChange(event: any) {
        this.currentPage = event.page + 1;
        this.rowsPerPage = event.rows;
        this.loadCoupons();
    }

    onSearchChange() {
        // Debounce search to avoid too many API calls
        if (this.searchTimeout) {
            clearTimeout(this.searchTimeout);
        }
        this.searchTimeout = setTimeout(() => {
            this.applyFilters();
        }, 500);
    }

    applyFilters() {
        this.currentPage = 1;
        this.loadCoupons();
    }

    clearFilters() {
        this.dateFrom = null;
        this.dateTo = null;
        this.selectedAdvertiser = null;
        this.searchTerm = '';
        this.currentPage = 1;
        this.loadCoupons();
    }

    exportData() {
        const dateFrom = this.dateFrom ? this.formatDate(this.dateFrom) : '';
        const dateTo = this.dateTo ? this.formatDate(this.dateTo) : '';
        const advertiserId = this.selectedAdvertiser?.id;

        let url = `${this.partnerService['apiUrl']}/partner/my-coupons/?export=csv`;
        if (dateFrom) url += `&date_from=${dateFrom}`;
        if (dateTo) url += `&date_to=${dateTo}`;
        if (advertiserId) url += `&advertiser_id=${advertiserId}`;
        if (this.searchTerm) url += `&search=${this.searchTerm}`;

        window.open(url, '_blank');
    }

    private formatDate(date: Date): string {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    }
}
