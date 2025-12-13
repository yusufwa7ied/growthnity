import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms'; // Force rebuild
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
    dateRange: Date[] | null = null;
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

        const dateFrom = (this.dateRange && this.dateRange[0]) ? this.formatDate(this.dateRange[0]) : undefined;
        const dateTo = (this.dateRange && this.dateRange[1]) ? this.formatDate(this.dateRange[1]) : undefined;
        const advertiserId = this.selectedAdvertiser?.id;
        const search = this.searchTerm ? this.searchTerm.toLowerCase() : undefined;

        this.partnerService.getMyCoupons(dateFrom, dateTo, advertiserId, search, this.currentPage).subscribe({
            next: (response) => {
                console.log('=== API RESPONSE ===');
                console.log('Count:', response.count);
                console.log('First item:', response.results[0]);
                console.log('First item structure:', JSON.stringify(response.results[0], null, 2));
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
        // Count total coupons across all campaigns
        this.totalCoupons = this.coupons.reduce((sum, campaign) => sum + campaign.coupon_count, 0);

        // Sum up metrics from all coupons in all campaigns
        let orders = 0;
        let sales = 0;
        let grossPayout = 0;
        let netPayout = 0;
        let hasNetPayout = false;

        this.coupons.forEach(campaign => {
            campaign.coupons.forEach(coupon => {
                orders += coupon.orders;
                sales += coupon.sales;
                grossPayout += coupon.gross_payout;
                if (coupon.net_payout !== null && coupon.net_payout !== undefined) {
                    netPayout += coupon.net_payout;
                    hasNetPayout = true;
                }
            });
        });

        this.totalOrders = orders;
        this.totalSales = sales;
        this.totalGrossPayout = grossPayout;
        this.totalNetPayout = hasNetPayout ? netPayout : null;
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

    onDateRangeChange() {
        if (this.dateRange && this.dateRange[0] && this.dateRange[1]) {
            this.applyFilters();
        } else if (!this.dateRange || (this.dateRange.length === 0)) {
            this.applyFilters();
        }
    }

    clearFilters() {
        this.dateRange = null;
        this.selectedAdvertiser = null;
        this.searchTerm = '';
        this.currentPage = 1;
        this.loadCoupons();
    }

    clearSearch() {
        this.searchTerm = '';
        this.applyFilters();
    }

    exportData() {
        const dateFrom = (this.dateRange && this.dateRange[0]) ? this.formatDate(this.dateRange[0]) : '';
        const dateTo = (this.dateRange && this.dateRange[1]) ? this.formatDate(this.dateRange[1]) : '';
        const advertiserId = this.selectedAdvertiser?.id;
        const search = this.searchTerm ? this.searchTerm.toLowerCase() : undefined;

        this.partnerService.exportMyCoupons(dateFrom, dateTo, advertiserId, search).subscribe({
            next: (blob) => {
                // Create download link
                const url = window.URL.createObjectURL(blob);
                const link = document.createElement('a');
                link.href = url;
                link.download = `my_coupons_${new Date().getTime()}.csv`;
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
                window.URL.revokeObjectURL(url);
            },
            error: (error) => {
                console.error('Error exporting data:', error);
                alert('Failed to export data. Please try again.');
            }
        });
    }

    private formatDate(date: Date): string {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    }
}
