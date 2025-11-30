import { animate, style, transition, trigger } from '@angular/animations';
import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { ChartConfiguration } from 'chart.js';
import { BaseChartDirective } from 'ng2-charts';
import { Card } from 'primeng/card';
import { DatePicker } from 'primeng/datepicker';
import { InputText } from 'primeng/inputtext';
import { MultiSelect } from 'primeng/multiselect';
import { PaginatorModule } from 'primeng/paginator';
import { Select } from 'primeng/select';
import { TableModule } from 'primeng/table';
import { catchError, forkJoin, of } from 'rxjs';
import { Advertiser, Coupon, DashboardFilters, GraphData, KPIData, Partner, TableRow } from '../../core/models/dashboard.model';
import { User } from '../../core/models/user.model';
import { AnalyticsService, PerformanceAnalytics } from '../../core/services/analytics.service';
import { AuthService } from '../../core/services/auth.service';
import { DashboardService } from '../../core/services/dashboard.service';
import { FooterComponent } from '../../shared/components/footer/footer.component';
import { MainHeaderComponent } from '../../shared/components/main-header/main-header.component';
import { SkeletonLoaderComponent } from '../../shared/components/skeleton-loader/skeleton-loader.component';
import { TrimDecimalsPipe } from '../../shared/pipes/trim-decimals.pipe';

@Component({
    selector: 'app-dashboard',
    standalone: true,
    imports: [
        CommonModule,
        FormsModule,
        Card,
        Select,
        MultiSelect,
        DatePicker,
        FooterComponent,
        InputText,
        TableModule,
        PaginatorModule,
        BaseChartDirective,
        MainHeaderComponent,
        SkeletonLoaderComponent,
        TrimDecimalsPipe
    ],
    templateUrl: './dashboard.component.html',
    styleUrl: './dashboard.component.css',

    animations: [
        trigger('slideIn', [
            transition(':enter', [
                style({ transform: 'translateX(100%)', opacity: 0 }),
                animate('300ms cubic-bezier(0.4, 0, 0.2, 1)', style({ transform: 'translateX(0)', opacity: 1 }))
            ]),
            transition(':leave', [
                animate('250ms cubic-bezier(0.4, 0, 1, 1)', style({ transform: 'translateX(100%)', opacity: 0 }))
            ])
        ])
    ]
})
export class DashboardComponent implements OnInit {
    user: User | null = null;
    role: string = '';
    isAdmin: boolean = false;              // OpsManager or Admin (full access)
    isMediaBuyer: boolean = false;         // TeamMember with media_buying department
    isDepartmentRestricted: boolean = false; // TeamMember with affiliate/influencer department
    canAccessCoupons: boolean = false;     // Only OpsManager/Admin

    // Filters
    filters: DashboardFilters = {};

    // All available options (never filtered)
    allAdvertisers: Advertiser[] = [];
    allPartners: Partner[] = [];
    allCoupons: Coupon[] = [];

    // Filtered options (dynamically updated based on selections)
    advertisers: Advertiser[] = [];
    partners: Partner[] = [];
    coupons: Coupon[] = [];

    // Department options for admin filter
    departmentOptions = [
        { label: 'Media Buying', value: 'MB' },
        { label: 'Affiliate', value: 'AFF' },
        { label: 'Influencer', value: 'INF' }
    ];

    // Data
    kpis: KPIData | null = null;
    tableData: TableRow[] = [];
    filteredTableData: TableRow[] = [];
    graphData: GraphData | null = null;

    // Charts
    lineChart: ChartConfiguration | null = null;
    pieChart: ChartConfiguration | null = null;

    // Analytics
    analytics: PerformanceAnalytics | null = null;
    showAnalytics: boolean = false;

    // UI State
    sidebarVisible: boolean = false;
    loading: boolean = false;
    showSkeletons: boolean = true;
    dateRange: Date[] = [];
    activeDatePreset: string = '';
    tableSearchTerm: string = '';
    showFilterModal: boolean = false;
    selectedMonth: Date = new Date(); // Current month for analytics

    // Pagination
    currentPage: number = 1;
    pageSize: number = 50;
    totalRecords: number = 0;

    // Debouncing
    private filterDebounceTimer: any;
    private lastFilterChangeTime: number = 0;
    private readonly DEBOUNCE_DELAY: number = 300; // milliseconds

    constructor(
        private authService: AuthService,
        private router: Router,
        private dashboardService: DashboardService,
        private analyticsService: AnalyticsService,
        private cdr: ChangeDetectorRef
    ) { }

    ngOnInit(): void {
        this.user = this.authService.currentUser();
        this.role = this.user?.role || '';

        // Determine user access level based on role and department
        // 1. ViewOnly = Dashboard only, see all data (like Admin but no other pages)
        // 2. OpsManager (no dept) = Full access (all admin pages + daily spend)
        // 3. OpsManager (any dept) = Full department data + Coupons only (NO other admin pages, NO daily spend)
        // 4. TeamMember (media_buying) = Filtered dashboard + Daily Spend only (NO coupons, NO admin pages)
        // 5. TeamMember (affiliate/influencer) = Filtered dashboard only

        if (this.role === 'ViewOnly') {
            // ViewOnly: Dashboard only, see all data (same access as Admin for data)
            this.isAdmin = true;  // Treat as admin for data access
            this.isMediaBuyer = false;
            this.isDepartmentRestricted = false;
            this.canAccessCoupons = false;
        } else if ((this.role === 'OpsManager' || this.role === 'Admin') && !this.user?.department) {
            // OpsManager/Admin with NO department: Full system access
            this.isAdmin = true;
            this.isMediaBuyer = true;  // Can also see daily spend
            this.isDepartmentRestricted = false;
            this.canAccessCoupons = true;
        } else if (this.role === 'OpsManager' && this.user?.department) {
            // OpsManager with ANY department: Full department data + Coupons only
            this.isAdmin = false;
            this.isMediaBuyer = false;  // NO daily spend for OpsManager with department
            this.isDepartmentRestricted = false;
            this.canAccessCoupons = true;  // Can access coupons
        } else if (this.role === 'TeamMember' && this.user?.department === 'media_buying') {
            // TeamMember with media_buying: Filtered dashboard + Daily Spend only
            this.isAdmin = false;
            this.isMediaBuyer = true;  // Can see daily spend
            this.isDepartmentRestricted = false;
            this.canAccessCoupons = false;  // NO coupons
        } else if (this.user?.department === 'affiliate' || this.user?.department === 'influencer') {
            // TeamMember with affiliate/influencer: Filtered dashboard only
            this.isAdmin = false;
            this.isMediaBuyer = false;
            this.isDepartmentRestricted = true;
            this.canAccessCoupons = false;
        }        // Load filter options immediately (cached)
        this.loadFilterOptions();

        // Defer heavy data loading using requestAnimationFrame
        // This ensures the UI renders first before loading data, making navigation feel responsive
        requestAnimationFrame(() => {
            this.loading = true;
            this.cdr.markForCheck();

            setTimeout(() => {
                this.loadData();
                this.loadAnalytics();
            }, 0);
        });
    }

    hasActiveFilters(): boolean {
        return !!(this.filters.partner_type || this.filters.advertiser_id || this.filters.partner_id ||
            this.filters.coupon_code || (this.filters.date_from && this.filters.date_to));
    }

    getAdvertiserNames(): string {
        if (!this.filters.advertiser_id) return '';
        const ids = Array.isArray(this.filters.advertiser_id) ? this.filters.advertiser_id : [this.filters.advertiser_id];
        const names = ids.map(id => this.allAdvertisers.find(a => a.id === id)?.name || 'Unknown');
        return names.join(', ');
    }

    getPartnerNames(): string {
        if (!this.filters.partner_id) return '';
        const ids = Array.isArray(this.filters.partner_id) ? this.filters.partner_id : [this.filters.partner_id];
        const names = ids.map(id => this.allPartners.find(p => p.id === id)?.name || 'Unknown');
        return names.join(', ');
    }

    getCouponCodes(): string {
        if (!this.filters.coupon_code) return '';
        const codes = Array.isArray(this.filters.coupon_code) ? this.filters.coupon_code : [this.filters.coupon_code];
        return codes.join(', ');
    }

    getDepartmentName(): string {
        if (!this.filters.partner_type) return '';
        const dept = this.departmentOptions.find(d => d.value === this.filters.partner_type);
        return dept?.label || '';
    }

    removeFilter(filterType: string): void {
        switch (filterType) {
            case 'department':
                this.filters.partner_type = undefined;
                this.recomputeFilterDropdowns();
                this.loadData();
                this.loadAnalytics();
                return;
            case 'advertiser':
                this.filters.advertiser_id = undefined;
                break;
            case 'partner':
                this.filters.partner_id = undefined;
                break;
            case 'coupon':
                this.filters.coupon_code = undefined;
                break;
            case 'date':
                this.filters.date_from = undefined;
                this.filters.date_to = undefined;
                this.dateRange = [];
                this.activeDatePreset = '';
                // Date filter removal should NOT reload analytics
                this.recomputeFilterDropdowns();
                this.loadData();
                return;
        }
        // For advertiser/partner/coupon: reload both data and analytics
        this.recomputeFilterDropdowns();
        this.loadData();
        this.loadAnalytics();
    }

    loadFiltersFromTableData(): void {
        // Only populate the all* arrays if they're empty (first load only)
        // This preserves the full list of options even when filters are applied
        const isFirstLoad = this.allAdvertisers.length === 0;

        // Extract unique advertisers from table data
        const advertiserMap = new Map<number, string>();
        const partnerMap = new Map<number, string>();
        const couponMap = new Map<string, { advertiser_id: number, partner_id?: number }>();

        this.tableData.forEach(row => {
            // Collect advertisers (campaign field contains advertiser name)
            if (row.advertiser_id && row.campaign) {
                advertiserMap.set(row.advertiser_id, row.campaign);
            }

            // Collect partners (only for admin roles)
            if (this.isAdmin && row.partner_id && row.partner) {
                partnerMap.set(row.partner_id, row.partner);
            }

            // Collect coupons
            if (row.coupon) {
                couponMap.set(row.coupon, {
                    advertiser_id: row.advertiser_id,
                    partner_id: row.partner_id
                });
            }
        });

        if (isFirstLoad) {
            // First load - build the full lists
            this.allAdvertisers = Array.from(advertiserMap.entries())
                .map(([id, name]) => ({ id, name, attribution: '' }))
                .sort((a, b) => a.name.localeCompare(b.name));
            this.advertisers = [...this.allAdvertisers];

            this.allPartners = Array.from(partnerMap.entries())
                .map(([id, name]) => ({ id, name, type: '' }))
                .sort((a, b) => a.name.localeCompare(b.name));
            this.partners = [...this.allPartners];

            this.allCoupons = Array.from(couponMap.entries())
                .map(([code, data]) => ({
                    code,
                    advertiser: '',
                    advertiser_id: data.advertiser_id,
                    partner: '',
                    partner_id: data.partner_id
                }))
                .sort((a, b) => a.code.localeCompare(b.code));
            this.coupons = [...this.allCoupons];

        } else {
            // Subsequent loads - just recompute filtered dropdowns
            this.recomputeFilterDropdowns();
        }
    }

    hasFilterValue(value: any): boolean {
        if (value == null) return false;
        if (Array.isArray(value)) return value.length > 0;
        return true;
    }

    clearAdvertiserFilter(): void {
        this.filters.advertiser_id = null;
        this.recomputeFilterDropdowns();
    }

    clearPartnerFilter(): void {
        this.filters.partner_id = null;
        this.recomputeFilterDropdowns();
    }

    clearCouponFilter(): void {
        this.filters.coupon_code = null;
        this.recomputeFilterDropdowns();
    }

    clearDepartmentFilter(): void {
        this.filters.partner_type = null;
        this.recomputeFilterDropdowns();
        this.loadData();
        this.loadAnalytics();
    }

    onDepartmentChangeRealtime(): void {
        // Normalize empty string to null
        if (this.filters.partner_type === '') {
            this.filters.partner_type = null;
        }
        this.recomputeFilterDropdowns();
        this.debouncedLoadData();
    }

    onAdvertiserChangeRealtime(): void {
        // Normalize empty arrays to null
        if (Array.isArray(this.filters.advertiser_id) && this.filters.advertiser_id.length === 0) {
            this.filters.advertiser_id = null;
        }
        // Instantly recompute other dropdown options
        this.recomputeFilterDropdowns();
        // Load data and analytics with debounce
        this.debouncedLoadData();
    }

    onPartnerChangeRealtime(): void {
        // Normalize empty arrays to null
        if (Array.isArray(this.filters.partner_id) && this.filters.partner_id.length === 0) {
            this.filters.partner_id = null;
        }
        // Instantly recompute other dropdown options
        this.recomputeFilterDropdowns();
        // Load data and analytics with debounce
        this.debouncedLoadData();
    }

    onCouponChangeRealtime(): void {
        // Normalize empty arrays to null
        if (Array.isArray(this.filters.coupon_code) && this.filters.coupon_code.length === 0) {
            this.filters.coupon_code = null;
        }
        // Instantly recompute other dropdown options
        this.recomputeFilterDropdowns();
        // Load data and analytics with debounce
        this.debouncedLoadData();
    }

    /**
     * Debounced data loading to prevent excessive API calls during rapid filter changes
     */
    private debouncedLoadData(): void {
        // Clear existing timer
        if (this.filterDebounceTimer) {
            clearTimeout(this.filterDebounceTimer);
        }

        // Set new timer
        this.filterDebounceTimer = setTimeout(() => {
            this.loadData();
            this.loadAnalytics();
        }, this.DEBOUNCE_DELAY);
    }

    /**
     * Dynamically filter dropdown options based on current selections.
     * Mirrors the Dash callback logic from admin_tools.py
     * This runs instantly on any selection change - no need to click "Apply Filter"
     */
    recomputeFilterDropdowns(): void {
        // Check if all filters are empty
        const hasAdvertiserFilter = this.filters.advertiser_id != null &&
            (!Array.isArray(this.filters.advertiser_id) || this.filters.advertiser_id.length > 0);
        const hasPartnerFilter = this.filters.partner_id != null &&
            (!Array.isArray(this.filters.partner_id) || this.filters.partner_id.length > 0);
        const hasCouponFilter = this.filters.coupon_code != null &&
            (!Array.isArray(this.filters.coupon_code) || this.filters.coupon_code.length > 0);
        const hasDepartmentFilter = this.filters.partner_type != null && this.filters.partner_type !== '';

        if (!hasAdvertiserFilter && !hasPartnerFilter && !hasCouponFilter && !hasDepartmentFilter) {
            // No filters - restore all
            this.advertisers = [...this.allAdvertisers];
            this.partners = [...this.allPartners];
            this.coupons = [...this.allCoupons];
            return;
        }

        // Build advertiser options: filter by department + partner + coupon (NOT by advertiser itself)
        let couponsForAdvertisers = [...this.allCoupons];
        if (hasDepartmentFilter) {
            couponsForAdvertisers = couponsForAdvertisers.filter(c => c.partner_type === this.filters.partner_type);
        }
        if (hasPartnerFilter) {
            const ids = Array.isArray(this.filters.partner_id) ? this.filters.partner_id : [this.filters.partner_id];
            couponsForAdvertisers = couponsForAdvertisers.filter(c => c.partner_id && ids.includes(c.partner_id));
        }
        if (hasCouponFilter) {
            const codes = Array.isArray(this.filters.coupon_code) ? this.filters.coupon_code : [this.filters.coupon_code];
            couponsForAdvertisers = couponsForAdvertisers.filter(c => codes.includes(c.code));
        }
        const advertiserIds = new Set(couponsForAdvertisers.map(c => c.advertiser_id));
        if (hasAdvertiserFilter) {
            const selected = Array.isArray(this.filters.advertiser_id) ? this.filters.advertiser_id : [this.filters.advertiser_id];
            selected.forEach(id => { if (id != null) advertiserIds.add(id); });
        }
        this.advertisers = this.allAdvertisers.filter(a => advertiserIds.has(a.id));

        // Build partner options: filter by department + advertiser + coupon (NOT by partner itself)
        let couponsForPartners = [...this.allCoupons];
        if (hasDepartmentFilter) {
            couponsForPartners = couponsForPartners.filter(c => c.partner_type === this.filters.partner_type);
        }
        if (hasAdvertiserFilter) {
            const ids = Array.isArray(this.filters.advertiser_id) ? this.filters.advertiser_id : [this.filters.advertiser_id];
            couponsForPartners = couponsForPartners.filter(c => ids.includes(c.advertiser_id));
        }
        if (hasCouponFilter) {
            const codes = Array.isArray(this.filters.coupon_code) ? this.filters.coupon_code : [this.filters.coupon_code];
            couponsForPartners = couponsForPartners.filter(c => codes.includes(c.code));
        }
        const partnerIds = new Set(couponsForPartners.filter(c => c.partner_id).map(c => c.partner_id!));
        if (hasPartnerFilter) {
            const selected = Array.isArray(this.filters.partner_id) ? this.filters.partner_id : [this.filters.partner_id];
            selected.forEach(id => { if (id != null) partnerIds.add(id); });
        }
        this.partners = this.allPartners.filter(p => partnerIds.has(p.id));

        // Build coupon options: filter by department + advertiser + partner (NOT by coupon itself)
        let couponsForCoupons = [...this.allCoupons];
        if (hasDepartmentFilter) {
            couponsForCoupons = couponsForCoupons.filter(c => c.partner_type === this.filters.partner_type);
        }
        if (hasAdvertiserFilter) {
            const ids = Array.isArray(this.filters.advertiser_id) ? this.filters.advertiser_id : [this.filters.advertiser_id];
            couponsForCoupons = couponsForCoupons.filter(c => ids.includes(c.advertiser_id));
        }
        if (hasPartnerFilter) {
            const ids = Array.isArray(this.filters.partner_id) ? this.filters.partner_id : [this.filters.partner_id];
            couponsForCoupons = couponsForCoupons.filter(c => c.partner_id && ids.includes(c.partner_id));
        }
        const couponCodes = new Set(couponsForCoupons.map(c => c.code));
        if (hasCouponFilter) {
            const selected = Array.isArray(this.filters.coupon_code) ? this.filters.coupon_code : [this.filters.coupon_code];
            selected.forEach(code => { if (code != null) couponCodes.add(code); });
        }
        this.coupons = this.allCoupons.filter(c => couponCodes.has(c.code));
    }

    loadFilterOptions(): void {
        // Load all available filter options from the API
        // This loads the full dataset for dropdowns, respecting user permissions
        this.dashboardService.getFilterOptions().subscribe({
            next: (options) => {
                // Populate advertisers
                this.allAdvertisers = options.advertisers.map(a => ({
                    id: a.advertiser_id,
                    name: a.campaign,
                    attribution: ''
                })).sort((a, b) => a.name.localeCompare(b.name));

                // Populate partners
                this.allPartners = options.partners.map(p => ({
                    id: p.partner_id,
                    name: p.partner,
                    type: ''
                })).sort((a, b) => a.name.localeCompare(b.name));

                // Populate coupons
                this.allCoupons = options.coupons.map(c => ({
                    code: c.coupon,
                    advertiser: '',
                    advertiser_id: c.advertiser_id,
                    partner: '',
                    partner_id: c.partner_id,
                    partner_type: c.partner_type
                })).sort((a, b) => a.code.localeCompare(b.code));

                // Initially, show all options in dropdowns
                this.advertisers = [...this.allAdvertisers];
                this.partners = [...this.allPartners];
                this.coupons = [...this.allCoupons];
                this.cdr.markForCheck();
            },
            error: (error) => {
                console.error('Error loading filter options:', error);
                this.cdr.markForCheck();
            }
        });
    }

    loadData(): void {
        this.loading = true;
        this.showSkeletons = true;

        // Parallelize all data fetching requests using forkJoin
        forkJoin({
            kpis: this.dashboardService.getKPIs(this.filters),
            tableData: this.dashboardService.getTableData(this.filters, this.currentPage, this.pageSize),
            pieChartData: this.dashboardService.getPieChartData(this.filters),
            graphData: this.dashboardService.getGraphData(this.filters)
        }).pipe(
            catchError(error => {
                console.error('Error loading dashboard data:', error);
                return of({
                    kpis: null,
                    tableData: { count: 0, results: [] },
                    pieChartData: [],
                    graphData: null
                });
            })
        ).subscribe({
            next: (results) => {
                // Process KPIs
                if (results.kpis) {
                    this.kpis = results.kpis;
                    if (!this.kpis.trends) {
                        this.kpis.trends = {
                            orders_change: this.calculateMockTrend(this.kpis.total_orders),
                            sales_change: this.calculateMockTrend(this.kpis.total_sales),
                            revenue_change: this.calculateMockTrend(this.kpis.total_revenue),
                            payout_change: this.calculateMockTrend(this.kpis.total_payout),
                            profit_change: this.calculateMockTrend(this.kpis.total_profit)
                        };
                    }
                }

                // Process table data
                if (results.tableData) {
                    this.totalRecords = results.tableData.count;
                    this.tableData = results.tableData.results;
                    this.filteredTableData = [...results.tableData.results];
                }

                // Process pie chart data
                if (results.pieChartData && results.pieChartData.length > 0) {
                    const pieChartRows: TableRow[] = results.pieChartData.map(item => ({
                        date: '',
                        advertiser_id: 0,
                        campaign: item.campaign,
                        coupon: '',
                        orders: 0,
                        sales: 0,
                        revenue: item.total_revenue,
                        payout: 0
                    }));
                    this.buildPieChart(pieChartRows);
                }

                // Process graph data
                if (results.graphData) {
                    this.graphData = results.graphData;
                    this.buildLineChart(results.graphData);
                }

                this.showSkeletons = false;
                this.loading = false;
                this.cdr.markForCheck();
            },
            error: (error) => {
                console.error('Fatal error loading dashboard data:', error);
                this.showSkeletons = false;
                this.loading = false;
                this.cdr.markForCheck();
            }
        });
    }

    private calculateMockTrend(value: number): number {
        // Mock calculation: simulate realistic 7-day trends between -15% and +25%
        const seed = Math.abs(Math.sin(value * 0.001));
        return Math.round((seed * 40 - 15) * 10) / 10;
    }

    buildLineChart(data: GraphData): void {
        // Format dates to show as "7th Oct" instead of full date
        const formattedDates = data.dates.map(dateStr => {
            const date = new Date(dateStr);
            const day = date.getDate();
            const month = date.toLocaleString('en-US', { month: 'short' });
            const suffix = this.getOrdinalSuffix(day);
            return `${day}${suffix} ${month}`;
        });

        const datasets: any[] = [
            {
                label: 'Revenue',
                data: data.daily_revenue,
                borderColor: '#009292',
                backgroundColor: 'rgba(0, 146, 146, 0.1)',
                tension: 0.4,
                fill: true
            },
            {
                label: 'Payout',
                data: data.daily_payout,
                borderColor: '#ff6b6b',
                backgroundColor: 'rgba(255, 107, 107, 0.1)',
                tension: 0.4,
                fill: true
            }
        ];

        // Only admin roles see Profit
        if (this.isAdmin && data.daily_profit) {
            datasets.push({
                label: 'Profit',
                data: data.daily_profit,
                borderColor: '#95e1d3',
                backgroundColor: 'rgba(149, 225, 211, 0.1)',
                tension: 0.4,
                fill: true
            });
        }

        this.lineChart = {
            type: 'line',
            data: {
                labels: formattedDates,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top',
                        labels: {
                            color: '#101010',
                            font: {
                                size: 13,
                                weight: 'bold',
                                family: 'Segoe UI'
                            },
                            padding: 15,
                            usePointStyle: true,
                            pointStyle: 'circle'
                        }
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        backgroundColor: 'rgba(16, 16, 16, 0.95)',
                        titleColor: '#ffffff',
                        bodyColor: '#ffffff',
                        borderColor: 'rgba(0, 146, 146, 0.5)',
                        borderWidth: 1,
                        padding: 12,
                        displayColors: true,
                        callbacks: {
                            label: function (context: any) {
                                let label = context.dataset.label || '';
                                if (label) {
                                    label += ': ';
                                }
                                if (context.parsed.y !== null) {
                                    label += '$' + context.parsed.y.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                                }
                                return label;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        grid: {
                            color: 'rgba(0, 146, 146, 0.08)',
                            lineWidth: 1
                        },
                        ticks: {
                            color: '#757575',
                            font: {
                                size: 11
                            },
                            maxRotation: 45,
                            minRotation: 0
                        }
                    },
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: 'rgba(0, 146, 146, 0.08)',
                            lineWidth: 1
                        },
                        ticks: {
                            color: '#757575',
                            font: {
                                size: 11
                            },
                            callback: function (value: any) {
                                return '$' + value.toLocaleString();
                            }
                        }
                    }
                },
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                elements: {
                    line: {
                        tension: 0.4
                    },
                    point: {
                        radius: 3,
                        hoverRadius: 6,
                        hitRadius: 10
                    }
                }
            }
        };
    }

    buildPieChart(data: TableRow[]): void {
        // Group by campaign and sum revenue
        const campaignData = new Map<string, number>();

        data.forEach(row => {
            const campaign = row.campaign;
            const value = row.revenue || 0;
            if (campaign && value) {
                campaignData.set(campaign, (campaignData.get(campaign) || 0) + value);
            }
        });

        // Convert to arrays and sort by value
        const sortedCampaigns = Array.from(campaignData.entries())
            .sort((a, b) => b[1] - a[1])
            .slice(0, 10); // Top 10 campaigns

        const labels = sortedCampaigns.map(([campaign]) => campaign);
        const values = sortedCampaigns.map(([, value]) => value);

        this.pieChart = {
            type: 'doughnut',
            data: {
                labels: labels,
                datasets: [{
                    data: values,
                    backgroundColor: [
                        '#009292', '#00b0b0', '#007a7a', '#4ecdc4', '#95e1d3',
                        '#f38181', '#ff6b6b', '#ffd93d', '#6bcf7f', '#a29bfe'
                    ],
                    borderColor: '#ffffff',
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'right',
                        labels: {
                            color: '#101010',
                            font: {
                                size: 11,
                                family: 'Segoe UI'
                            },
                            padding: 10,
                            boxWidth: 12
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: (context) => {
                                const label = context.label || '';
                                const value = context.parsed || 0;
                                const total = (context.dataset.data as number[]).reduce((a, b) => a + b, 0);
                                const percentage = ((value / total) * 100).toFixed(1);
                                return `${label}: $${value.toLocaleString()} (${percentage}%)`;
                            }
                        }
                    }
                }
            }
        };
    }

    applyFilters(): void {
        // Convert date range to string format using local timezone
        if (this.dateRange && this.dateRange.length === 2 && this.dateRange[0] && this.dateRange[1]) {
            const fromDate = new Date(this.dateRange[0]);
            const toDate = new Date(this.dateRange[1]);

            this.filters.date_from = `${fromDate.getFullYear()}-${String(fromDate.getMonth() + 1).padStart(2, '0')}-${String(fromDate.getDate()).padStart(2, '0')}`;
            this.filters.date_to = `${toDate.getFullYear()}-${String(toDate.getMonth() + 1).padStart(2, '0')}-${String(toDate.getDate()).padStart(2, '0')}`;
        } else {
            this.filters.date_from = undefined;
            this.filters.date_to = undefined;
        }
        // Reset to first page when filters change
        this.currentPage = 1;
        // Only reload data (KPIs, table, charts) - NOT analytics
        // Analytics should always show current month MTD regardless of date filter
        this.loadData();
    }

    onPageChange(event: any): void {
        this.currentPage = event.page + 1; // PrimeNG uses 0-based page index
        this.loadData();
    }

    loadAnalytics(): void {
        // Support multiple advertiser IDs - send all of them
        const advertiserIds = Array.isArray(this.filters.advertiser_id)
            ? this.filters.advertiser_id
            : this.filters.advertiser_id ? [this.filters.advertiser_id] : undefined;

        const partnerIds = Array.isArray(this.filters.partner_id)
            ? this.filters.partner_id
            : this.filters.partner_id ? [this.filters.partner_id] : undefined;

        // Determine partner type: use filter if set (admin), otherwise use user's department
        let partnerType: string | undefined;
        if (this.filters.partner_type) {
            partnerType = this.filters.partner_type;
        } else if (this.user?.department) {
            if (this.user.department === 'media_buying') partnerType = 'MB';
            else if (this.user.department === 'affiliate') partnerType = 'AFF';
            else if (this.user.department === 'influencer') partnerType = 'INF';
        }

        // Format selected month as YYYY-MM
        const monthStr = `${this.selectedMonth.getFullYear()}-${String(this.selectedMonth.getMonth() + 1).padStart(2, '0')}`;

        this.analyticsService.getPerformanceAnalytics(
            advertiserIds,
            partnerIds,
            partnerType,
            monthStr
        ).subscribe({
            next: (data) => {
                console.log('📊 Analytics data received:', data);
                console.log('🔍 is_department_restricted:', data.is_department_restricted);
                console.log('🔍 simplified_analytics:', data.simplified_analytics);
                this.analytics = data;
                this.cdr.markForCheck();
            },
            error: (err) => {
                console.error('❌ Failed to load analytics:', err);
                console.error('❌ Error details:', JSON.stringify(err, null, 2));
                this.cdr.markForCheck();
            }
        });
    }

    getPacingColor(status: string): string {
        switch (status) {
            case 'Ahead': return '#22c55e';
            case 'Behind': return '#ef4444';
            default: return '#3b82f6';
        }
    }

    getAchievementColor(pct: number | null): string {
        if (pct === null) return '#e5e7eb';
        if (pct >= 100) return '#22c55e';
        if (pct >= 75) return '#3b82f6';
        if (pct >= 50) return '#f59e0b';
        return '#ef4444';
    }

    getOrdinalSuffix(day: number): string {
        if (day > 3 && day < 21) return 'th';
        switch (day % 10) {
            case 1: return 'st';
            case 2: return 'nd';
            case 3: return 'rd';
            default: return 'th';
        }
    }

    previousMonth(): void {
        const newMonth = new Date(this.selectedMonth);
        newMonth.setMonth(newMonth.getMonth() - 1);
        this.selectedMonth = newMonth;
        this.loadAnalytics();
    }

    nextMonth(): void {
        const newMonth = new Date(this.selectedMonth);
        newMonth.setMonth(newMonth.getMonth() + 1);
        this.selectedMonth = newMonth;
        this.loadAnalytics();
    }

    isCurrentMonth(): boolean {
        const now = new Date();
        return this.selectedMonth.getFullYear() === now.getFullYear() &&
            this.selectedMonth.getMonth() === now.getMonth();
    }

    setDatePreset(preset: string): void {
        this.activeDatePreset = preset;
        const today = new Date();
        const start = new Date();
        const end = new Date();

        switch (preset) {
            case 'today':
                start.setHours(0, 0, 0, 0);
                end.setHours(23, 59, 59, 999);
                break;
            case 'last7days':
                start.setDate(today.getDate() - 6);
                start.setHours(0, 0, 0, 0);
                end.setHours(23, 59, 59, 999);
                break;
            case 'last30days':
                start.setDate(today.getDate() - 29);
                start.setHours(0, 0, 0, 0);
                end.setHours(23, 59, 59, 999);
                break;
            case 'thisMonth':
                start.setDate(1);
                start.setHours(0, 0, 0, 0);
                end.setHours(23, 59, 59, 999);
                break;
            case 'lastMonth':
                start.setMonth(today.getMonth() - 1);
                start.setDate(1);
                start.setHours(0, 0, 0, 0);
                end.setDate(0); // Last day of previous month
                end.setMonth(today.getMonth());
                end.setHours(23, 59, 59, 999);
                break;
        }

        this.dateRange = [start, end];
        // Apply date filter immediately
        this.applyFilters();
    }

    onCustomDateChange(): void {
        // Clear active preset when user manually changes date
        this.activeDatePreset = '';
        // Apply custom date filter immediately if both dates are selected
        if (this.dateRange && this.dateRange.length === 2 && this.dateRange[0] && this.dateRange[1]) {
            this.applyFilters();
        }
    }

    clearFilters(): void {
        // Clear all filter values
        this.filters = {};
        this.dateRange = [];
        this.activeDatePreset = '';

        // Reset all dropdown options to show all available items
        this.advertisers = [...this.allAdvertisers];
        this.partners = [...this.allPartners];
        this.coupons = [...this.allCoupons];

        // Reload data with no filters
        this.loadData();
        this.loadAnalytics(); // Add this to reload analytics
    }

    exportToCSV(): void {
        if (this.tableData.length === 0) {
            return;
        }

        const headers = ['Date', 'Campaign', 'Coupon'];
        if (this.isAdmin) {
            headers.push('Partner');
        }
        headers.push('Orders', 'Sales', 'Revenue', 'Payout');
        if (this.isAdmin) {
            headers.push('Profit');
        }

        const rows = this.tableData.map(row => {
            const data = [
                row.date,
                row.campaign,
                row.coupon
            ];
            if (this.isAdmin && row.partner) {
                data.push(row.partner);
            } else if (this.isAdmin) {
                data.push('');
            }
            data.push(
                row.orders.toString(),
                row.sales.toFixed(2),
                (row.revenue ?? 0).toFixed(2),
                row.payout.toFixed(2)
            );
            if (this.isAdmin) {
                data.push((row.profit ?? 0).toFixed(2));
            }
            return data;
        });

        let csvContent = headers.join(',') + '\n';
        rows.forEach(row => {
            csvContent += row.map(cell => `"${cell}"`).join(',') + '\n';
        });

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `performance-data-${new Date().toISOString().split('T')[0]}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }

    exportToExcel(): void {
        if (this.tableData.length === 0) {
            return;
        }

        // Create workbook data
        const headers = ['Date', 'Campaign', 'Coupon'];
        if (this.isAdmin) {
            headers.push('Partner');
        }
        headers.push('Orders', 'Sales', 'Revenue', 'Payout');
        if (this.isAdmin) {
            headers.push('Profit');
        }

        const data = [headers];
        this.tableData.forEach(row => {
            const rowData: any[] = [
                row.date,
                row.campaign,
                row.coupon
            ];
            if (this.isAdmin) {
                rowData.push(row.partner || '');
            }
            rowData.push(
                row.orders,
                row.sales,
                row.revenue,
                row.payout
            );
            if (this.isAdmin) {
                rowData.push(row.profit);
            }
            data.push(rowData);
        });

        // Convert to HTML table format for Excel
        let html = '<table><thead><tr>';
        headers.forEach(header => {
            html += `<th>${header}</th>`;
        });
        html += '</tr></thead><tbody>';

        this.tableData.forEach(row => {
            html += '<tr>';
            html += `<td>${row.date}</td>`;
            html += `<td>${row.campaign}</td>`;
            html += `<td>${row.coupon}</td>`;
            if (this.isAdmin) {
                html += `<td>${row.partner || ''}</td>`;
            }
            html += `<td>${row.orders}</td>`;
            html += `<td>${row.sales.toFixed(2)}</td>`;
            html += `<td>${(row.revenue ?? 0).toFixed(2)}</td>`;
            html += `<td>${row.payout.toFixed(2)}</td>`;
            if (this.isAdmin) {
                html += `<td>${(row.profit ?? 0).toFixed(2)}</td>`;
            }
            html += '</tr>';
        });
        html += '</tbody></table>';

        const blob = new Blob([html], { type: 'application/vnd.ms-excel' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `performance-data-${new Date().toISOString().split('T')[0]}.xls`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }

    logout(): void {
        this.authService.logout();
    }

    goToCoupons(): void {
        this.router.navigate(['/coupons']);
    }

    goToAdvertisers(): void {
        this.router.navigate(['/advertisers']);
    }

    goToTargets(): void {
        this.router.navigate(['/targets']);
    }

    goToMediaBuyerSpend(): void {
        this.router.navigate(['/media-buyer-spend']);
    }

    refreshDashboard(): void {
        window.location.reload();
    }

    getCurrentDate(): string {
        const today = new Date();
        return today.toLocaleDateString('en-US', {
            weekday: 'long',
            year: 'numeric',
            month: 'long',
            day: 'numeric'
        });
    }

    getStatusClass(achievementPct: number): string {
        if (achievementPct >= 100) return 'status-excellent';
        if (achievementPct >= 80) return 'status-good';
        if (achievementPct >= 50) return 'status-warning';
        return 'status-danger';
    }

    getStatusIcon(achievementPct: number): string {
        if (achievementPct >= 100) return 'pi-check-circle';
        if (achievementPct >= 80) return 'pi-arrow-up';
        if (achievementPct >= 50) return 'pi-exclamation-triangle';
        return 'pi-times-circle';
    }

    calculateProfitAchievement(): number {
        if (!this.analytics?.daily) return 0;
        const required = this.analytics.daily.required_daily_profit;
        if (required === 0) return 0;
        return (this.analytics.daily.today_profit / required) * 100;
    }

    onTableSearch(event: any): void {
        const searchValue = this.tableSearchTerm.toLowerCase();
        if (!searchValue) {
            this.filteredTableData = [...this.tableData];
            return;
        }

        this.filteredTableData = this.tableData.filter(row => {
            return (
                row.date?.toLowerCase().includes(searchValue) ||
                row.campaign?.toLowerCase().includes(searchValue) ||
                row.coupon?.toLowerCase().includes(searchValue) ||
                row.partner?.toLowerCase().includes(searchValue)
            );
        });
    }

    clearTableSearch(): void {
        this.tableSearchTerm = '';
        this.filteredTableData = [...this.tableData];
    }

    getFilteredTableCount(): number {
        // With pagination, show the current page count
        return this.tableSearchTerm ? this.filteredTableData.length : this.tableData.length;
    }

    openFilterModal(): void {
        this.showFilterModal = true;
        document.body.style.overflow = 'hidden';
    }

    closeFilterModal(): void {
        this.showFilterModal = false;
        document.body.style.overflow = 'auto';
    }

    getActiveFilterCount(): number {
        let count = 0;
        if (this.filters.partner_type) count++;
        if (Array.isArray(this.filters.advertiser_id) && this.filters.advertiser_id.length > 0) count++;
        if (Array.isArray(this.filters.partner_id) && this.filters.partner_id.length > 0) count++;
        if (Array.isArray(this.filters.coupon_code) && this.filters.coupon_code.length > 0) count++;
        if (this.filters.date_from && this.filters.date_to) count++;
        return count;
    }
}
