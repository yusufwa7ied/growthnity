import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { MessageService } from 'primeng/api';
import { ButtonModule } from 'primeng/button';
import { Card } from 'primeng/card';
import { InputNumber } from 'primeng/inputnumber';
import { InputText } from 'primeng/inputtext';
import { MultiSelect } from 'primeng/multiselect';
import { Select } from 'primeng/select';
import { TableModule } from 'primeng/table';
import { Textarea } from 'primeng/textarea';
import { ToastModule } from 'primeng/toast';
import { TooltipModule } from 'primeng/tooltip';
import { Coupon } from '../../core/models/coupon.model';
import { AuthService } from '../../core/services/auth.service';
import { CouponService } from '../../core/services/coupon.service';
import { FooterComponent } from '../../shared/components/footer/footer.component';
import { MainHeaderComponent } from '../../shared/components/main-header/main-header.component';
import { SkeletonLoaderComponent } from '../../shared/components/skeleton-loader/skeleton-loader.component';

@Component({
    selector: 'app-coupons',
    standalone: true,
    imports: [CommonModule, FormsModule, ButtonModule, Card, Textarea, Select, MultiSelect, TableModule, ToastModule, InputNumber, InputText, TooltipModule, MainHeaderComponent, FooterComponent, SkeletonLoaderComponent],
    providers: [MessageService],
    templateUrl: './coupons.component.html',
    styleUrls: ['./coupons.component.css']
})
export class CouponsComponent implements OnInit {
    isAdmin: boolean = false;
    loading: boolean = false;
    showSkeletons: boolean = true;
    private skeletonMinDuration = 500; // Minimum ms to show skeleton

    // Form toggle states
    showNewCouponForm: boolean = false;
    showAssignCouponForm: boolean = false;

    // Form data
    newCouponCodes: string = '';
    selectedAdvertiser: number | null = null;
    selectedPartner: number | null = null;
    discountPercent: number | null = null;
    geo: string = '';
    selectedExistingCoupons: string[] = [];
    selectedCouponAdvertisers: { [key: string]: number } = {};  // Map coupon code+advertiser to advertiser_id

    // All available options (never filtered)
    allAdvertisers: any[] = [];
    allPartners: any[] = [];
    allCouponOptions: { label: string; value: string }[] = [];

    // Filtered options (dynamically updated based on selections)
    advertisers: any[] = [];
    partners: any[] = [];
    couponOptions: { label: string; value: string }[] = [];

    // Table data
    allCoupons: Coupon[] = [];
    coupons: Coupon[] = [];
    filteredCoupons: Coupon[] = [];

    // History data
    expandedRows: { [key: string]: boolean } = {};
    couponHistory: { [key: string]: any[] } = {};
    loadingHistory: { [key: string]: boolean } = {};

    // Analytics/Stats
    stats = {
        totalCoupons: 0,
        assignedCoupons: 0,
        unassignedCoupons: 0,
        activeAdvertisers: 0,
        activePartners: 0,
        utilizationRate: 0
    };

    // Filters
    filterAdvertiser: number | null = null;
    filterPartner: number | null = null;
    filterCoupon: string | null = null;
    showUnassignedOnly: boolean = false;

    constructor(
        private authService: AuthService,
        private couponService: CouponService,
        private messageService: MessageService,
        private router: Router
    ) { }

    ngOnInit(): void {
        const user = this.authService.currentUser();
        this.isAdmin = ['Admin', 'OpsManager'].includes(user?.role || '');

        if (!this.isAdmin) {
            this.router.navigate(['/dashboard']);
            return;
        }

        this.loadData();
    }

    loadData(): void {
        this.loading = true;
        this.showSkeletons = true;
        const startTime = Date.now();
        this.couponService.getCoupons().subscribe({
            next: (data) => {
                this.coupons = data;
                this.allCoupons = data;
                this.filteredCoupons = data;

                // Build coupon options with advertiser info for display
                // Use compound key (code|advertiser_id) to handle duplicate codes
                this.allCouponOptions = this.allCoupons.map(c => ({
                    label: `${c.code} (${c.advertiser})`,
                    value: `${c.code}|${c.advertiser_id}`  // Compound key
                }));
                this.couponOptions = [...this.allCouponOptions];

                // Build map of compound key to advertiser ID
                this.selectedCouponAdvertisers = {};
                this.allCoupons.forEach(c => {
                    const key = `${c.code}|${c.advertiser_id}`;
                    this.selectedCouponAdvertisers[key] = c.advertiser_id;
                });

                this.calculateStats();
                this.applyFilters(); // Apply current filters to refresh the table
                // Ensure minimum skeleton display duration
                const elapsed = Date.now() - startTime;
                const delay = Math.max(0, this.skeletonMinDuration - elapsed);
                setTimeout(() => {
                    this.showSkeletons = false;
                    this.loading = false;
                }, delay);
            },
            error: () => {
                this.messageService.add({ severity: 'error', summary: 'Error', detail: 'Failed to load coupons' });
                const elapsed = Date.now() - startTime;
                const delay = Math.max(0, this.skeletonMinDuration - elapsed);
                setTimeout(() => {
                    this.showSkeletons = false;
                    this.loading = false;
                }, delay);
            }
        });

        this.couponService.getAdvertisers().subscribe({
            next: (data) => {
                this.allAdvertisers = data.map(a => ({ label: a.name, value: a.id }));
                this.advertisers = [...this.allAdvertisers];
            }
        });

        this.couponService.getPartners().subscribe({
            next: (data) => {
                this.allPartners = data.map(p => ({ label: p.name, value: p.id }));
                this.partners = [...this.allPartners];
            }
        });
    }

    toggleNewCouponForm(): void {
        this.showNewCouponForm = !this.showNewCouponForm;
        if (this.showNewCouponForm) {
            this.showAssignCouponForm = false;
        }
        this.resetForm();
    }

    toggleAssignCouponForm(): void {
        this.showAssignCouponForm = !this.showAssignCouponForm;
        if (this.showAssignCouponForm) {
            this.showNewCouponForm = false;
        }
        this.resetForm();
    }

    resetForm(): void {
        this.newCouponCodes = '';
        this.selectedAdvertiser = null;
        this.selectedPartner = null;
        this.discountPercent = null;
        this.geo = '';
        this.selectedExistingCoupons = [];
        this.selectedCouponAdvertisers = {};
    }

    submitCoupon(): void {
        if (this.showNewCouponForm) {
            // Create new coupons (one request per code)
            if (!this.selectedAdvertiser) {
                this.messageService.add({ severity: 'warn', summary: 'Missing advertiser', detail: 'Please select an advertiser.' });
                return;
            }

            const codes = this.newCouponCodes
                .split(/[\n,]+/)
                .map(s => s.trim())
                .filter(Boolean);

            if (codes.length === 0) {
                this.messageService.add({ severity: 'warn', summary: 'No codes', detail: 'Please enter at least one coupon code.' });
                return;
            }

            this.loading = true;
            let successes = 0;
            let failures = 0;

            codes.forEach(code => {
                const payload: any = {
                    code: code,
                    advertiser: this.selectedAdvertiser,
                };

                if (this.selectedPartner) payload.partner = this.selectedPartner;
                if (this.geo) payload.geo = this.geo;
                if (this.discountPercent !== null && this.discountPercent !== undefined) payload.discount_percent = this.discountPercent;

                this.couponService.createCoupon(payload).subscribe({
                    next: (response) => {
                        successes++;
                        if (successes + failures === codes.length) {
                            this.loading = false;
                            if (failures === 0) {
                                this.messageService.add({ severity: 'success', summary: 'Success', detail: `All ${successes} coupons created successfully.` });
                            } else {
                                this.messageService.add({ severity: 'info', summary: 'Partial Success', detail: `${successes} created, ${failures} failed.` });
                            }
                            this.loadData();
                            this.resetForm();
                            this.showNewCouponForm = false;
                        }
                    },
                    error: (err) => {
                        failures++;
                        const detail = err?.error?.error || err?.error?.message || `Failed to create ${code}`;
                        this.messageService.add({ severity: 'error', summary: `Error: ${code}`, detail });
                        if (successes + failures === codes.length) {
                            this.loading = false;
                            if (successes > 0) {
                                this.messageService.add({ severity: 'info', summary: 'Completed', detail: `${successes} created, ${failures} failed.` });
                            }
                            this.loadData();
                            this.resetForm();
                        }
                    }
                });
            });

            return;
        }

        if (this.showAssignCouponForm) {
            // Assign existing coupons to a partner (PATCH per coupon)
            if (!this.selectedExistingCoupons || this.selectedExistingCoupons.length === 0) {
                this.messageService.add({ severity: 'warn', summary: 'No coupons selected', detail: 'Please select at least one existing coupon.' });
                return;
            }

            this.loading = true;
            let successes = 0;
            let failures = 0;
            const total = this.selectedExistingCoupons.length;

            this.selectedExistingCoupons.forEach(couponKey => {
                // couponKey is in format "code|advertiser_id"
                const [code, advertiserId] = couponKey.split('|');

                const payload: any = {
                    partner: this.selectedPartner || null,  // Allow null to unassign
                    geo: this.geo || null,  // Always send geo field, use null to clear
                };
                if (this.discountPercent !== null && this.discountPercent !== undefined) {
                    payload.discount_percent = this.discountPercent;
                }

                const advIdNum = parseInt(advertiserId, 10);

                this.couponService.updateCoupon(code, payload, advIdNum).subscribe({
                    next: (response) => {
                        successes++;
                        if (successes + failures === total) {
                            this.loading = false;
                            if (failures === 0) {
                                this.messageService.add({ severity: 'success', summary: 'Success', detail: `All ${successes} coupons assigned successfully.` });
                            } else {
                                this.messageService.add({ severity: 'info', summary: 'Partial Success', detail: `${successes} assigned, ${failures} failed.` });
                            }
                            this.loadData();
                            this.resetForm();
                            this.showAssignCouponForm = false;
                        }
                    },
                    error: (err) => {
                        failures++;
                        const detail = err?.error?.error || err?.error?.message || `Failed to assign ${code}`;
                        this.messageService.add({ severity: 'error', summary: `Error: ${code}`, detail });
                        if (successes + failures === total) {
                            this.loading = false;
                            if (successes > 0) {
                                this.messageService.add({ severity: 'info', summary: 'Completed', detail: `${successes} assigned, ${failures} failed.` });
                            }
                            this.loadData();
                            this.resetForm();
                        }
                    }
                });
            });
        }
    }

    editCoupon(row: Coupon): void {
        // Prefill assign form to quickly edit a coupon
        this.showAssignCouponForm = true;
        this.showNewCouponForm = false;
        // Use compound key: "code|advertiser_id"
        this.selectedExistingCoupons = [`${row.code}|${row.advertiser_id}`];
        this.selectedPartner = row.partner_id || null;
        this.discountPercent = row.discount || null;
        this.geo = row.geo || '';  // geo will be null or string, convert null to empty string for input

        // Scroll to top so user sees the form
        window.scrollTo({ top: 0, behavior: 'smooth' });
    }

    onFilterAdvertiserChange(): void {
        this.recomputeFilterDropdowns();
        this.applyFilters();
    }

    onFilterPartnerChange(): void {
        this.recomputeFilterDropdowns();
        this.applyFilters();
    }

    onFilterCouponChange(): void {
        this.recomputeFilterDropdowns();
        this.applyFilters();
    }


    /**
     * Dynamically filter dropdown options based on current filter selections.
     * Mirrors the logic from dashboard component
     */
    recomputeFilterDropdowns(): void {
        // Start with all available coupons
        let filteredData = [...this.allCoupons];

        // Apply current filter selections to narrow down the dataset
        if (this.filterAdvertiser) {
            filteredData = filteredData.filter(c => c.advertiser_id === this.filterAdvertiser);
        }

        if (this.filterPartner) {
            filteredData = filteredData.filter(c => c.partner_id === this.filterPartner);
        }

        if (this.filterCoupon) {
            // filterCoupon might be just a code, extract it if it's a compound key
            const filterCode = this.filterCoupon.includes('|') ? this.filterCoupon.split('|')[0] : this.filterCoupon;
            filteredData = filteredData.filter(c => c.code === filterCode);
        }

        // Build dropdown options from filtered dataset
        const advertiserIds = new Set(filteredData.map(c => c.advertiser_id));
        this.advertisers = this.allAdvertisers.filter(a => advertiserIds.has(a.value));

        const partnerIds = new Set(filteredData.filter(c => c.partner_id).map(c => c.partner_id));
        this.partners = this.allPartners.filter(p => partnerIds.has(p.value));

        // Build coupon options from filtered coupons with compound keys
        this.couponOptions = filteredData.map(c => ({
            label: `${c.code} (${c.advertiser})`,
            value: `${c.code}|${c.advertiser_id}`
        }));
    }

    applyFilters(): void {
        this.filteredCoupons = this.allCoupons.filter(c => {
            if (this.showUnassignedOnly && c.partner_id !== null) return false;
            if (this.filterAdvertiser && c.advertiser_id !== this.filterAdvertiser) return false;
            if (this.filterPartner && c.partner_id !== this.filterPartner) return false;
            if (this.filterCoupon && c.code !== this.filterCoupon) return false;
            return true;
        });
    }

    toggleUnassignedFilter(): void {
        this.showUnassignedOnly = !this.showUnassignedOnly;
        this.applyFilters();
    }

    clearFilters(): void {
        this.filterAdvertiser = null;
        this.filterPartner = null;
        this.filterCoupon = null;
        this.showUnassignedOnly = false;
        this.filteredCoupons = this.allCoupons;
        // Reset dropdown options to show all
        this.advertisers = [...this.allAdvertisers];
        this.partners = [...this.allPartners];
        this.couponOptions = [...this.allCouponOptions];
    }

    goToDashboard(): void {
        this.router.navigate(['/dashboard']);
    }

    goToAdvertisers(): void {
        this.router.navigate(['/advertisers']);
    }

    goToTargets(): void {
        this.router.navigate(['/targets']);
    }

    logout(): void {
        this.authService.logout();
        this.router.navigate(['/login']);
    }

    get user() {
        return this.authService.currentUser();
    }

    get role() {
        return this.user?.role || '';
    }

    calculateStats(): void {
        this.stats.totalCoupons = this.allCoupons.length;
        this.stats.assignedCoupons = this.allCoupons.filter(c => c.partner_id !== null).length;
        this.stats.unassignedCoupons = this.stats.totalCoupons - this.stats.assignedCoupons;

        // Count unique advertisers and partners
        const uniqueAdvertisers = new Set(this.allCoupons.map(c => c.advertiser_id));
        const uniquePartners = new Set(this.allCoupons.filter(c => c.partner_id).map(c => c.partner_id));

        this.stats.activeAdvertisers = uniqueAdvertisers.size;
        this.stats.activePartners = uniquePartners.size;
        this.stats.utilizationRate = this.stats.totalCoupons > 0
            ? Math.round((this.stats.assignedCoupons / this.stats.totalCoupons) * 100)
            : 0;
    }

    toggleHistory(couponCode: string): void {
        this.expandedRows[couponCode] = !this.expandedRows[couponCode];

        // Load history if expanding and not already loaded
        if (this.expandedRows[couponCode] && !this.couponHistory[couponCode]) {
            this.loadHistory(couponCode);
        }
    }

    loadHistory(couponCode: string): void {
        this.loadingHistory[couponCode] = true;
        this.couponService.getCouponHistory(couponCode).subscribe({
            next: (history) => {
                this.couponHistory[couponCode] = history;
                this.loadingHistory[couponCode] = false;
            },
            error: () => {
                this.messageService.add({
                    severity: 'error',
                    summary: 'Error',
                    detail: 'Failed to load coupon history'
                });
                this.loadingHistory[couponCode] = false;
            }
        });
    }
}
