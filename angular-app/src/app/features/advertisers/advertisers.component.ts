import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { TableModule } from 'primeng/table';
import { User } from '../../core/models/user.model';
import { Advertiser, AdvertiserFormData, AdvertiserService, PartnerPayout } from '../../core/services/advertiser.service';
import { AuthService } from '../../core/services/auth.service';
import { FooterComponent } from '../../shared/components/footer/footer.component';
import { MainHeaderComponent } from '../../shared/components/main-header/main-header.component';
import { SkeletonLoaderComponent } from '../../shared/components/skeleton-loader/skeleton-loader.component';

@Component({
    selector: 'app-advertisers',
    standalone: true,
    imports: [CommonModule, FormsModule, TableModule, MainHeaderComponent, FooterComponent, SkeletonLoaderComponent],
    templateUrl: './advertisers.component.html',
    styleUrl: './advertisers.component.css'
})
export class AdvertisersComponent implements OnInit {
    advertisers: Advertiser[] = [];
    partners: any[] = [];
    user: User | null = null;
    role: string = '';
    loading = false;
    error = '';
    successMessage = '';

    // Form fields
    showAddForm = false;
    editingId: number | null = null;
    advertiserName = '';
    attribution = 'Coupon';
    revRateType = 'percent';
    revFtuRate: number | null = null;
    revRtuRate: number | null = null;
    revFtuFixedBonus: number | null = null;
    revRtuFixedBonus: number | null = null;
    currency = 'USD';
    exchangeRate: number | null = 1;
    defaultPayoutRateType = 'percent';
    defaultFtuPayout: number | null = null;
    defaultRtuPayout: number | null = null;
    defaultFtuFixedBonus: number | null = null;
    defaultRtuFixedBonus: number | null = null;
    partnerPayouts: PartnerPayout[] = [];
    selectedAdvertiserForPayouts: number | null = null;

    constructor(
        private advertiserService: AdvertiserService,
        private router: Router,
        private authService: AuthService
    ) { }

    ngOnInit() {
        this.user = this.authService.currentUser();
        this.role = this.user?.role || '';
        this.loadAdvertisers();
        this.loadPartners();
    }

    loadPartners() {
        this.advertiserService.getPartners().subscribe({
            next: (data) => {
                this.partners = data;
            },
            error: (err) => {
                console.error('Failed to load partners', err);
            }
        });
    }

    loadAdvertisers() {
        this.loading = true;
        this.error = '';
        this.advertiserService.getAdvertisers().subscribe({
            next: (data) => {
                this.advertisers = data;
                this.loading = false;
            },
            error: (err) => {
                this.error = 'Failed to load advertisers';
                this.loading = false;
                console.error(err);
            }
        });
    }

    showAdd() {
        this.showAddForm = true;
        this.resetForm();
    }

    cancelForm() {
        this.showAddForm = false;
        this.editingId = null;
        this.resetForm();
    }

    resetForm() {
        this.advertiserName = '';
        this.attribution = 'Coupon';
        this.revRateType = 'percent';
        this.revFtuRate = null;
        this.revRtuRate = null;
        this.revFtuFixedBonus = null;
        this.revRtuFixedBonus = null;
        this.currency = 'USD';
        this.exchangeRate = 1;
        this.defaultPayoutRateType = 'percent';
        this.defaultFtuPayout = null;
        this.defaultRtuPayout = null;
        this.defaultFtuFixedBonus = null;
        this.defaultRtuFixedBonus = null;
        this.partnerPayouts = [];
        this.selectedAdvertiserForPayouts = null;
        this.error = '';
    }

    addPartnerPayout() {
        this.partnerPayouts.push({
            partner_id: 0,
            ftu_payout: null,
            rtu_payout: null,
            ftu_fixed_bonus: null,
            rtu_fixed_bonus: null,
            rate_type: 'percent'
        });
    }

    removePartnerPayout(index: number) {
        this.partnerPayouts.splice(index, 1);
    }

    savePartnerPayouts() {
        if (!this.selectedAdvertiserForPayouts) {
            this.error = 'Please select an advertiser first';
            setTimeout(() => this.error = '', 3000);
            return;
        }

        const advertiser = this.advertisers.find(a => a.id == this.selectedAdvertiserForPayouts);
        if (!advertiser) {
            this.error = 'Selected advertiser not found. Please refresh and try again.';
            setTimeout(() => this.error = '', 3000);
            return;
        }

        // Filter out partner payouts with no partner selected
        const validPayouts = this.partnerPayouts.filter(p => p.partner_id && p.partner_id > 0);

        if (validPayouts.length === 0) {
            this.error = 'Please add at least one partner payout with a selected partner';
            setTimeout(() => this.error = '', 3000);
            return;
        }

        const advertiserData: AdvertiserFormData = {
            name: advertiser.name,
            attribution: advertiser.attribution,
            rev_rate_type: advertiser.rev_rate_type,
            rev_ftu_rate: advertiser.rev_ftu_rate,
            rev_rtu_rate: advertiser.rev_rtu_rate,
            rev_ftu_fixed_bonus: advertiser.rev_ftu_fixed_bonus,
            rev_rtu_fixed_bonus: advertiser.rev_rtu_fixed_bonus,
            currency: advertiser.currency,
            exchange_rate: advertiser.exchange_rate,
            default_payout_rate_type: advertiser.default_payout_rate_type,
            default_ftu_payout: advertiser.default_ftu_payout,
            default_rtu_payout: advertiser.default_rtu_payout,
            default_ftu_fixed_bonus: advertiser.default_ftu_fixed_bonus,
            default_rtu_fixed_bonus: advertiser.default_rtu_fixed_bonus,
            partner_payouts: validPayouts
        };

        this.loading = true;
        this.error = '';

        this.advertiserService.updateAdvertiser(this.selectedAdvertiserForPayouts, advertiserData).subscribe({
            next: () => {
                this.loadAdvertisers();
                this.loading = false;
                this.error = '';
                this.selectedAdvertiserForPayouts = null;
                this.partnerPayouts = [];

                this.successMessage = 'Partner payouts updated successfully!';
                window.scrollTo({ top: 0, behavior: 'smooth' });
                setTimeout(() => this.successMessage = '', 3000);
            },
            error: (err) => {
                this.error = 'Failed to update partner payouts. Please try again.';
                this.loading = false;
                setTimeout(() => this.error = '', 3000);
                console.error(err);
            }
        });
    }

    saveAdvertiser() {
        if (!this.advertiserName.trim()) {
            this.error = 'Advertiser name is required';
            return;
        }

        // Filter out partner payouts with no partner selected
        const validPayouts = this.partnerPayouts.filter(p => p.partner_id && p.partner_id > 0);

        const advertiserData: AdvertiserFormData = {
            name: this.advertiserName.trim(),
            attribution: this.attribution,
            rev_rate_type: this.revRateType,
            rev_ftu_rate: this.revFtuRate,
            rev_rtu_rate: this.revRtuRate,
            rev_ftu_fixed_bonus: this.revFtuFixedBonus,
            rev_rtu_fixed_bonus: this.revRtuFixedBonus,
            currency: this.currency,
            exchange_rate: this.exchangeRate,
            default_payout_rate_type: this.defaultPayoutRateType,
            default_ftu_payout: this.defaultFtuPayout,
            default_rtu_payout: this.defaultRtuPayout,
            default_ftu_fixed_bonus: this.defaultFtuFixedBonus,
            default_rtu_fixed_bonus: this.defaultRtuFixedBonus,
            partner_payouts: validPayouts
        };

        this.loading = true;
        this.error = '';

        if (this.editingId) {
            this.advertiserService.updateAdvertiser(this.editingId, advertiserData).subscribe({
                next: () => {
                    this.loadAdvertisers();
                    this.cancelForm();
                    this.loading = false;

                    this.successMessage = 'Advertiser updated successfully!';
                    window.scrollTo({ top: 0, behavior: 'smooth' });
                    setTimeout(() => this.successMessage = '', 3000);
                },
                error: (err) => {
                    this.error = 'Failed to update advertiser. Please try again.';
                    this.loading = false;
                    setTimeout(() => this.error = '', 3000);
                    console.error(err);
                }
            });
        } else {
            this.advertiserService.createAdvertiser(advertiserData).subscribe({
                next: () => {
                    this.loadAdvertisers();
                    this.cancelForm();
                    this.loading = false;

                    this.successMessage = 'Advertiser created successfully!';
                    window.scrollTo({ top: 0, behavior: 'smooth' });
                    setTimeout(() => this.successMessage = '', 3000);
                },
                error: (err) => {
                    this.error = 'Failed to create advertiser. Please try again.';
                    this.loading = false;
                    setTimeout(() => this.error = '', 3000);
                    console.error(err);
                }
            });
        }
    }

    editAdvertiser(advertiser: Advertiser) {
        // Prevent editing Noon - it uses bracket-based payouts configured in code
        if (advertiser.name === 'Noon') {
            this.error = 'Noon advertiser cannot be edited. It uses a bracket-based payout system. To add special payouts for partners, use the Admin → Partner Payouts section.';
            window.scrollTo({ top: 0, behavior: 'smooth' });
            setTimeout(() => {
                this.error = '';
            }, 5000);
            return;
        }

        this.editingId = advertiser.id;
        this.advertiserName = advertiser.name;
        this.attribution = advertiser.attribution;
        this.revRateType = advertiser.rev_rate_type;
        this.revFtuRate = advertiser.rev_ftu_rate;
        this.revRtuRate = advertiser.rev_rtu_rate;
        this.revFtuFixedBonus = advertiser.rev_ftu_fixed_bonus || null;
        this.revRtuFixedBonus = advertiser.rev_rtu_fixed_bonus || null;
        this.currency = advertiser.currency;
        this.exchangeRate = advertiser.exchange_rate;
        this.defaultPayoutRateType = advertiser.default_payout_rate_type;
        this.defaultFtuPayout = advertiser.default_ftu_payout;
        this.defaultRtuPayout = advertiser.default_rtu_payout;
        this.defaultFtuFixedBonus = advertiser.default_ftu_fixed_bonus || null;
        this.defaultRtuFixedBonus = advertiser.default_rtu_fixed_bonus || null;
        this.partnerPayouts = advertiser.partner_payouts ? [...advertiser.partner_payouts] : [];
        this.showAddForm = true;

        // Scroll to top so user sees the form
        window.scrollTo({ top: 0, behavior: 'smooth' });
    }

    canEditAdvertiser(advertiser: Advertiser): boolean {
        // Noon uses bracket-based payouts and cannot be edited here
        return advertiser.name !== 'Noon';
    }

    deleteAdvertiser(id: number) {
        if (!confirm('Are you sure you want to delete this advertiser? This action cannot be undone.')) {
            return;
        }

        this.loading = true;
        this.error = '';

        this.advertiserService.deleteAdvertiser(id).subscribe({
            next: () => {
                this.loadAdvertisers();
                this.loading = false;

                this.successMessage = 'Advertiser deleted successfully!';
                window.scrollTo({ top: 0, behavior: 'smooth' });
                setTimeout(() => this.successMessage = '', 3000);
            },
            error: (err) => {
                this.error = 'Failed to delete advertiser. Please try again.';
                this.loading = false;
                setTimeout(() => this.error = '', 3000);
                console.error(err);
            }
        });
    }

    onAdvertiserForPayoutsChange(): void {
        if (this.selectedAdvertiserForPayouts) {
            // Load existing partner payouts for the selected advertiser
            const advertiser = this.advertisers.find(a => a.id === this.selectedAdvertiserForPayouts);
            if (advertiser && advertiser.partner_payouts) {
                this.partnerPayouts = [...advertiser.partner_payouts];
            } else {
                this.partnerPayouts = [];
            }
        } else {
            this.partnerPayouts = [];
        }
    }

    goToDashboard(): void {
        this.router.navigate(['/dashboard']);
    }

    goToCoupons(): void {
        this.router.navigate(['/coupons']);
    }

    goToTargets(): void {
        this.router.navigate(['/targets']);
    }

    logout(): void {
        this.authService.logout();
    }
}
