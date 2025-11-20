import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { ButtonModule } from 'primeng/button';
import { TableModule } from 'primeng/table';
import { User } from '../../core/models/user.model';
import { AuthService } from '../../core/services/auth.service';
import { Partner, PartnerFormData, PartnerService } from '../../core/services/partner.service';
import { FooterComponent } from '../../shared/components/footer/footer.component';
import { MainHeaderComponent } from '../../shared/components/main-header/main-header.component';

@Component({
    selector: 'app-partners',
    standalone: true,
    imports: [CommonModule, FormsModule, ButtonModule, TableModule, MainHeaderComponent, FooterComponent],
    templateUrl: './partners.component.html',
    styleUrls: ['./partners.component.css']
})
export class PartnersComponent implements OnInit {
    user: User | null = null;
    role: string = '';
    partners: Partner[] = [];
    showAddForm: boolean = false;
    editingId: number | null = null;
    successMessage = '';
    loading = false;
    selectedTypeFilter = '';
    filteredPartners: Partner[] = [];
    expandedPartnerId: number | null = null;
    showOnlySpecial = false;

    formData: PartnerFormData = {
        name: '',
        partner_type: '',
        email: '',
        phone: ''
    };

    partnerTypes = [
        { label: 'Affiliate', value: 'AFF' },
        { label: 'Influencer', value: 'INF' },
        { label: 'Media Buying', value: 'MB' }
    ];

    stats = {
        totalPartners: 0,
        affiliates: 0,
        influencers: 0,
        mediaBuyers: 0
    };

    constructor(
        private partnerService: PartnerService,
        private authService: AuthService,
        private router: Router
    ) { }

    ngOnInit(): void {
        this.user = this.authService.currentUser();
        this.role = this.user?.role || '';
        this.loadPartners();
    }

    loadPartners(): void {
        this.partnerService.getPartners().subscribe({
            next: (data) => {
                this.partners = data;
                this.calculateStats();
                this.filterPartners();
            },
            error: (error) => console.error('Error loading partners:', error)
        });
    }

    filterPartners(): void {
        let filtered = [...this.partners];

        // Filter by type
        if (this.selectedTypeFilter) {
            filtered = filtered.filter(p => p.partner_type === this.selectedTypeFilter);
        }

        // Filter by special payouts
        if (this.showOnlySpecial) {
            filtered = filtered.filter(p => p.has_special_payouts);
        }

        this.filteredPartners = filtered;
    }

    toggleSpecialFilter(): void {
        this.showOnlySpecial = !this.showOnlySpecial;
        this.filterPartners();
    }

    calculateStats(): void {
        this.stats.totalPartners = this.partners.length;
        this.stats.affiliates = this.partners.filter(p => p.partner_type === 'AFF').length;
        this.stats.influencers = this.partners.filter(p => p.partner_type === 'INF').length;
        this.stats.mediaBuyers = this.partners.filter(p => p.partner_type === 'MB').length;
    }

    toggleAddForm(): void {
        this.showAddForm = !this.showAddForm;
        if (!this.showAddForm) {
            this.resetForm();
        }
    }

    resetForm(): void {
        this.formData = {
            name: '',
            partner_type: '',
            email: '',
            phone: ''
        };
        this.editingId = null;
    }

    editPartner(partner: Partner): void {
        this.editingId = partner.id;
        this.formData = {
            name: partner.name,
            partner_type: partner.partner_type,
            email: partner.email,
            phone: partner.phone
        };
        this.showAddForm = true;
    }

    savePartner(): void {
        if (this.editingId) {
            this.partnerService.updatePartner(this.editingId, this.formData).subscribe({
                next: () => {
                    this.loadPartners();
                    this.toggleAddForm();

                    this.successMessage = 'Partner updated successfully!';
                    window.scrollTo({ top: 0, behavior: 'smooth' });
                    setTimeout(() => this.successMessage = '', 3000);
                },
                error: (error) => {
                    console.error('Error updating partner:', error);
                    alert('Failed to update partner. Please try again.');
                }
            });
        } else {
            this.partnerService.createPartner(this.formData).subscribe({
                next: () => {
                    this.loadPartners();
                    this.toggleAddForm();

                    this.successMessage = 'Partner created successfully!';
                    window.scrollTo({ top: 0, behavior: 'smooth' });
                    setTimeout(() => this.successMessage = '', 3000);
                },
                error: (error) => {
                    console.error('Error creating partner:', error);
                    alert('Failed to create partner. Please try again.');
                }
            });
        }
    }

    deletePartner(id: number): void {
        if (confirm('Are you sure you want to delete this partner? This action cannot be undone.')) {
            this.partnerService.deletePartner(id).subscribe({
                next: () => {
                    this.loadPartners();

                    this.successMessage = 'Partner deleted successfully!';
                    window.scrollTo({ top: 0, behavior: 'smooth' });
                    setTimeout(() => this.successMessage = '', 3000);
                },
                error: (error) => {
                    console.error('Error deleting partner:', error);
                    alert('Failed to delete partner. Please try again.');
                }
            });
        }
    }

    getPartnerTypeLabel(type: string): string {
        const found = this.partnerTypes.find(t => t.value === type);
        return found ? found.label : type;
    }

    toggleSpecialPayouts(partnerId: number): void {
        if (this.expandedPartnerId === partnerId) {
            this.expandedPartnerId = null;
        } else {
            this.expandedPartnerId = partnerId;
        }
    }

}
