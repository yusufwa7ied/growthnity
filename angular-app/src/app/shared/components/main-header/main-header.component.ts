import { CommonModule } from '@angular/common';
import { Component, EventEmitter, Input, Output } from '@angular/core';
import { Router } from '@angular/router';
import { User } from '../../../core/models/user.model';
import { AuthService } from '../../../core/services/auth.service';

@Component({
    selector: 'app-main-header',
    standalone: true,
    imports: [CommonModule],
    templateUrl: './main-header.component.html',
    styleUrl: './main-header.component.css'
})
export class MainHeaderComponent {
    @Input() showSidebarButton: boolean = true;
    @Input() activeTab: 'dashboard' | 'daily-spend' | 'coupons' | 'advertisers' | 'partners' | 'targets' = 'dashboard';
    @Output() sidebarToggle = new EventEmitter<void>();

    user: User | null = null;
    role: string = '';
    isAdmin: boolean = false;
    isMediaBuyer: boolean = false;
    displayRole: string = '';

    constructor(
        private authService: AuthService,
        private router: Router
    ) {
        this.user = this.authService.currentUser();
        this.role = this.user?.role || '';
        this.isAdmin = ['Admin', 'OpsManager'].includes(this.role);
        this.isMediaBuyer = this.role === 'TeamMember' && this.user?.department === 'media_buying';
        this.displayRole = this.getDisplayRole();
    }

    private getDisplayRole(): string {
        if (this.role === 'Admin' || this.role === 'OpsManager') {
            return this.role;
        }

        if (this.role === 'TeamMember' && this.user?.department) {
            const departmentMap: { [key: string]: string } = {
                'media_buying': 'Media Buyer',
                'affiliate': 'Affiliate',
                'influencer': 'Influencer'
            };
            return departmentMap[this.user.department] || this.role;
        }

        return this.role;
    }

    toggleSidebar(): void {
        this.sidebarToggle.emit();
    }

    logout(): void {
        this.authService.logout();
    }

    refreshDashboard(): void {
        if (this.activeTab === 'dashboard') {
            window.location.reload();
        } else {
            this.router.navigate(['/dashboard']);
        }
    }

    goToDashboard(): void {
        this.router.navigate(['/dashboard'], { skipLocationChange: false });
    }

    goToCoupons(): void {
        this.router.navigate(['/coupons'], { skipLocationChange: false });
    }

    goToAdvertisers(): void {
        this.router.navigate(['/advertisers'], { skipLocationChange: false });
    }

    goToTargets(): void {
        this.router.navigate(['/targets'], { skipLocationChange: false });
    }

    goToPartners(): void {
        this.router.navigate(['/partners'], { skipLocationChange: false });
    }

    goToMediaBuyerSpend(): void {
        this.router.navigate(['/media-buyer-spend'], { skipLocationChange: false });
    }
}