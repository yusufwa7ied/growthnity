import { CommonModule } from '@angular/common';
import { Component, EventEmitter, Input, Output, OnInit } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';
import { filter } from 'rxjs';
import { User } from '../../../core/models/user.model';
import { AuthService } from '../../../core/services/auth.service';

@Component({
    selector: 'app-main-header',
    standalone: true,
    imports: [CommonModule],
    templateUrl: './main-header.component.html',
    styleUrl: './main-header.component.css'
})
export class MainHeaderComponent implements OnInit {
    @Input() showSidebarButton: boolean = true;
    @Output() sidebarToggle = new EventEmitter<void>();

    activeTab: 'dashboard' | 'daily-spend' | 'coupons' | 'advertisers' | 'partners' | 'targets' = 'dashboard';
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

    ngOnInit(): void {
        // Dynamically update activeTab based on current route
        this.router.events.pipe(
            filter(event => event instanceof NavigationEnd)
        ).subscribe((event: any) => {
            const url = event.urlAfterRedirects || '';
            if (url.includes('/dashboard')) {
                this.activeTab = 'dashboard';
            } else if (url.includes('/coupons')) {
                this.activeTab = 'coupons';
            } else if (url.includes('/advertisers')) {
                this.activeTab = 'advertisers';
            } else if (url.includes('/partners')) {
                this.activeTab = 'partners';
            } else if (url.includes('/targets')) {
                this.activeTab = 'targets';
            } else if (url.includes('/media-buyer-spend')) {
                this.activeTab = 'daily-spend';
            }
        });
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