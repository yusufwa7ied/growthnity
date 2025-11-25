import { CommonModule } from '@angular/common';
import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { NavigationEnd, Router } from '@angular/router';
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
    isAdmin: boolean = false;              // OpsManager or Admin (full access)
    isMediaBuyer: boolean = false;         // TeamMember with media_buying department
    isDepartmentRestricted: boolean = false; // TeamMember with affiliate/influencer department
    canAccessCoupons: boolean = false;     // Only OpsManager/Admin
    displayRole: string = '';

    constructor(
        private authService: AuthService,
        private router: Router
    ) {
        this.user = this.authService.currentUser();
        this.role = this.user?.role || '';
        this.isAdmin = ['Admin', 'OpsManager'].includes(this.role);
        
        // Determine user access level based on role and department
        if (this.isAdmin) {
            // OpsManager/Admin: Full system access
            this.isMediaBuyer = false;
            this.isDepartmentRestricted = false;
            this.canAccessCoupons = true;
        } else if (this.role === 'TeamMember') {
            // TeamMember: Check department
            if (this.user?.department === 'media_buying') {
                this.isMediaBuyer = true;
                this.isDepartmentRestricted = false;
                this.canAccessCoupons = false;
            } else if (this.user?.department === 'affiliate' || this.user?.department === 'influencer') {
                this.isMediaBuyer = false;
                this.isDepartmentRestricted = true;
                this.canAccessCoupons = false;
            }
        }
        
        this.displayRole = this.getDisplayRole();
    }

    ngOnInit(): void {
        // Set initial active tab based on current URL
        this.updateActiveTab(this.router.url);

        // Dynamically update activeTab based on route changes
        this.router.events.pipe(
            filter(event => event instanceof NavigationEnd)
        ).subscribe((event: any) => {
            this.updateActiveTab(event.urlAfterRedirects || event.url || '');
        });
    }

    private updateActiveTab(url: string): void {
        // Use exact match to avoid conflicts (e.g., /dashboard matching /coupons)
        if (url === '/' || url.startsWith('/dashboard')) {
            this.activeTab = 'dashboard';
        } else if (url.startsWith('/coupons')) {
            this.activeTab = 'coupons';
        } else if (url.startsWith('/advertisers')) {
            this.activeTab = 'advertisers';
        } else if (url.startsWith('/partners')) {
            this.activeTab = 'partners';
        } else if (url.startsWith('/targets')) {
            this.activeTab = 'targets';
        } else if (url.startsWith('/media-buyer-spend')) {
            this.activeTab = 'daily-spend';
        }
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
        this.activeTab = 'dashboard';
        this.router.navigate(['/dashboard'], { skipLocationChange: false });
    }

    goToCoupons(): void {
        this.activeTab = 'coupons';
        this.router.navigate(['/coupons'], { skipLocationChange: false });
    }

    goToAdvertisers(): void {
        this.activeTab = 'advertisers';
        this.router.navigate(['/advertisers'], { skipLocationChange: false });
    }

    goToTargets(): void {
        this.activeTab = 'targets';
        this.router.navigate(['/targets'], { skipLocationChange: false });
    }

    goToPartners(): void {
        this.activeTab = 'partners';
        this.router.navigate(['/partners'], { skipLocationChange: false });
    }

    goToMediaBuyerSpend(): void {
        this.activeTab = 'daily-spend';
        this.router.navigate(['/media-buyer-spend'], { skipLocationChange: false });
    }
}