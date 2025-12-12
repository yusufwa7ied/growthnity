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

    activeTab: 'dashboard' | 'daily-spend' | 'coupons' | 'advertisers' | 'partners' | 'targets' | 'my-coupons' | 'campaigns' = 'dashboard';
    user: User | null = null;
    role: string = '';
    isAdmin: boolean = false;              // OpsManager or Admin (full access)
    isMediaBuyer: boolean = false;         // TeamMember with media_buying department
    isDepartmentRestricted: boolean = false; // TeamMember with affiliate/influencer department
    canAccessCoupons: boolean = false;     // Only OpsManager/Admin
    isViewOnly: boolean = false;           // ViewOnly role - dashboard only, all data
    isPartner: boolean = false;            // TeamMember with affiliate/influencer department
    displayRole: string = '';

    constructor(
        private authService: AuthService,
        private router: Router
    ) {
        this.user = this.authService.currentUser();
        this.role = this.user?.role || '';

        // Determine user access level based on role and department
        // 1. ViewOnly = Dashboard only, see all data (NO other pages)
        // 2. OpsManager (no dept) = Full access (all admin pages + daily spend)
        // 3. OpsManager (media_buying dept) = Full department data + Coupons + Daily Spend (NO other admin pages)
        // 4. OpsManager (other dept) = Full department data + Coupons only (NO other admin pages, NO daily spend)
        // 5. TeamMember (media_buying) = Filtered dashboard + Daily Spend only (NO coupons, NO admin pages)
        // 6. TeamMember (affiliate/influencer) = Filtered dashboard only

        if (this.role === 'ViewOnly') {
            // ViewOnly: Dashboard only, see all numbers
            this.isAdmin = false;
            this.isMediaBuyer = false;
            this.isDepartmentRestricted = false;
            this.canAccessCoupons = false;
            this.isViewOnly = true;
        } else if ((this.role === 'OpsManager' || this.role === 'Admin') && !this.user?.department) {
            // OpsManager/Admin with NO department: Full system access
            this.isAdmin = true;
            this.isMediaBuyer = true;  // Can also see daily spend
            this.isDepartmentRestricted = false;
            this.canAccessCoupons = true;
        } else if (this.role === 'OpsManager' && this.user?.department === 'media_buying') {
            // OpsManager with media_buying department: Full department data + Coupons + Daily Spend
            this.isAdmin = false;
            this.isMediaBuyer = true;  // CAN access daily spend for media buying ops managers
            this.isDepartmentRestricted = false;
            this.canAccessCoupons = true;  // Can access coupons
        } else if (this.role === 'OpsManager' && this.user?.department) {
            // OpsManager with other departments: Full department data + Coupons only
            this.isAdmin = false;
            this.isMediaBuyer = false;  // NO daily spend for other departments
            this.isDepartmentRestricted = false;
            this.canAccessCoupons = true;  // Can access coupons
        } else if (this.role === 'TeamMember' && this.user?.department === 'media_buying') {
            // TeamMember with media_buying: Filtered dashboard + Daily Spend only
            this.isAdmin = false;
            this.isMediaBuyer = true;  // Can see daily spend
            this.isDepartmentRestricted = false;
            this.canAccessCoupons = false;  // NO coupons
        } else if (this.user?.department === 'affiliate' || this.user?.department === 'influencer') {
            // TeamMember with affiliate/influencer: Partner portal access
            this.isAdmin = false;
            this.isMediaBuyer = false;
            this.isDepartmentRestricted = true;
            this.canAccessCoupons = false;
            this.isPartner = true;
        } this.displayRole = this.getDisplayRole();
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
        } else if (url.startsWith('/my-coupons')) {
            this.activeTab = 'my-coupons';
        } else if (url.startsWith('/campaigns')) {
            this.activeTab = 'campaigns';
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
        if (this.role === 'ViewOnly') {
            return 'View Only';
        }

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

    goToMyCoupons(): void {
        this.activeTab = 'my-coupons';
        this.router.navigate(['/my-coupons'], { skipLocationChange: false });
    }

    goToCampaigns(): void {
        this.activeTab = 'campaigns';
        this.router.navigate(['/campaigns'], { skipLocationChange: false });
    }
}