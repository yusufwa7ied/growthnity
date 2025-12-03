import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { ButtonModule } from 'primeng/button';
import { DialogModule } from 'primeng/dialog';
import { TableModule } from 'primeng/table';
import { TooltipModule } from 'primeng/tooltip';
import { Advertiser } from '../../core/models/dashboard.model';
import { DepartmentTarget, TargetWithActuals } from '../../core/models/targets.model';
import { User } from '../../core/models/user.model';
import { AuthService } from '../../core/services/auth.service';
import { DashboardService } from '../../core/services/dashboard.service';
import { TargetsService } from '../../core/services/targets.service';
import { FooterComponent } from '../../shared/components/footer/footer.component';
import { MainHeaderComponent } from '../../shared/components/main-header/main-header.component';
import { SkeletonLoaderComponent } from '../../shared/components/skeleton-loader/skeleton-loader.component';

@Component({
  selector: 'app-targets',
  standalone: true,
  imports: [CommonModule, FormsModule, ButtonModule, DialogModule, TableModule, TooltipModule, MainHeaderComponent, FooterComponent, SkeletonLoaderComponent],
  templateUrl: './targets.component.html',
  styleUrl: './targets.component.css'
})
export class TargetsComponent implements OnInit {
  targets: TargetWithActuals[] = [];
  filteredTargets: TargetWithActuals[] = [];
  advertisers: Advertiser[] = [];
  partners: any[] = [];
  filteredPartners: any[] = [];
  teamMembers: any[] = [];
  filteredTeamMembers: any[] = [];
  user: User | null = null;
  role: string = '';

  loading = false;
  showSkeletons = true;
  showForm = false;
  editingTarget: DepartmentTarget | null = null;
  private skeletonMinDuration = 500; // Minimum ms to show skeleton

  // Filters
  showCurrentMonthOnly = false;
  filterAdvertiser: number | null = null;
  filterDepartment: string | null = null;
  filterMonth: string | null = null;

  advertiserOptions: any[] = [];
  departmentOptions: any[] = [];
  monthOptions: any[] = [];

  // KPI Stats
  stats = {
    totalTargets: 0,
    currentMonth: 0,
    activeAdvertisers: 0,
    departments: 0
  };

  // Form fields
  formData: DepartmentTarget = {
    month: new Date().toISOString().substring(0, 7), // YYYY-MM format
    advertiser: 0,
    partner_type: 'MB',
    assigned_to: null,
    orders_target: 0,
    revenue_target: 0,
    profit_target: 0,
    spend_target: 0
  };

  partnerTypes = [
    { label: 'Media Buyer', value: 'MB' },
    { label: 'Affiliate', value: 'AFF' },
    { label: 'Influencer', value: 'INF' }
  ];

  constructor(
    private targetsService: TargetsService,
    private dashboardService: DashboardService,
    private authService: AuthService,
    private router: Router
  ) { }

  ngOnInit(): void {
    this.user = this.authService.currentUser();
    this.role = this.user?.role || '';
    this.showSkeletons = true;
    this.loadAdvertisers();
    // Don't load team members on init - they'll be loaded when department is selected
    this.loadTargets();
  }

  loadAdvertisers(): void {
    this.dashboardService.getAdvertisers().subscribe(data => {
      this.advertisers = data;
    });
  }

  loadTargets(): void {
    this.loading = true;
    const startTime = Date.now();
    this.targetsService.getTargets().subscribe({
      next: (data) => {
        this.targets = data as TargetWithActuals[];
        this.filteredTargets = [...this.targets];
        this.calculateStats();
        this.buildFilterOptions();
        // Ensure minimum skeleton display duration
        const elapsed = Date.now() - startTime;
        const delay = Math.max(0, this.skeletonMinDuration - elapsed);
        setTimeout(() => {
          this.showSkeletons = false;
          this.loading = false;
        }, delay);
      },
      error: () => {
        const elapsed = Date.now() - startTime;
        const delay = Math.max(0, this.skeletonMinDuration - elapsed);
        setTimeout(() => {
          this.showSkeletons = false;
          this.loading = false;
        }, delay);
      }
    });
  }

  calculateStats(): void {
    this.stats.totalTargets = this.targets.length;

    const currentMonth = new Date().toISOString().substring(0, 7);
    this.stats.currentMonth = this.targets.filter(t => t.month.startsWith(currentMonth)).length;

    const uniqueAdvertisers = new Set(this.targets.map(t => t.advertiser));
    this.stats.activeAdvertisers = uniqueAdvertisers.size;

    const uniqueDepartments = new Set(this.targets.map(t => t.partner_type));
    this.stats.departments = uniqueDepartments.size;
  }

  buildFilterOptions(): void {
    const uniqueAdvertisers = [...new Set(this.targets.map(t => t.advertiser))];
    this.advertiserOptions = uniqueAdvertisers.map(id => {
      const adv = this.advertisers.find(a => a.id === id);
      return { label: adv?.name || 'Unknown', value: id };
    });

    const uniqueDepartments = [...new Set(this.targets.map(t => t.partner_type))];
    this.departmentOptions = this.partnerTypes.filter(pt => uniqueDepartments.includes(pt.value as any));

    const uniqueMonths = [...new Set(this.targets.map(t => t.month.substring(0, 7)))];
    this.monthOptions = uniqueMonths.sort().reverse().map(m => {
      const date = new Date(m + '-01');
      return {
        label: date.toLocaleDateString('en-US', { month: 'long', year: 'numeric' }),
        value: m
      };
    });
  }

  toggleCurrentMonthFilter(): void {
    this.showCurrentMonthOnly = !this.showCurrentMonthOnly;
    this.applyFilters();
  }

  applyFilters(): void {
    this.filteredTargets = this.targets.filter(target => {
      const currentMonth = new Date().toISOString().substring(0, 7);
      if (this.showCurrentMonthOnly && !target.month.startsWith(currentMonth)) return false;
      if (this.filterAdvertiser && target.advertiser !== Number(this.filterAdvertiser)) return false;
      if (this.filterDepartment && target.partner_type !== this.filterDepartment) return false;
      if (this.filterMonth && !target.month.startsWith(this.filterMonth)) return false;
      return true;
    });
  }

  clearFilters(): void {
    this.showCurrentMonthOnly = false;
    this.filterAdvertiser = null;
    this.filterDepartment = null;
    this.filterMonth = null;
    this.filteredTargets = [...this.targets];
  }

  logout(): void {
    this.authService.logout();
    this.router.navigate(['/login']);
  }

  goToDashboard(): void {
    this.router.navigate(['/dashboard']);
  }

  goToCoupons(): void {
    this.router.navigate(['/coupons']);
  }

  goToAdvertisers(): void {
    this.router.navigate(['/advertisers']);
  }

  goToTargets(): void {
    // Already on targets page
  }

  toggleForm(): void {
    this.showForm = !this.showForm;
    if (!this.showForm) {
      this.editingTarget = null;
      this.resetForm();
    }
  }

  editTarget(target: DepartmentTarget): void {
    this.editingTarget = target;
    this.formData = { ...target };
    // Convert YYYY-MM-DD to YYYY-MM for month input
    this.formData.month = target.month.substring(0, 7);
    // Ensure spend_target is not null for calculations
    if (this.formData.spend_target === null || this.formData.spend_target === undefined) {
      this.formData.spend_target = 0;
    }
    this.calculateProfit();
    
    // Load team members for the department when editing
    if (this.formData.partner_type) {
      this.loadTeamMembers(this.formData.partner_type);
    }
    
    this.showForm = true;
  }

  loadTeamMembers(partnerType: string): void {
    if (!partnerType) {
      this.teamMembers = [];
      return;
    }

    this.dashboardService.getTeamMembersByDepartment(partnerType).subscribe({
      next: (data) => {
        // Transform team members data for dropdown
        this.teamMembers = data.map((m: any) => ({
          id: m.id,
          name: m.name,
          username: m.username
        }));
      },
      error: (err) => {
        console.error('Error loading team members for department:', err);
        this.teamMembers = [];
      }
    });
  }

  onPartnerTypeChange(partnerType: string): void {
    // Reset member selection when type changes
    this.formData.assigned_to = null;
    
    // Load team members for the selected department
    this.loadTeamMembers(partnerType);
  }

  deleteTarget(id: number): void {
    if (confirm('Are you sure you want to delete this target?')) {
      this.targetsService.deleteTarget(id).subscribe({
        next: () => {
          this.loadTargets();
        },
        error: (err) => console.error('Error deleting target:', err)
      });
    }
  }

  saveTarget(): void {
    // Ensure month is in YYYY-MM-01 format for backend
    const monthValue = this.formData.month.includes('-01')
      ? this.formData.month
      : `${this.formData.month}-01`;

    const dataToSave = { ...this.formData, month: monthValue };

    if (this.editingTarget?.id) {
      // Update existing target
      this.targetsService.updateTarget(this.editingTarget.id, dataToSave).subscribe({
        next: () => {
          this.loadTargets();
          this.toggleForm();
        },
        error: (err) => console.error('Error updating target:', err)
      });
    } else {
      // Create new target
      this.targetsService.createTarget(dataToSave).subscribe({
        next: () => {
          this.loadTargets();
          this.toggleForm();
        },
        error: (err) => console.error('Error creating target:', err)
      });
    }
  }

  calculateProfit(): void {
    const revenue = this.formData.revenue_target || 0;
    const spend = this.formData.spend_target || 0;
    this.formData.profit_target = revenue - spend;
  }

  onRevenueChange(): void {
    this.calculateProfit();
  }

  onSpendChange(): void {
    this.calculateProfit();
  }

  resetForm(): void {
    this.formData = {
      month: new Date().toISOString().substring(0, 7), // YYYY-MM format
      advertiser: 0,
      partner_type: 'MB',
      assigned_to: null,
      orders_target: 0,
      revenue_target: 0,
      profit_target: 0,
      spend_target: 0
    };
  }
}
