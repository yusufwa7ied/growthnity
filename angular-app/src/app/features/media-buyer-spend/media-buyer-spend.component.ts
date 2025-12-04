import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { ButtonModule } from 'primeng/button';
import { Card } from 'primeng/card';
import { DatePicker } from 'primeng/datepicker';
import { InputNumber } from 'primeng/inputnumber';
import { Select } from 'primeng/select';
import { TableModule } from 'primeng/table';
import { AdvertiserService } from '../../core/services/advertiser.service';
import { MediaBuyerSpend, MediaBuyerSpendService, Platform } from '../../core/services/media-buyer-spend.service';
import { FooterComponent } from '../../shared/components/footer/footer.component';
import { MainHeaderComponent } from '../../shared/components/main-header/main-header.component';
import { SkeletonLoaderComponent } from '../../shared/components/skeleton-loader/skeleton-loader.component';

@Component({
  selector: 'app-media-buyer-spend',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    TableModule,
    ButtonModule,
    DatePicker,
    Select,
    InputNumber,
    Card,
    MainHeaderComponent,
    FooterComponent,
    SkeletonLoaderComponent
  ],
  templateUrl: './media-buyer-spend.component.html',
  styleUrl: './media-buyer-spend.component.css'
})
export class MediaBuyerSpendComponent implements OnInit {
  user: any = null;
  role: string = '';

  allSpendRecords: MediaBuyerSpend[] = [];
  spendRecords: MediaBuyerSpend[] = [];
  allAdvertisers: any[] = []; // All advertisers for form
  advertisers: any[] = []; // Filtered advertisers for filter dropdown
  partners: any[] = [];

  // Platform options matching backend choices
  platformOptions: Platform[] = [
    { label: 'Meta', value: 'Meta' },
    { label: 'Snapchat', value: 'Snapchat' },
    { label: 'TikTok', value: 'TikTok' },
    { label: 'Google', value: 'Google' },
    { label: 'Twitter', value: 'Twitter' },
    { label: 'LinkedIn', value: 'LinkedIn' },
    { label: 'YouTube', value: 'YouTube' },
    { label: 'Other', value: 'Other' }
  ];

  // Form fields
  selectedDateRange: Date[] | null = null;
  selectedAdvertiser: any = null;
  selectedPartner: any = null;
  selectedPlatform: any = this.platformOptions[0]; // Default to Meta
  amountSpent: number = 0;
  selectedCurrency: string = 'USD';

  // Filter fields
  filterDateRange: Date[] | null = null;
  filterAdvertiser: any = null;
  filterPartner: any = null;
  filterPlatform: any = null;

  // Analytics data
  analytics: any = null;

  loading = false;
  saving = false;
  showSkeletons = true;
  showAddForm = false;
  private skeletonMinDuration = 500; // Minimum ms to show skeleton

  constructor(
    private spendService: MediaBuyerSpendService,
    private advertiserService: AdvertiserService,
    private router: Router
  ) { }

  ngOnInit() {
    const userStr = localStorage.getItem('logged_user');
    if (userStr) {
      this.user = JSON.parse(userStr);
      this.role = this.user.role || '';
    }
    this.loadAdvertisers();
    this.loadPartners();
    this.loadSpendRecords();
    this.loadAnalytics();
  }

  loadAdvertisers() {
    this.advertiserService.getAdvertisers().subscribe({
      next: (data) => {
        this.allAdvertisers = data.map(a => ({ label: a.name, value: a }));
        // Use the same advertisers for filtering
        this.advertisers = data.map(a => ({ label: a.name, value: a }));
      },
      error: (err) => console.error('Error loading advertisers:', err)
    });
  }

  loadPartners() {
    this.advertiserService.getPartners().subscribe({
      next: (data) => {
        // Filter only media buyers
        const mediaBuyers = data.filter((p: any) => p.type === 'Media Buying');
        this.partners = mediaBuyers.map(p => ({ label: p.name, value: p }));

        // Auto-select partner ONLY for TeamMembers who have a partner_id
        // OpsManagers and Admins should manually select from dropdown
        if (this.role !== 'OpsManager' && this.role !== 'Admin' && this.user && this.user.partner_id && this.partners.length > 0) {
          const userPartner = this.partners.find(p => p.value.id === this.user.partner_id);
          if (userPartner) {
            this.selectedPartner = userPartner;
          }
        }
      },
      error: (err) => console.error('Error loading partners:', err)
    });
  }

  loadSpendRecords() {
    this.loading = true;
    this.showSkeletons = true;
    const startTime = Date.now();

    this.spendService.getSpendRecords().subscribe({
      next: (data) => {
        this.allSpendRecords = data;
        this.applyFilters();
        // Ensure minimum skeleton display duration
        const elapsed = Date.now() - startTime;
        const delay = Math.max(0, this.skeletonMinDuration - elapsed);
        setTimeout(() => {
          this.showSkeletons = false;
          this.loading = false;
        }, delay);
      },
      error: (err) => {
        console.error('Error loading spend records:', err);
        const elapsed = Date.now() - startTime;
        const delay = Math.max(0, this.skeletonMinDuration - elapsed);
        setTimeout(() => {
          this.showSkeletons = false;
          this.loading = false;
        }, delay);
      }
    });
  }

  saveSpendRecord() {
    if (!this.selectedDateRange || !this.selectedDateRange[0] || !this.selectedAdvertiser || !this.amountSpent) {
      alert('Please fill all required fields (Date, Advertiser, Amount)');
      return;
    }

    if (!this.selectedPartner) {
      if (this.role === 'OpsManager' || this.role === 'Admin') {
        alert('Please select a partner from the dropdown.');
      } else {
        alert('Partner information is missing. Your user account may not be associated with a partner.');
      }
      return;
    }

    if (!this.selectedPlatform) {
      alert('Please select a platform');
      return;
    }

    this.saving = true;

    // Calculate date range
    const startDate = this.selectedDateRange[0];
    const endDate = this.selectedDateRange[1] || startDate;
    const dates = this.getDatesBetween(startDate, endDate);
    const dailyAmount = this.amountSpent / dates.length;

    // Create spend records sequentially to avoid database lock issues
    this.createRecordsSequentially(dates, dailyAmount, 0, 0, 0);
  }

  createRecordsSequentially(dates: Date[], dailyAmount: number, index: number, completed: number, errors: number) {
    if (index >= dates.length) {
      // All records processed
      this.saving = false;
      if (errors === 0) {
        alert(`Successfully created ${completed} daily spend records`);
        this.resetForm();
        this.loadSpendRecords();
      } else {
        alert(`Created ${completed} records with ${errors} errors.`);
        this.loadSpendRecords();
      }
      return;
    }

    const date = dates[index];
    const spendData: MediaBuyerSpend = {
      date: this.formatDate(date),
      advertiser_id: this.selectedAdvertiser.value.id,
      partner_id: this.selectedPartner.value.id,
      platform: this.selectedPlatform.value,
      amount_spent: dailyAmount,
      currency: this.selectedCurrency
    };

    this.spendService.createSpendRecord(spendData).subscribe({
      next: () => {
        // Move to next record
        this.createRecordsSequentially(dates, dailyAmount, index + 1, completed + 1, errors);
      },
      error: (err) => {
        console.error('Error saving spend record for date', this.formatDate(date), ':', err.error);
        // Move to next record even if this one failed
        this.createRecordsSequentially(dates, dailyAmount, index + 1, completed, errors + 1);
      }
    });
  }

  getDatesBetween(startDate: Date, endDate: Date): Date[] {
    const dates: Date[] = [];
    const current = new Date(startDate);
    const end = new Date(endDate);

    while (current <= end) {
      dates.push(new Date(current));
      current.setDate(current.getDate() + 1);
    }

    return dates;
  }

  deleteSpendRecord(id: number) {
    if (confirm('Are you sure you want to delete this spend record?')) {
      this.spendService.deleteSpendRecord(id).subscribe({
        next: () => {
          alert('Spend record deleted successfully');
          this.loadSpendRecords();
        },
        error: (err) => {
          console.error('Error deleting spend record:', err);
          alert('Error deleting spend record');
        }
      });
    }
  }

  resetForm() {
    this.selectedDateRange = null;
    this.selectedAdvertiser = null;
    this.selectedPlatform = this.platformOptions[0]; // Reset to Meta
    // Keep partner as it's auto-selected from user
    this.amountSpent = 0;
    this.selectedCurrency = 'USD';
  }

  loadAnalytics() {
    const filters: any = {};
    
    if (this.filterDateRange && this.filterDateRange.length > 0) {
      filters.date_from = this.formatDate(this.filterDateRange[0]);
      if (this.filterDateRange[1]) {
        filters.date_to = this.formatDate(this.filterDateRange[1]);
      }
    }

    if (this.filterAdvertiser && this.filterAdvertiser.value) {
      filters.advertiser_id = this.filterAdvertiser.value.id;
    }

    if (this.filterPartner && this.filterPartner.value) {
      filters.partner_id = this.filterPartner.value.id;
    }

    if (this.filterPlatform && this.filterPlatform.value) {
      filters.platform = this.filterPlatform.value;
    }

    this.spendService.getSpendAnalytics(filters).subscribe({
      next: (data) => {
        this.analytics = data;
      },
      error: (err) => {
        console.error('Error loading analytics:', err);
      }
    });
  }

  applyFilters() {
    let filtered = [...this.allSpendRecords];

    // Filter by date range
    if (this.filterDateRange && this.filterDateRange.length > 0) {
      const fromDate = this.formatDate(this.filterDateRange[0]);
      filtered = filtered.filter(r => r.date >= fromDate);

      if (this.filterDateRange[1]) {
        const toDate = this.formatDate(this.filterDateRange[1]);
        filtered = filtered.filter(r => r.date <= toDate);
      }
    }

    // Filter by advertiser
    if (this.filterAdvertiser && this.filterAdvertiser.value) {
      filtered = filtered.filter(r => r.advertiser_id === this.filterAdvertiser.value.id);
    }

    // Filter by partner
    if (this.filterPartner && this.filterPartner.value) {
      filtered = filtered.filter(r => r.partner_id === this.filterPartner.value.id);
    }

    // Filter by platform
    if (this.filterPlatform && this.filterPlatform.value) {
      filtered = filtered.filter(r => r.platform === this.filterPlatform.value);
    }

    this.spendRecords = filtered;
    
    // Reload analytics with new filters
    this.loadAnalytics();
  }

  clearFilters() {
    this.filterDateRange = null;
    this.filterAdvertiser = null;
    this.filterPartner = null;
    this.filterPlatform = null;
    this.spendRecords = [...this.allSpendRecords];
    this.loadAnalytics();
  }

  formatDate(date: Date): string {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  getTotalSpend(): number {
    return this.spendRecords.reduce((sum, record) => sum + record.amount_spent, 0);
  }

  getUniqueAdvertisers(): number {
    const unique = new Set(this.spendRecords.map(r => r.advertiser_id));
    return unique.size;
  }

  getUniquePartners(): number {
    const unique = new Set(this.spendRecords.map(r => r.partner_id));
    return unique.size;
  }

  logout(): void {
    localStorage.removeItem('access_token');
    localStorage.removeItem('user');
    this.router.navigate(['/login']);
  }

  toggleAddForm(): void {
    this.showAddForm = !this.showAddForm;
  }
}
