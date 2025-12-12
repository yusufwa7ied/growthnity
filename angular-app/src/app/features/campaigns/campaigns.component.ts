import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { Button } from 'primeng/button';
import { Card } from 'primeng/card';
import { Tag } from 'primeng/tag';
import { CampaignSummary, PartnerService } from '../../core/services/partner.service';
import { FooterComponent } from '../../shared/components/footer/footer.component';
import { MainHeaderComponent } from '../../shared/components/main-header/main-header.component';
import { TrimDecimalsPipe } from '../../shared/pipes/trim-decimals.pipe';

@Component({
  selector: 'app-campaigns',
  standalone: true,
  imports: [
    CommonModule,
    Card,
    Button,
    Tag,
    MainHeaderComponent,
    FooterComponent,
    TrimDecimalsPipe
  ],
  templateUrl: './campaigns.component.html',
  styleUrl: './campaigns.component.css'
})
export class CampaignsComponent implements OnInit {
  campaigns: CampaignSummary[] = [];
  workingCampaigns: CampaignSummary[] = [];
  notWorkingCampaigns: CampaignSummary[] = [];
  loading = false;

  constructor(private partnerService: PartnerService) {}

  ngOnInit() {
    this.loadCampaigns();
  }

  loadCampaigns() {
    this.loading = true;
    this.partnerService.getCampaigns().subscribe({
      next: (response) => {
        this.campaigns = response.campaigns;
        this.workingCampaigns = this.campaigns.filter(c => c.is_working);
        this.notWorkingCampaigns = this.campaigns.filter(c => !c.is_working);
        this.loading = false;
      },
      error: (error) => {
        console.error('Error loading campaigns:', error);
        this.loading = false;
      }
    });
  }

  getStatusSeverity(isWorking: boolean): 'success' | 'secondary' {
    return isWorking ? 'success' : 'secondary';
  }
}
