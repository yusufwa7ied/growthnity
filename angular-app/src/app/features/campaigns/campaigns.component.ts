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
    requestingCoupon: { [key: number]: boolean } = {};

    constructor(private partnerService: PartnerService) { }

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

    requestCoupon(advertiserId: number) {
        if (this.requestingCoupon[advertiserId]) return;

        this.requestingCoupon[advertiserId] = true;
        this.partnerService.requestCoupon(advertiserId).subscribe({
            next: (response) => {
                alert('Coupon request submitted successfully! Our team will review it shortly.');
                this.requestingCoupon[advertiserId] = false;
            },
            error: (error) => {
                console.error('Error requesting coupon:', error);
                const message = error.error?.detail || 'Failed to submit request. Please try again.';
                alert(message);
                this.requestingCoupon[advertiserId] = false;
            }
        });
    }

    getStatusSeverity(isWorking: boolean): 'success' | 'secondary' {
        return isWorking ? 'success' : 'secondary';
    }

    formatRate(campaign: CampaignSummary): string {
        if (!campaign.payout_rate_type) return '';
        
        const ftu = campaign.ftu_payout ? `FTU: ${campaign.ftu_payout}${campaign.payout_rate_type === 'percent' ? '%' : ' ' + campaign.currency}` : '';
        const rtu = campaign.rtu_payout ? `RTU: ${campaign.rtu_payout}${campaign.payout_rate_type === 'percent' ? '%' : ' ' + campaign.currency}` : '';
        
        return [ftu, rtu].filter(Boolean).join(' | ');
    }
}
