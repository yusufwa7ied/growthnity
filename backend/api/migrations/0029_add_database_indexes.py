# Generated migration to add database indexes for performance optimization
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0028_alter_departmenttarget_unique_together_and_more'),
    ]

    operations = [
        # Add indexes on CampaignPerformance for frequently filtered fields
        migrations.AddIndex(
            model_name='campaignperformance',
            index=models.Index(fields=['advertiser_id', 'date'], name='api_campaignperf_advertiser_date_idx'),
        ),
        migrations.AddIndex(
            model_name='campaignperformance',
            index=models.Index(fields=['partner_id', 'date'], name='api_campaignperf_partner_date_idx'),
        ),
        migrations.AddIndex(
            model_name='campaignperformance',
            index=models.Index(fields=['coupon_id', 'date'], name='api_campaignperf_coupon_date_idx'),
        ),
        migrations.AddIndex(
            model_name='campaignperformance',
            index=models.Index(fields=['date'], name='api_campaignperf_date_idx'),
        ),
        # Add combined index for date range queries
        migrations.AddIndex(
            model_name='campaignperformance',
            index=models.Index(fields=['advertiser_id', 'partner_id', 'date'], name='api_campaignperf_adv_partner_date_idx'),
        ),
        
        # Add indexes on Coupon for code lookups and advertiser associations
        migrations.AddIndex(
            model_name='coupon',
            index=models.Index(fields=['code'], name='api_coupon_code_idx'),
        ),
        migrations.AddIndex(
            model_name='coupon',
            index=models.Index(fields=['advertiser_id'], name='api_coupon_advertiser_idx'),
        ),
        
        # Add indexes on transaction models for date-based queries
        migrations.AddIndex(
            model_name='drnutritiontransaction',
            index=models.Index(fields=['created_date'], name='api_drnutrition_created_date_idx'),
        ),
        migrations.AddIndex(
            model_name='drnutritiontransaction',
            index=models.Index(fields=['coupon'], name='api_drnutrition_coupon_idx'),
        ),
        migrations.AddIndex(
            model_name='drnutritiontransaction',
            index=models.Index(fields=['advertiser_name'], name='api_drnutrition_advertiser_name_idx'),
        ),
        
        # Add similar indexes for other transaction models (StyliTransaction, SpringRoseTransaction, NoonNamshiTransaction)
        migrations.AddIndex(
            model_name='stylitransaction',
            index=models.Index(fields=['created_date'], name='api_styli_created_date_idx'),
        ),
        migrations.AddIndex(
            model_name='stylitransaction',
            index=models.Index(fields=['coupon'], name='api_styli_coupon_idx'),
        ),
        
        migrations.AddIndex(
            model_name='springrosetransaction',
            index=models.Index(fields=['created_date'], name='api_springrose_created_date_idx'),
        ),
        migrations.AddIndex(
            model_name='springrosetransaction',
            index=models.Index(fields=['coupon'], name='api_springrose_coupon_idx'),
        ),
        
        migrations.AddIndex(
            model_name='noonnamshitransaction',
            index=models.Index(fields=['created_date'], name='api_noonnamshi_created_date_idx'),
        ),
        migrations.AddIndex(
            model_name='noonnamshitransaction',
            index=models.Index(fields=['coupon'], name='api_noonnamshi_coupon_idx'),
        ),
    ]
