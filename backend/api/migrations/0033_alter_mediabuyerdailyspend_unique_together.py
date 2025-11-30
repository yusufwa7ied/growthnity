# Generated migration to remove coupon from MediaBuyerDailySpend unique constraint

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0032_remove_duplicate_spend_records'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='mediabuyerdailyspend',
            unique_together={('date', 'advertiser', 'partner', 'platform')},
        ),
    ]
