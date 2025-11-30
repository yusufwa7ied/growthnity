# Generated migration to remove coupon from MediaBuyerDailySpend unique constraint

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0030_alter_noon30dayspayout_coupon_and_more'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='mediabuyerdailyspend',
            unique_together={('date', 'advertiser', 'partner', 'platform')},
        ),
    ]
