# Generated manually to recreate RDELTransaction table

from django.db import migrations, models
import django.db.models.deletion
import uuid


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0036_drop_rdeltransaction'),
    ]

    operations = [
        migrations.CreateModel(
            name='RDELTransaction',
            fields=[
                ('uuid', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('order_id', models.CharField(max_length=200)),
                ('created_date', models.DateTimeField()),
                ('country', models.CharField(max_length=10)),
                ('coupon', models.CharField(max_length=50)),
                ('user_type', models.CharField(default='RTU', max_length=10)),
                ('partner_name', models.CharField(blank=True, max_length=150, null=True)),
                ('advertiser_name', models.CharField(max_length=150)),
                ('order_count', models.IntegerField(default=1)),
                ('currency', models.CharField(default='SAR', max_length=10)),
                ('rate_type', models.CharField(default='percent', max_length=20)),
                ('sales', models.DecimalField(decimal_places=4, default=0, max_digits=12)),
                ('commission', models.DecimalField(decimal_places=4, default=0, max_digits=12)),
                ('our_rev', models.DecimalField(decimal_places=4, default=0, max_digits=12)),
                ('ftu_rate', models.DecimalField(decimal_places=4, default=0, max_digits=12)),
                ('rtu_rate', models.DecimalField(decimal_places=4, default=0, max_digits=12)),
                ('ftu_fixed_bonus', models.DecimalField(decimal_places=4, default=0, max_digits=12)),
                ('rtu_fixed_bonus', models.DecimalField(decimal_places=4, default=0, max_digits=12)),
                ('payout', models.DecimalField(decimal_places=4, default=0, max_digits=12)),
                ('our_rev_usd', models.DecimalField(decimal_places=4, default=0, max_digits=12)),
                ('payout_usd', models.DecimalField(decimal_places=4, default=0, max_digits=12)),
                ('profit_usd', models.DecimalField(decimal_places=4, default=0, max_digits=12)),
                ('advertiser', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='api.advertiser')),
                ('partner', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='noonnamshi_transactions', to='api.partner')),
            ],
            options={
                'ordering': ['-created_date'],
            },
        ),
    ]
