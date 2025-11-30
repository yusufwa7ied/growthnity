# Migration to remove duplicate MediaBuyerDailySpend records before changing unique constraint

from django.db import migrations


def remove_duplicates(apps, schema_editor):
    """
    Remove duplicate MediaBuyerDailySpend records.
    Keep the record with the highest ID (most recent) for each (date, advertiser, partner, platform).
    """
    MediaBuyerDailySpend = apps.get_model('api', 'MediaBuyerDailySpend')
    
    # Find all unique combinations
    unique_combos = MediaBuyerDailySpend.objects.values('date', 'advertiser_id', 'partner_id', 'platform').distinct()
    
    deleted_count = 0
    for combo in unique_combos:
        # Get all records for this combination
        records = MediaBuyerDailySpend.objects.filter(
            date=combo['date'],
            advertiser_id=combo['advertiser_id'],
            partner_id=combo['partner_id'],
            platform=combo['platform']
        ).order_by('-id')  # Order by ID descending (newest first)
        
        # If there are duplicates, delete all except the first (newest)
        if records.count() > 1:
            records_to_delete = list(records)[1:]  # All except first
            for record in records_to_delete:
                deleted_count += 1
                record.delete()
    
    if deleted_count > 0:
        print(f"🗑️  Removed {deleted_count} duplicate MediaBuyerDailySpend records")


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0031_noontransaction'),
    ]

    operations = [
        migrations.RunPython(remove_duplicates, reverse_code=migrations.RunPython.noop),
    ]
