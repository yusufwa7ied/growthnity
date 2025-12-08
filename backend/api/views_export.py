"""
Export views for generating detailed CSV reports
"""
import csv
from datetime import datetime
from django.http import HttpResponse
from django.db.models import Sum, Q
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status

from .models import (
    CampaignPerformance,
    CompanyUser,
    AccountAssignment,
    MediaBuyerDailySpend,
    Advertiser,
    Partner,
)


def format_advertiser_name(name, geo):
    """Format advertiser name with geo for Noon"""
    if name == "Noon" and geo:
        return f"Noon {geo.upper()}" if geo.lower() == "gcc" else f"Noon {geo.title()}"
    return name


def apply_team_member_filter(qs, team_member_ids):
    """Apply team member filtering to queryset"""
    if team_member_ids:
        from django.db.models import Q
        filter_q = Q()
        for tm_id in team_member_ids:
            try:
                tm_id_int = int(tm_id)
                assignments = AccountAssignment.objects.filter(
                    company_user_id=tm_id_int
                ).prefetch_related("advertisers", "partners")
                
                for assignment in assignments:
                    adv_ids = list(assignment.advertisers.values_list("id", flat=True))
                    partner_ids = list(assignment.partners.values_list("id", flat=True))
                    
                    if adv_ids and partner_ids:
                        filter_q |= Q(advertiser_id__in=adv_ids, partner_id__in=partner_ids)
                    elif adv_ids:
                        filter_q |= Q(advertiser_id__in=adv_ids)
                    elif partner_ids:
                        filter_q |= Q(partner_id__in=partner_ids)
            except (ValueError, TypeError):
                continue
        
        if filter_q:
            qs = qs.filter(filter_q)
    
    return qs


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def export_performance_report(request):
    """
    Export detailed performance report as CSV with summary statistics.
    Respects all filters and role-based permissions.
    """
    user = request.user
    company_user = CompanyUser.objects.select_related("role").filter(user=user).first()
    
    if not company_user:
        if not user.is_superuser:
            return Response({"detail": "Access denied"}, status=status.HTTP_403_FORBIDDEN)
        role = "Admin"
        department = None
    else:
        role = company_user.role.name if company_user.role else None
        department = company_user.department

    # Base queryset
    qs = CampaignPerformance.objects.all()

    # Get filters from request
    advertiser_ids = request.GET.getlist("advertiser_id")
    partner_ids = request.GET.getlist("partner_id")
    coupon_codes = request.GET.getlist("coupon_code")
    geos = request.GET.getlist("geo")
    partner_type = request.GET.get("partner_type")
    team_member_ids = request.GET.getlist("team_member_id")
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")

    # Track applied filters for summary
    applied_filters = []
    
    # Department scoping for OpsManager
    if company_user and department and role == "OpsManager" and not partner_type:
        if department == "media_buying":
            qs = qs.filter(partner__partner_type="MB")
            applied_filters.append(f"Department: Media Buying")
        elif department == "affiliate":
            qs = qs.filter(partner__partner_type="AFF")
            applied_filters.append(f"Department: Affiliate")
        elif department == "influencer":
            qs = qs.filter(partner__partner_type="INF")
            applied_filters.append(f"Department: Influencer")

    # Apply team member filter
    qs = apply_team_member_filter(qs, team_member_ids)
    if team_member_ids:
        applied_filters.append(f"Team Members: {len(team_member_ids)} selected")

    # Apply other filters
    if advertiser_ids:
        qs = qs.filter(advertiser_id__in=advertiser_ids)
        # Get advertiser names
        advertisers = Advertiser.objects.filter(id__in=advertiser_ids).values_list('name', flat=True)
        advertiser_names = ', '.join(advertisers)
        applied_filters.append(f"Advertisers: {advertiser_names}")
    
    if geos:
        qs = qs.filter(geo__in=geos)
        applied_filters.append(f"Regions: {', '.join(geos)}")
    
    if partner_ids:
        qs = qs.filter(partner_id__in=partner_ids)
        applied_filters.append(f"Partners: {len(partner_ids)} selected")
    
    if partner_type:
        qs = qs.filter(partner__partner_type=partner_type)
        applied_filters.append(f"Partner Type: {partner_type}")
    
    if coupon_codes:
        qs = qs.filter(coupon__code__in=coupon_codes)
        applied_filters.append(f"Coupons: {len(coupon_codes)} selected")
    
    if date_from:
        qs = qs.filter(date__gte=date_from)
        applied_filters.append(f"From: {date_from}")
    
    if date_to:
        qs = qs.filter(date__lte=date_to)
        applied_filters.append(f"To: {date_to}")

    # Role-based access control
    full_access_roles = {"Admin", "OpsManager"}
    has_full_access = role in full_access_roles
    can_see_profit = has_full_access or (role == "TeamMember" and department == "media_buying")

    if not has_full_access:
        assignments = AccountAssignment.objects.filter(company_user=company_user)
        advertiser_ids_allowed = set()
        partner_ids_allowed = set()

        for a in assignments:
            advertiser_ids_allowed.update(a.advertisers.values_list("id", flat=True))
            partner_ids_allowed.update(a.partners.values_list("id", flat=True))

        if not advertiser_ids_allowed and not partner_ids_allowed:
            qs = qs.none()
        else:
            if advertiser_ids_allowed:
                qs = qs.filter(advertiser_id__in=list(advertiser_ids_allowed))
            if partner_ids_allowed:
                qs = qs.filter(partner_id__in=list(partner_ids_allowed))

    # Fetch data with related fields
    data = qs.select_related('advertiser', 'partner', 'coupon').order_by('-date')

    # Calculate summary statistics
    summary_stats = calculate_summary_statistics(qs, has_full_access)

    # Create CSV response
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = f'attachment; filename="performance_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"'
    
    writer = csv.writer(response)
    
    # Write summary header
    write_summary_section(writer, summary_stats, applied_filters, user, role, can_see_profit)
    
    # Write detailed data
    write_detailed_data(writer, data, has_full_access, can_see_profit)
    
    return response


def calculate_summary_statistics(qs, has_full_access):
    """Calculate summary statistics from queryset"""
    stats = qs.aggregate(
        total_orders=Sum('total_orders'),
        ftu_orders=Sum('ftu_orders'),
        rtu_orders=Sum('rtu_orders'),
        total_sales=Sum('total_sales'),
        ftu_sales=Sum('ftu_sales'),
        rtu_sales=Sum('rtu_sales'),
        total_revenue=Sum('total_revenue'),
        ftu_revenue=Sum('ftu_revenue'),
        rtu_revenue=Sum('rtu_revenue'),
        total_payout=Sum('total_payout'),
        ftu_payout=Sum('ftu_payout'),
        rtu_payout=Sum('rtu_payout'),
    )
    
    # Calculate MB spend if needed
    if has_full_access:
        mb_qs = qs.filter(partner__partner_type="MB")
        if mb_qs.exists():
            spend_keys = mb_qs.values_list('date', 'advertiser_id', 'partner_id').distinct()
            spend_conditions = Q()
            for date, adv_id, part_id in spend_keys:
                spend_conditions |= Q(date=date, advertiser_id=adv_id, partner_id=part_id)
            
            if spend_conditions:
                mb_spend_agg = MediaBuyerDailySpend.objects.filter(spend_conditions).aggregate(
                    total=Sum('amount_spent')
                )
                mb_spend = float(mb_spend_agg['total'] or 0)
            else:
                mb_spend = 0
        else:
            mb_spend = 0
        
        # Get non-MB payout
        non_mb_agg = qs.exclude(partner__partner_type="MB").aggregate(
            total=Sum('total_payout')
        )
        non_mb_payout = float(non_mb_agg['total'] or 0)
        
        stats['total_cost'] = mb_spend + non_mb_payout
        stats['total_profit'] = float(stats['total_revenue'] or 0) - stats['total_cost']
    
    return stats


def write_summary_section(writer, stats, filters, user, role, can_see_profit):
    """Write summary section at top of CSV"""
    writer.writerow(['PERFORMANCE SUMMARY REPORT'])
    writer.writerow(['Export Date:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
    writer.writerow(['Generated By:', f'{user.username} ({role})'])
    writer.writerow([])
    
    if filters:
        writer.writerow(['FILTERS APPLIED:'])
        for f in filters:
            writer.writerow(['', f])
        writer.writerow([])
    
    writer.writerow(['SUMMARY METRICS:'])
    writer.writerow(['Total Orders:', f"{stats['total_orders'] or 0:,}"])
    writer.writerow(['', f"FTU Orders: {stats['ftu_orders'] or 0:,}"])
    writer.writerow(['', f"RTU Orders: {stats['rtu_orders'] or 0:,}"])
    writer.writerow([])
    
    writer.writerow(['Total Sales (GMV):', f"${stats['total_sales'] or 0:,.2f}"])
    writer.writerow(['', f"FTU Sales: ${stats['ftu_sales'] or 0:,.2f}"])
    writer.writerow(['', f"RTU Sales: ${stats['rtu_sales'] or 0:,.2f}"])
    writer.writerow([])
    
    if can_see_profit:
        writer.writerow(['Total Revenue:', f"${stats['total_revenue'] or 0:,.2f}"])
        writer.writerow(['', f"FTU Revenue: ${stats['ftu_revenue'] or 0:,.2f}"])
        writer.writerow(['', f"RTU Revenue: ${stats['rtu_revenue'] or 0:,.2f}"])
        writer.writerow([])
        
        writer.writerow(['Total Cost:', f"${stats.get('total_cost', 0):,.2f}"])
        writer.writerow(['Total Profit:', f"${stats.get('total_profit', 0):,.2f}"])
    
    writer.writerow([])
    writer.writerow([])
    writer.writerow(['DETAILED DATA:'])
    writer.writerow([])


def write_detailed_data(writer, data, has_full_access, can_see_profit):
    """Write detailed performance data rows"""
    # Write header
    headers = [
        'Date', 'Campaign', 'Geo', 'Coupon', 'Partner', 'Partner Type',
        'Total Orders', 'FTU Orders', 'RTU Orders',
        'Total Sales', 'FTU Sales', 'RTU Sales',
    ]
    
    if has_full_access or can_see_profit:
        headers.extend([
            'Total Revenue', 'FTU Revenue', 'RTU Revenue',
            'Total Cost', 'FTU Payout', 'RTU Payout',
        ])
        if can_see_profit:
            headers.append('Profit')
    else:
        headers.append('Payout')
    
    writer.writerow(headers)
    
    # Write data rows
    for record in data:
        campaign_name = format_advertiser_name(
            record.advertiser.name if record.advertiser else 'N/A',
            record.geo
        )
        
        row = [
            record.date,
            campaign_name,
            record.geo or '',
            record.coupon.code if record.coupon else '',
            record.partner.name if record.partner else '',
            record.partner.partner_type if record.partner else '',
            record.total_orders,
            record.ftu_orders,
            record.rtu_orders,
            f"{float(record.total_sales):.2f}",
            f"{float(record.ftu_sales):.2f}",
            f"{float(record.rtu_sales):.2f}",
        ]
        
        if has_full_access or can_see_profit:
            revenue = float(record.total_revenue)
            payout = float(record.total_payout)
            
            row.extend([
                f"{float(record.total_revenue):.2f}",
                f"{float(record.ftu_revenue):.2f}",
                f"{float(record.rtu_revenue):.2f}",
                f"{payout:.2f}",
                f"{float(record.ftu_payout):.2f}",
                f"{float(record.rtu_payout):.2f}",
            ])
            
            if can_see_profit:
                profit = revenue - payout
                row.append(f"{profit:.2f}")
        else:
            row.append(f"{float(record.total_payout):.2f}")
        
        writer.writerow(row)
