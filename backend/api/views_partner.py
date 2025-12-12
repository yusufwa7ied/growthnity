from django.http import JsonResponse
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from django.db.models import Sum, Count, Q
from datetime import datetime, timedelta
from decimal import Decimal

from .models import (
    CompanyUser, Partner, CampaignPerformance, 
    Coupon, Advertiser, AccountAssignment, AdvertiserCancellationRate
)
# Force rebuild


def get_cancellation_rate_for_date(advertiser_id, target_date):
    """
    Get the applicable cancellation rate for an advertiser on a specific date.
    Returns the cancellation rate as a Decimal, or 0 if no rate is found.
    """
    rate = AdvertiserCancellationRate.objects.filter(
        advertiser_id=advertiser_id,
        start_date__lte=target_date
    ).filter(
        Q(end_date__gte=target_date) | Q(end_date__isnull=True)
    ).order_by('-start_date').first()
    
    if rate:
        return rate.cancellation_rate
    return Decimal('0')


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def partner_coupons_performance_view(request):
    """
    Get coupon performance data for the logged-in partner (TeamMember with affiliate/influencer department)
    Grouped by advertiser/campaign and coupon
    """
    user = request.user
    
    try:
        company_user = CompanyUser.objects.get(user=user)
    except CompanyUser.DoesNotExist:
        return Response({"detail": "User not found"}, status=status.HTTP_404_NOT_FOUND)
    
    # Check if user is a partner (TeamMember with affiliate/influencer department)
    if company_user.role.name != 'TeamMember' or company_user.department not in ['affiliate', 'influencer']:
        return Response(
            {"detail": "Access denied. This page is only for partners."},
            status=status.HTTP_403_FORBIDDEN
        )
    
    # Get the partner(s) assigned to this user
    assignment = company_user.accountassignment_set.first()
    if not assignment:
        return Response({"detail": "No partner assignment found"}, status=status.HTTP_404_NOT_FOUND)
    
    user_partners = assignment.partners.all()
    if not user_partners.exists():
        return Response({"detail": "No partners assigned"}, status=status.HTTP_404_NOT_FOUND)
    
    # Get date filters
    date_from_str = request.GET.get('date_from')
    date_to_str = request.GET.get('date_to')
    advertiser_id = request.GET.get('advertiser_id')
    search = request.GET.get('search', '').lower()
    
    # Default to current month if no dates provided
    if not date_from_str or not date_to_str:
        today = datetime.now()
        date_from = today.replace(day=1)
        if today.month == 12:
            date_to = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            date_to = today.replace(month=today.month + 1, day=1) - timedelta(days=1)
    else:
        try:
            date_from = datetime.strptime(date_from_str, '%Y-%m-%d')
            date_to = datetime.strptime(date_to_str, '%Y-%m-%d')
        except ValueError:
            return Response({"detail": "Invalid date format. Use YYYY-MM-DD"}, status=status.HTTP_400_BAD_REQUEST)
    
    # Build query filters
    filters = {
        'partner__in': user_partners,
        'date__gte': date_from,
        'date__lte': date_to
    }
    
    if advertiser_id:
        filters['advertiser_id'] = advertiser_id
    
    # Get performance data
    performance_qs = CampaignPerformance.objects.filter(**filters).select_related(
        'advertiser', 'partner', 'coupon'
    )
    
    # Group by coupon only
    grouped_data = {}
    has_any_cancellation_rate = False
    
    for record in performance_qs:
        coupon_code = record.coupon.code if record.coupon else 'No Coupon'
        
        # Get first advertiser name for display
        advertiser_name = record.advertiser.name if record.advertiser else 'Unknown'
        
        if coupon_code not in grouped_data:
            grouped_data[coupon_code] = {
                'advertiser': advertiser_name,
                'advertiser_id': record.advertiser.id if record.advertiser else None,
                'coupon': coupon_code,
                'coupon_id': record.coupon.id if record.coupon else None,
                'total_orders': 0,
                'total_sales': 0,
                'total_revenue': 0,
                'total_payout_gross': 0,
                'total_payout_net': 0,
                'ftu_orders': 0,
                'rtu_orders': 0,
                'ftu_sales': 0,
                'rtu_sales': 0,
                'has_cancellation_rate': False
            }
        
        # Aggregate metrics
        grouped_data[coupon_code]['total_orders'] += record.total_orders or 0
        grouped_data[coupon_code]['total_sales'] += float(record.total_sales or 0)
        grouped_data[coupon_code]['total_revenue'] += float(record.total_revenue or 0)
        grouped_data[coupon_code]['total_payout_gross'] += float(record.total_payout or 0)
        grouped_data[coupon_code]['ftu_orders'] += record.ftu_orders or 0
        grouped_data[coupon_code]['rtu_orders'] += record.rtu_orders or 0
        grouped_data[coupon_code]['ftu_sales'] += float(record.ftu_sales or 0)
        grouped_data[coupon_code]['rtu_sales'] += float(record.rtu_sales or 0)
        
        # Calculate net payout using date-specific cancellation rate
        payout_amount = float(record.total_payout or 0)
        try:
            if record.advertiser:
                cancellation_rate = get_cancellation_rate_for_date(record.advertiser.id, record.date)
                if cancellation_rate > 0:
                    has_any_cancellation_rate = True
                    grouped_data[coupon_code]['has_cancellation_rate'] = True
                    net_payout = payout_amount * float(Decimal('1') - (cancellation_rate / Decimal('100')))
                    grouped_data[coupon_code]['total_payout_net'] += net_payout
        except Exception:
            pass
    
    # Convert to list and apply search filter
    result = list(grouped_data.values())
    
    if search:
        result = [
            item for item in result
            if search in item['advertiser'].lower() or search in item['coupon'].lower()
        ]
    
    # Sort by total_sales descending
    result.sort(key=lambda x: x['total_sales'], reverse=True)
    
    # Format results to match frontend expectations
    formatted_results = []
    for item in result:
        # Set net_payout to None if no cancellation rate exists
        net_payout_value = None
        if item['has_cancellation_rate']:
            net_payout_value = round(item['total_payout_net'], 2)
        
        formatted_results.append({
            'advertiser_id': item['advertiser_id'],
            'advertiser_name': item['advertiser'],
            'coupon': item['coupon'],
            'orders': item['total_orders'],
            'sales': round(item['total_sales'], 2),
            'gross_payout': round(item['total_payout_gross'], 2),
            'net_payout': net_payout_value
        })
    
    return Response({
        'results': formatted_results,
        'count': len(formatted_results),
        'next': None,
        'previous': None
    }, status=status.HTTP_200_OK)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def partner_campaigns_view(request):
    """
    Get all campaigns (advertisers) the partner is working on
    """
    user = request.user
    
    try:
        company_user = CompanyUser.objects.get(user=user)
    except CompanyUser.DoesNotExist:
        return Response({"detail": "User not found"}, status=status.HTTP_404_NOT_FOUND)
    
    # Check if user is a partner
    if company_user.role.name != 'TeamMember' or company_user.department not in ['affiliate', 'influencer']:
        return Response(
            {"detail": "Access denied. This page is only for partners."},
            status=status.HTTP_403_FORBIDDEN
        )
    
    # Get the partner(s) assigned to this user
    assignment = company_user.accountassignment_set.first()
    if not assignment:
        return Response({"detail": "No partner assignment found"}, status=status.HTTP_404_NOT_FOUND)
    
    user_partners = assignment.partners.all()
    if not user_partners.exists():
        return Response({"detail": "No partners assigned"}, status=status.HTTP_404_NOT_FOUND)
    
    # Get date range for performance stats (last 30 days)
    date_to = datetime.now()
    date_from = date_to - timedelta(days=30)
    
    # Get all advertisers
    all_advertisers = Advertiser.objects.all()
    
    # Get advertiser IDs the partner has worked with
    working_advertiser_ids = set(
        CampaignPerformance.objects.filter(
            partner__in=user_partners
        ).values_list('advertiser_id', flat=True).distinct()
    )
    
    campaigns = []
    for advertiser in all_advertisers:
        # Get performance data for last 30 days
        perf = CampaignPerformance.objects.filter(
            advertiser=advertiser,
            partner__in=user_partners,
            date__gte=date_from,
            date__lte=date_to
        ).aggregate(
            total_orders=Sum('total_orders'),
            total_sales=Sum('total_sales'),
            total_payout=Sum('total_payout'),
            total_revenue=Sum('total_revenue')
        )
        
        # Get assigned coupons count
        coupons_count = Coupon.objects.filter(
            advertiser=advertiser,
            partner__in=user_partners
        ).count()
        
        # Get lifetime performance
        lifetime_perf = CampaignPerformance.objects.filter(
            advertiser=advertiser,
            partner__in=user_partners
        ).aggregate(
            lifetime_orders=Sum('total_orders'),
            lifetime_sales=Sum('total_sales'),
            lifetime_payout=Sum('total_payout')
        )
        
        # Calculate net payout
        gross_payout = float(perf['total_payout'] or 0)
        net_payout = gross_payout
        
        try:
            latest_rate = AdvertiserCancellationRate.objects.filter(
                advertiser=advertiser
            ).order_by('-start_date').first()
            if latest_rate:
                cancellation_rate = float(latest_rate.cancellation_rate)
                net_payout = gross_payout * (1 - cancellation_rate / 100)
        except Exception:
            pass
        
        # Determine if partner is working on this campaign
        is_working = advertiser.id in working_advertiser_ids
        
        campaigns.append({
            'advertiser_id': advertiser.id,
            'advertiser_name': advertiser.name,
            'orders': perf['total_orders'] or 0,
            'sales': round(float(perf['total_sales'] or 0), 2),
            'revenue': round(float(perf['total_revenue'] or 0), 2),
            'payout': round(net_payout, 2),
            'profit': round(float(perf['total_revenue'] or 0) - net_payout, 2),
            'is_working': is_working
        })
    
    # Sort: working campaigns first, then by sales
    campaigns.sort(key=lambda x: (not x['is_working'], -x['sales']))
    
    return Response({
        'campaigns': campaigns
    }, status=status.HTTP_200_OK)
