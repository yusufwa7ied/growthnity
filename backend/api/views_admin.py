from django.http import JsonResponse
from django.utils.dateparse import parse_date
from datetime import datetime
from rest_framework.decorators import api_view, permission_classes# type: ignore
from rest_framework.permissions import IsAuthenticated# type: ignore
from rest_framework.response import Response# type: ignore
from rest_framework import status# type: ignore
from django.db.models import Sum, Count, Q
from django.db import transaction

from .models import Advertiser, CampaignPerformance, MediaBuyerDailySpend, DepartmentTarget, CompanyUser, Partner, PartnerPayout, Coupon
from .serializers import AdvertiserDetailSerializer, PartnerSerializer

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def high_level_dashboard_view(request):
    user = request.user
    try:
        company_user = CompanyUser.objects.get(user=user)
    except CompanyUser.DoesNotExist:
        return JsonResponse({"detail": "Forbidden"}, status=403)

    if company_user.role.name not in ["Admin", "OpsManager"]:# type: ignore
        return JsonResponse({"detail": "Forbidden"}, status=403)

    advertiser_id = request.GET.get('advertiser_id')
    month_str = request.GET.get('month')
    partner_type = request.GET.get('partner_type')

    if month_str:
        try:
            month_date = datetime.strptime(month_str, '%Y-%m')
        except ValueError:
            return JsonResponse({"detail": "Invalid month format. Use YYYY-MM."}, status=400)
    else:
        month_date = datetime.now()

    month_start = month_date.replace(day=1)
    if month_date.month == 12:
        month_end = month_date.replace(year=month_date.year + 1, month=1, day=1)
    else:
        month_end = month_date.replace(month=month_date.month + 1, day=1)

    advertisers_qs = Advertiser.objects.all()
    if advertiser_id:
        advertisers_qs = advertisers_qs.filter(id=advertiser_id)

    results = []

    for advertiser in advertisers_qs:
        cp_filters = {
            'advertiser': advertiser,
            'date__gte': month_start,
            'date__lt': month_end,
        }
        mb_filters = {
            'advertiser': advertiser,
            'date__gte': month_start,
            'date__lt': month_end,
        }
        dt_filters = {
            'month': month_start.date(),
            'advertiser': advertiser,
        }

        if partner_type in ['MB', 'AFF', 'INF']:
            cp_filters['partner__partner_type'] = partner_type
            mb_filters['partner__partner_type'] = partner_type
            dt_filters['partner_type'] = partner_type
        else:
            partner_type = None

        cp_agg = CampaignPerformance.objects.filter(**cp_filters).aggregate(
            total_revenue_sum = Sum('total_revenue'),
            total_payout_sum = Sum('total_payout'),
            total_orders_sum = Sum('total_orders'),
        )
        revenue = cp_agg['total_revenue_sum'] or 0
        payout = cp_agg['total_payout_sum'] or 0
        orders = cp_agg['total_orders_sum'] or 0
        profit = revenue - payout

        mb_agg = MediaBuyerDailySpend.objects.filter(**mb_filters).aggregate(
            amount_spent_sum = Sum('amount_spent')
        )
        spend = mb_agg['amount_spent_sum'] or 0

        try:
            targets = DepartmentTarget.objects.get(**dt_filters)
            targets_dict = {
                "revenue_target": targets.revenue_target,
                "profit_target": targets.profit_target,
                "orders_target": targets.orders_target,
                "spend_target": targets.spend_target,
            }
        except DepartmentTarget.DoesNotExist:
            targets_dict = {
                "revenue_target": None,
                "profit_target": None,
                "orders_target": None,
                "spend_target": None,
            }

        results.append({
            "advertiser": advertiser.name,
            "month": dt_filters['month'].strftime('%Y-%m'),
            "partner_type": partner_type,
            "revenue": revenue,
            "payout": payout,
            "profit": profit,
            "spend": spend,
            "orders": orders,
            "targets": targets_dict,
        })

    return JsonResponse(results, safe=False)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def list_advertisers_view(request):
    """List all advertisers with statistics"""
    user = request.user
    try:
        company_user = CompanyUser.objects.get(user=user)
    except CompanyUser.DoesNotExist:
        return Response({"detail": "Forbidden"}, status=status.HTTP_403_FORBIDDEN)

    # Allow team members to view advertisers (read-only for spend tracking)
    if company_user.role.name not in ["Admin", "OpsManager", "TeamMember"]:
        return Response({"detail": "Forbidden"}, status=status.HTTP_403_FORBIDDEN)

    advertisers = Advertiser.objects.all().prefetch_related('payouts', 'payouts__partner')
    
    # Calculate stats for each advertiser
    data = []
    for adv in advertisers:
        coupon_count = Coupon.objects.filter(advertiser=adv).count()
        partner_count = adv.payouts.values('partner').distinct().count()
        
        data.append({
            **AdvertiserDetailSerializer(adv).data,
            'stats': {
                'coupon_count': coupon_count,
                'partner_count': partner_count,
            }
        })
    
    return Response(data)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def create_advertiser_view(request):
    """Create a new advertiser with default partner payouts"""
    user = request.user
    try:
        company_user = CompanyUser.objects.get(user=user)
    except CompanyUser.DoesNotExist:
        return Response({"detail": "Forbidden"}, status=status.HTTP_403_FORBIDDEN)

    if company_user.role.name not in ["Admin", "OpsManager"]:
        return Response({"detail": "Forbidden"}, status=status.HTTP_403_FORBIDDEN)

    with transaction.atomic():
        # Create advertiser
        serializer = AdvertiserDetailSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        advertiser = serializer.save()
        
        # Create default partner payouts if provided
        partner_payouts = request.data.get('partner_payouts', [])
        for payout_data in partner_payouts:
            partner_id = payout_data.get('partner_id')
            if partner_id:
                PartnerPayout.objects.create(
                    advertiser=advertiser,
                    partner_id=partner_id,
                    ftu_payout=payout_data.get('ftu_payout'),
                    rtu_payout=payout_data.get('rtu_payout'),
                    ftu_fixed_bonus=payout_data.get('ftu_fixed_bonus'),
                    rtu_fixed_bonus=payout_data.get('rtu_fixed_bonus'),
                    exchange_rate=payout_data.get('exchange_rate'),
                    currency=payout_data.get('currency'),
                    rate_type=payout_data.get('rate_type', 'percent'),
                    condition=payout_data.get('condition'),
                    start_date=payout_data.get('start_date'),
                    end_date=payout_data.get('end_date'),
                )
        
        # Refresh to get payouts
        advertiser.refresh_from_db()
        return Response(
            AdvertiserDetailSerializer(advertiser).data, 
            status=status.HTTP_201_CREATED
        )


@api_view(['PUT'])
@permission_classes([IsAuthenticated])
def update_advertiser_view(request, pk):
    """Update an advertiser and its partner payouts"""
    user = request.user
    try:
        company_user = CompanyUser.objects.get(user=user)
    except CompanyUser.DoesNotExist:
        return Response({"detail": "Forbidden"}, status=status.HTTP_403_FORBIDDEN)

    if company_user.role.name not in ["Admin", "OpsManager"]:
        return Response({"detail": "Forbidden"}, status=status.HTTP_403_FORBIDDEN)

    try:
        advertiser = Advertiser.objects.get(pk=pk)
    except Advertiser.DoesNotExist:
        return Response({"detail": "Not found"}, status=status.HTTP_404_NOT_FOUND)

    with transaction.atomic():
        serializer = AdvertiserDetailSerializer(advertiser, data=request.data, partial=True)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        serializer.save()
        
        # Add new partner payouts if provided
        # This endpoint ONLY adds new special payouts, never updates or deletes existing ones
        # Existing payouts are managed via Django admin (where dates can be set)
        partner_payouts = request.data.get('partner_payouts', [])
        if partner_payouts:
            from django.utils import timezone
            from .models import PayoutRuleHistory
            
            for payout_data in partner_payouts:
                partner_id = payout_data.get('partner_id')
                if not partner_id:
                    continue
                
                # Always create new records - don't update existing ones
                # Use create() which will raise IntegrityError if duplicate exists
                try:
                    payout = PartnerPayout.objects.create(
                        advertiser=advertiser,
                        partner_id=partner_id,
                        ftu_payout=payout_data.get('ftu_payout'),
                        rtu_payout=payout_data.get('rtu_payout'),
                        ftu_fixed_bonus=payout_data.get('ftu_fixed_bonus'),
                        rtu_fixed_bonus=payout_data.get('rtu_fixed_bonus'),
                        exchange_rate=payout_data.get('exchange_rate'),
                        currency=payout_data.get('currency'),
                        rate_type=payout_data.get('rate_type', 'percent'),
                        condition=payout_data.get('condition'),
                        # App doesn't set dates - admin sets them later if needed
                        start_date=None,
                        end_date=None,
                    )
                    
                    # Create PayoutRuleHistory for this new partner payout
                    PayoutRuleHistory.objects.create(
                        advertiser=advertiser,
                        partner_id=partner_id,
                        effective_date=timezone.now(),
                        ftu_payout=payout_data.get('ftu_payout'),
                        rtu_payout=payout_data.get('rtu_payout'),
                        ftu_fixed_bonus=payout_data.get('ftu_fixed_bonus'),
                        rtu_fixed_bonus=payout_data.get('rtu_fixed_bonus'),
                        rate_type=payout_data.get('rate_type', 'percent'),
                        assigned_by=request.user,
                        notes=f"Partner-specific payout created via API by {request.user.username}"
                    )
                except Exception as e:
                    # If duplicate (same advertiser+partner+start_date), skip it
                    # This prevents errors if user tries to add same partner twice
                    continue
        
        advertiser.refresh_from_db()
        return Response(AdvertiserDetailSerializer(advertiser).data)


@api_view(['DELETE'])
@permission_classes([IsAuthenticated])
def delete_advertiser_view(request, pk):
    """Delete an advertiser"""
    user = request.user
    try:
        company_user = CompanyUser.objects.get(user=user)
    except CompanyUser.DoesNotExist:
        return Response({"detail": "Forbidden"}, status=status.HTTP_403_FORBIDDEN)

    if company_user.role.name not in ["Admin", "OpsManager"]:
        return Response({"detail": "Forbidden"}, status=status.HTTP_403_FORBIDDEN)

    try:
        advertiser = Advertiser.objects.get(pk=pk)
    except Advertiser.DoesNotExist:
        return Response({"detail": "Not found"}, status=status.HTTP_404_NOT_FOUND)

    advertiser.delete()
    return Response({"detail": "Deleted successfully"}, status=status.HTTP_204_NO_CONTENT)


@api_view(['GET', 'POST'])
@permission_classes([IsAuthenticated])
def media_buyer_spend_view(request):
    """Get all spends or create a new spend entry"""
    user = request.user
    try:
        company_user = CompanyUser.objects.get(user=user)
    except CompanyUser.DoesNotExist:
        return Response({"detail": "Forbidden"}, status=status.HTTP_403_FORBIDDEN)

    # Only allow Admin, OpsManager, or TeamMembers in media_buying department
    role = company_user.role.name if company_user.role else None
    if role not in ["Admin", "OpsManager"]:
        if role != "TeamMember" or company_user.department != "media_buying":
            return Response({"detail": "Access denied. This page is only for media buyers."}, status=status.HTTP_403_FORBIDDEN)

    if request.method == 'GET':
        # Get spend history
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        advertiser_id = request.GET.get('advertiser_id')
        partner_id = request.GET.get('partner_id')

        spends = MediaBuyerDailySpend.objects.select_related('advertiser', 'partner').all()

        # If user is TeamMember (media buyer), only show their own spends
        if role == "TeamMember" and company_user.department == "media_buying":
            # Get the user's assigned partner
            assignment = company_user.accountassignment_set.first()
            if assignment:
                user_partners = assignment.partners.filter(partner_type="MB")
                spends = spends.filter(partner__in=user_partners)
            else:
                # No assignment means no spends visible
                spends = spends.none()
        # Admin and OpsManager see all spends (no filtering needed)

        if date_from:
            spends = spends.filter(date__gte=date_from)
        if date_to:
            spends = spends.filter(date__lte=date_to)
        if advertiser_id:
            spends = spends.filter(advertiser_id=advertiser_id)
        if partner_id:
            spends = spends.filter(partner_id=partner_id)

        spends = spends.order_by('-date')[:100]  # Limit to 100 recent records

        data = [{
            'id': s.id,
            'date': s.date,
            'advertiser_id': s.advertiser.id,
            'advertiser_name': s.advertiser.name,
            'partner_id': s.partner.id if s.partner else None,
            'partner_name': s.partner.name if s.partner else None,
            'coupon_id': s.coupon.id if s.coupon else None,
            'coupon_code': s.coupon.code if s.coupon else None,
            'platform': s.platform,
            'amount_spent': float(s.amount_spent),
            'currency': s.currency or 'USD'
        } for s in spends]

        return Response(data, status=status.HTTP_200_OK)

    elif request.method == 'POST':
        # Create new spend entry
        date = request.data.get('date')
        advertiser_id = request.data.get('advertiser_id')
        partner_id = request.data.get('partner_id')
        platform = request.data.get('platform', 'Meta')
        amount_spent = request.data.get('amount_spent')
        currency = request.data.get('currency', 'USD')

        if not all([date, advertiser_id, partner_id, amount_spent]):
            return Response({"detail": "Missing required fields: date, advertiser_id, partner_id, amount_spent"}, status=status.HTTP_400_BAD_REQUEST)

        # If TeamMember, verify they can only create for their own partner
        if role == "TeamMember" and company_user.department == "media_buying":
            assignment = company_user.accountassignment_set.first()
            if assignment:
                user_partners = assignment.partners.filter(partner_type="MB")
                if not user_partners.filter(id=partner_id).exists():
                    return Response({"detail": "You can only create records for your own partner"}, status=status.HTTP_403_FORBIDDEN)
            else:
                return Response({"detail": "You can only create records for your own partner"}, status=status.HTTP_403_FORBIDDEN)

        try:
            spend, created = MediaBuyerDailySpend.objects.update_or_create(
                date=date,
                advertiser_id=advertiser_id,
                partner_id=partner_id,
                platform=platform,
                defaults={
                    'amount_spent': amount_spent,
                    'currency': currency,
                    'coupon': None  # Coupon is now always NULL
                }
            )
            
            return Response({
                'id': spend.id,
                'date': spend.date,
                'advertiser_id': spend.advertiser.id,
                'advertiser_name': spend.advertiser.name,
                'partner_id': spend.partner.id if spend.partner else None,
                'partner_name': spend.partner.name if spend.partner else None,
                'coupon_id': spend.coupon.id if spend.coupon else None,
                'coupon_code': spend.coupon.code if spend.coupon else None,
                'platform': spend.platform,
                'amount_spent': float(spend.amount_spent),
                'currency': spend.currency or 'USD',
                'created': created
            }, status=status.HTTP_201_CREATED if created else status.HTTP_200_OK)

        except Exception as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)


@api_view(['DELETE'])
@permission_classes([IsAuthenticated])
def delete_media_buyer_spend_view(request, pk):
    """Delete a spend entry"""
    user = request.user
    try:
        company_user = CompanyUser.objects.get(user=user)
    except CompanyUser.DoesNotExist:
        return Response({"detail": "Forbidden"}, status=status.HTTP_403_FORBIDDEN)

    # Only allow Admin, OpsManager, or TeamMembers in media_buying department
    role = company_user.role.name if company_user.role else None
    if role not in ["Admin", "OpsManager"]:
        if role != "TeamMember" or company_user.department != "media_buying":
            return Response({"detail": "Access denied. This page is only for media buyers."}, status=status.HTTP_403_FORBIDDEN)

    try:
        spend = MediaBuyerDailySpend.objects.get(pk=pk)
    except MediaBuyerDailySpend.DoesNotExist:
        return Response({"detail": "Not found"}, status=status.HTTP_404_NOT_FOUND)

    # If user is TeamMember (media buyer), only allow deleting their own spends
    if role == "TeamMember" and company_user.department == "media_buying":
        assignment = company_user.accountassignment_set.first()
        if assignment:
            user_partners = assignment.partners.filter(partner_type="MB")
            if spend.partner not in user_partners:
                return Response({"detail": "You can only delete your own records"}, status=status.HTTP_403_FORBIDDEN)
        else:
            return Response({"detail": "You can only delete your own records"}, status=status.HTTP_403_FORBIDDEN)
    # Admin and OpsManager can delete any record

    spend.delete()
    return Response({"detail": "Deleted successfully"}, status=status.HTTP_204_NO_CONTENT)


# ==================== Partner Management ====================

@api_view(['GET', 'POST'])
@permission_classes([IsAuthenticated])
def partners_view(request):
    """List all partners or create a new partner"""
    user = request.user
    try:
        company_user = CompanyUser.objects.get(user=user)
    except CompanyUser.DoesNotExist:
        return Response({"detail": "Forbidden"}, status=status.HTTP_403_FORBIDDEN)

    # Only Admin and OpsManager can manage partners
    role = company_user.role.name if company_user.role else None
    if role not in ["Admin", "OpsManager"]:
        return Response({"detail": "Access denied"}, status=status.HTTP_403_FORBIDDEN)

    if request.method == 'GET':
        partners = Partner.objects.all().order_by('-id')
        data = []
        for p in partners:
            # Get special payouts for this partner across all advertisers
            special_payouts = PartnerPayout.objects.filter(
                partner=p
            ).select_related('advertiser').order_by('advertiser__name')
            
            special_payout_info = []
            for sp in special_payouts:
                special_payout_info.append({
                    'advertiser': sp.advertiser.name,
                    'advertiser_id': sp.advertiser.id,
                    'ftu_payout': float(sp.ftu_payout) if sp.ftu_payout else None,
                    'rtu_payout': float(sp.rtu_payout) if sp.rtu_payout else None,
                    'ftu_fixed_bonus': float(sp.ftu_fixed_bonus) if sp.ftu_fixed_bonus else None,
                    'rtu_fixed_bonus': float(sp.rtu_fixed_bonus) if sp.rtu_fixed_bonus else None,
                    'rate_type': sp.rate_type,
                })
            
            data.append({
                'id': p.id,
                'name': p.name,
                'partner_type': p.partner_type,
                'email': p.email or '',
                'phone': p.phone or '',
                'has_special_payouts': len(special_payout_info) > 0,
                'special_payouts_count': len(special_payout_info),
                'special_payouts': special_payout_info
            })
        return Response(data, status=status.HTTP_200_OK)

    elif request.method == 'POST':
        name = request.data.get('name')
        partner_type = request.data.get('partner_type')
        email = request.data.get('email', '')
        phone = request.data.get('phone', '')

        if not all([name, partner_type]):
            return Response({"detail": "Name and partner type are required"}, status=status.HTTP_400_BAD_REQUEST)

        if partner_type not in ['AFF', 'INF', 'MB']:
            return Response({"detail": "Invalid partner type"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            partner = Partner.objects.create(
                name=name,
                partner_type=partner_type,
                email=email,
                phone=phone
            )
            return Response({
                'id': partner.id,
                'name': partner.name,
                'partner_type': partner.partner_type,
                'email': partner.email or '',
                'phone': partner.phone or ''
            }, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)


@api_view(['PUT', 'DELETE'])
@permission_classes([IsAuthenticated])
def partner_detail_view(request, pk):
    """Update or delete a partner"""
    user = request.user
    try:
        company_user = CompanyUser.objects.get(user=user)
    except CompanyUser.DoesNotExist:
        return Response({"detail": "Forbidden"}, status=status.HTTP_403_FORBIDDEN)

    role = company_user.role.name if company_user.role else None
    if role not in ["Admin", "OpsManager"]:
        return Response({"detail": "Access denied"}, status=status.HTTP_403_FORBIDDEN)

    try:
        partner = Partner.objects.get(pk=pk)
    except Partner.DoesNotExist:
        return Response({"detail": "Partner not found"}, status=status.HTTP_404_NOT_FOUND)

    if request.method == 'PUT':
        partner.name = request.data.get('name', partner.name)
        partner.partner_type = request.data.get('partner_type', partner.partner_type)
        partner.email = request.data.get('email', partner.email)
        partner.phone = request.data.get('phone', partner.phone)
        partner.save()

        return Response({
            'id': partner.id,
            'name': partner.name,
            'partner_type': partner.partner_type,
            'email': partner.email or '',
            'phone': partner.phone or ''
        }, status=status.HTTP_200_OK)

    elif request.method == 'DELETE':
        partner.delete()
        return Response({"detail": "Partner deleted successfully"}, status=status.HTTP_204_NO_CONTENT)

