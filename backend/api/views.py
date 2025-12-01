
from rest_framework.response import Response # type: ignore
from rest_framework_simplejwt.exceptions import InvalidToken, TokenError # type: ignore
from rest_framework.permissions import IsAuthenticated # type: ignore
from rest_framework.pagination import PageNumberPagination # type: ignore


from .models import CompanyUser, AccountAssignment, Advertiser, Partner
from .models import CampaignPerformance, Coupon, PartnerPayout
from .models import Coupon, Advertiser, Partner, CouponAssignmentHistory
from .models import DepartmentTarget, MediaBuyerDailySpend
from .serializers import AdvertiserSerializer, PartnerSerializer

from django.db.models import Sum
from django.utils.dateparse import parse_date

from decimal import Decimal
from rest_framework.decorators import api_view, permission_classes # type: ignore
from django.db.models import F, Q

from datetime import datetime, date, timedelta
from calendar import monthrange


# Pagination class for performance table
class PerformanceTablePagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 100


@api_view(['POST'])
def token_refresh_view(request):
    """
    Refresh the access token using the refresh token.
    Expects: { "refresh": "refresh_token" }
    Returns: { "access": "new_access_token", "refresh": "refresh_token" }
    """
    from rest_framework_simplejwt.views import TokenRefreshView # type: ignore
    
    try:
        refresh_token = request.data.get('refresh')
        if not refresh_token:
            return Response({'error': 'Refresh token is required'}, status=400)
        
        view = TokenRefreshView.as_view()
        return view(request)
    except (InvalidToken, TokenError) as e:
        return Response({'error': 'Invalid or expired refresh token'}, status=401)






@api_view(['GET'])
@permission_classes([IsAuthenticated])
def context_view(request):
    user = request.user
    try:
        cu = CompanyUser.objects.get(user=user)
        role = cu.role.name if cu.role else ""
    except CompanyUser.DoesNotExist:
        role = ""

    print("🌐 SENDING ROLE IN CONTEXT:", role)

    return Response({
        "username": user.username,
        "role": role
    })


# Returns the authenticated user's dashboard context
@api_view(["GET"])
@permission_classes([IsAuthenticated])
def user_dashboard_context(request):
    user = request.user
    try:
        company_user = CompanyUser.objects.get(user=user)
    except CompanyUser.DoesNotExist:
        return Response({"username": user.username, "role": "Unknown", "error": "No CompanyUser found."})

    role = company_user.role.name if company_user.role else "Unknown"
    department = company_user.department if company_user.department else None
    print("🌐 SENDING ROLE IN CONTEXT:", role, "DEPARTMENT:", department)
    base = {
        "username": user.username,
        "role": role,
        "department": department,
    }

    # ViewOnly without department → C-level, see all data, dashboard only
    if role == "ViewOnly" and not department:
        base["can_see_all"] = True
        base["is_view_only"] = True
        return Response(base)

    # Admin or OpsManager without department → see all
    if role in ["Admin", "OpsManager"] and not department:
        base["can_see_all"] = True
        return Response(base)

    # OpsManager with department → see whole department (no assignments needed)
    if role == "OpsManager" and department:
        base["can_see_all"] = True  # See all data in their department
        return Response(base)

    # ViewOnly with department OR TeamMember → use AccountAssignment
    # These users see only their assigned advertisers/partners
    assignments = AccountAssignment.objects.filter(company_user=company_user)
    advertisers = Advertiser.objects.filter(accountassignment__in=assignments).distinct()

    base["advertisers"] = AdvertiserSerializer(advertisers, many=True).data
    base["department"] = company_user.department

    if company_user.department == "affiliate":
        affs = Partner.objects.filter(partner_type="AFF")
        base["affiliates"] = PartnerSerializer(affs, many=True).data
    elif company_user.department == "influencer":
        infs = Partner.objects.filter(partner_type="INF")
        base["influencers"] = PartnerSerializer(infs, many=True).data
    elif company_user.department == "media_buying":
        mbs = Partner.objects.filter(partner_type="MB")
        base["media_buyers"] = PartnerSerializer(mbs, many=True).data
        # If user is assigned to a specific partner, include partner_id
        assigned_partners = Partner.objects.filter(accountassignment__in=assignments, partner_type="MB").first()
        if assigned_partners:
            base["partner_id"] = assigned_partners.id

    # Build advertiser → {affiliates, influencers} map for dropdown chaining
    advertiser_partner_map = {}
    for adv in advertisers:
        partner_map = {
            "affiliates": list(Partner.objects.filter(partner_type="AFF").values_list("id", flat=True)),
            "influencers": list(Partner.objects.filter(partner_type="INF").values_list("id", flat=True)),
            "media_buyers": list(Partner.objects.filter(partner_type="MB").values_list("id", flat=True)),
        }
        advertiser_partner_map[adv.id] = partner_map
    base["advertiser_partner_map"] = advertiser_partner_map

    return Response(base)

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def kpis_view(request):
    """
    Returns KPI totals:
    - total_orders
    - total_sales
    - total_revenue
    - total_payout
    - total_profit
    """

    user = request.user
    company_user = CompanyUser.objects.select_related("role").filter(user=user).first()

    qs = CampaignPerformance.objects.all()

    # -------------------------------
    # Filters (NOW INCLUDES COUPON) - SUPPORT MULTIPLE VALUES
    # -------------------------------
    advertiser_ids = request.GET.getlist("advertiser_id")
    partner_ids = request.GET.getlist("partner_id")
    coupon_codes = request.GET.getlist("coupon_code")
    partner_type = request.GET.get("partner_type")
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")

    # -------------------------------
    # Department scoping (only for OpsManager with department)
    # BUT: Skip if explicit partner_type filter is provided (user's choice takes precedence)
    # TeamMembers will be filtered by AccountAssignment later
    # -------------------------------
    if company_user and company_user.department and company_user.role.name == "OpsManager" and not partner_type:
        dept = company_user.department
        if dept == "media_buying":
            qs = qs.filter(partner__partner_type="MB")
        elif dept == "affiliate":
            qs = qs.filter(partner__partner_type="AFF")
        elif dept == "influencer":
            qs = qs.filter(partner__partner_type="INF")

    if advertiser_ids:
        qs = qs.filter(advertiser_id__in=advertiser_ids)

    if partner_ids:
        qs = qs.filter(partner_id__in=partner_ids)
    
    if partner_type:
        qs = qs.filter(partner__partner_type=partner_type)

    if coupon_codes:
        qs = qs.filter(coupon__code__in=coupon_codes)

    if date_from:
        d = parse_date(date_from)
        if d:
            qs = qs.filter(date__gte=d)

    if date_to:
        d = parse_date(date_to)
        if d:
            qs = qs.filter(date__lte=d)

    # -------------------------------
    # Role-based access for Members
    # -------------------------------
    if company_user and company_user.role:
        role = company_user.role.name
        department = company_user.department
        
        # Full access: Admin (no dept), OpsManager (no dept), ViewOnly (no dept)
        full_access_roles = {"Admin", "OpsManager"}
        has_full_access = (role in full_access_roles and not department) or (role == "ViewOnly" and not department)

        if not has_full_access:
            assignments = AccountAssignment.objects.filter(company_user=company_user).prefetch_related("advertisers")
            advertiser_ids = set()
            partner_ids = set()

            for a in assignments:
                advertiser_ids.update(a.advertisers.values_list("id", flat=True))
                partner_ids.update(a.partners.values_list("id", flat=True))

            # If TeamMember has NO assignments, return empty queryset
            if not advertiser_ids and not partner_ids:
                qs = qs.none()
            else:
                # Filter by assignments
                if advertiser_ids:
                    qs = qs.filter(advertiser_id__in=list(advertiser_ids))
                if partner_ids:
                    qs = qs.filter(partner_id__in=list(partner_ids))

    # -------------------------------
    # Aggregation
    # -------------------------------
    agg = qs.aggregate(
        total_orders_sum=Sum("total_orders"),
        total_sales_sum=Sum("total_sales"),
        total_revenue_sum=Sum("total_revenue"),
        total_payout_sum=Sum("total_payout")
    )

    total_orders = agg["total_orders_sum"] or 0
    total_sales = agg["total_sales_sum"] or 0
    total_revenue = agg["total_revenue_sum"] or 0
    total_payout_original = agg["total_payout_sum"] or 0
    
    # Check if we have MB records in the filtered data
    is_media_buyer = company_user and company_user.department == "media_buying"
    mb_qs = qs.filter(partner__partner_type="MB")
    has_mb = mb_qs.exists()
    
    # Get MB spend if there are any MB records
    if has_mb:
        spend_keys = mb_qs.values_list('date', 'advertiser_id', 'partner_id').distinct()
        
        from django.db.models import Q
        spend_conditions = Q()
        for date, adv_id, part_id in spend_keys:
            spend_conditions |= Q(date=date, advertiser_id=adv_id, partner_id=part_id)
        
        if spend_conditions:
            spend_qs = MediaBuyerDailySpend.objects.filter(spend_conditions)
            spend_agg = spend_qs.aggregate(total_spend=Sum("amount_spent"))
            mb_spend = float(spend_agg["total_spend"] or 0)
        else:
            mb_spend = 0
    else:
        mb_spend = 0
    
    # Get non-MB payout
    non_mb_qs = qs.exclude(partner__partner_type="MB")
    non_mb_agg = non_mb_qs.aggregate(total_payout=Sum("total_payout"))
    non_mb_payout = float(non_mb_agg["total_payout"] or 0)
    
    # Total "payout" = MB spend + non-MB actual payout
    total_payout = mb_spend + non_mb_payout
    
    # Profit = revenue - payout (where payout includes MB spend)
    total_profit = float(total_revenue) - total_payout

    return Response({
        "total_orders": int(total_orders),
        "total_sales": float(total_sales),
        "total_revenue": float(total_revenue),
        "total_payout": float(total_payout),
        "total_profit": float(total_profit),
        "records_count": qs.count()
    })

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def graph_data_view(request):
    user = request.user
    company_user = CompanyUser.objects.select_related("role").filter(user=user).first()

    # base queryset
    qs = CampaignPerformance.objects.all()

    # Optional filters - SUPPORT MULTIPLE VALUES
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")
    coupon_codes = request.GET.getlist("coupon_code")
    partner_ids = request.GET.getlist("partner_id")
    advertiser_ids = request.GET.getlist("advertiser_id")
    partner_type = request.GET.get("partner_type")

    # Department scope: MB, AFF, INF (only for OpsManager with department)
    # BUT: Skip if explicit partner_type filter is provided (user's choice takes precedence)
    # TeamMembers will be filtered by AccountAssignment later
    if company_user and company_user.department and company_user.role.name == "OpsManager" and not partner_type:
        dept = company_user.department
        if dept == "media_buying":
            qs = qs.filter(partner__partner_type="MB")
        elif dept == "affiliate":
            qs = qs.filter(partner__partner_type="AFF")
        elif dept == "influencer":
            qs = qs.filter(partner__partner_type="INF")

    if advertiser_ids:
        qs = qs.filter(advertiser_id__in=advertiser_ids)
    if partner_ids:
        qs = qs.filter(partner_id__in=partner_ids)
    if coupon_codes:
        qs = qs.filter(coupon__code__in=coupon_codes)
    if date_from:
        d = parse_date(date_from)
        if d:
            qs = qs.filter(date__gte=d)

    if date_to:
        d = parse_date(date_to)
        if d:
            qs = qs.filter(date__lte=d)

    # Assignment scope for non-admin roles
    if company_user and company_user.role:
        role = company_user.role.name
        department = company_user.department
        
        # Full access: Admin (no dept), OpsManager (no dept), ViewOnly (no dept)
        full_access_roles = {"Admin", "OpsManager"}
        has_full_access = (role in full_access_roles and not department) or (role == "ViewOnly" and not department)

        if not has_full_access:
            assignments = AccountAssignment.objects.filter(
                company_user=company_user
            ).prefetch_related("advertisers", "partners")

            advertiser_ids = set()
            partner_ids = set()

            for a in assignments:
                advertiser_ids.update(a.advertisers.values_list("id", flat=True))
                partner_ids.update(a.partners.values_list("id", flat=True))

            # If TeamMember has NO assignments, return empty queryset
            if not advertiser_ids and not partner_ids:
                qs = qs.none()
            else:
                if advertiser_ids:
                    qs = qs.filter(advertiser_id__in=list(advertiser_ids))
                if partner_ids:
                    qs = qs.filter(partner_id__in=list(partner_ids))
            if advertiser_ids:
                qs = qs.filter(advertiser_id__in=list(advertiser_ids))
            if partner_ids:
                qs = qs.filter(partner_id__in=list(partner_ids))    # Detect if user is full access (Admin / OpsManager)
    user_is_admin = company_user and company_user.role and company_user.role.name in {"Admin", "OpsManager"}

    # Aggregate KPIs per day - ALL ROLES now get revenue
    daily_data = qs.values("date").annotate(
        total_sales=Sum("total_sales"),
        total_revenue=Sum("total_revenue"),
        total_payout=Sum("total_payout"),
    ).order_by("date")

    # calculate profit (only for admin roles)
    if user_is_admin:
        for entry in daily_data:
            rev = entry["total_revenue"] or 0
            payout = entry["total_payout"] or 0
            entry["total_profit"] = rev - payout

        result = {
            "dates": [e["date"] for e in daily_data],
            "daily_sales": [e["total_sales"] for e in daily_data],
            "daily_revenue": [e["total_revenue"] for e in daily_data],
            "daily_payout": [e["total_payout"] for e in daily_data],
            "daily_profit": [e["total_profit"] for e in daily_data],
        }
    else:
        # Team members see sales, revenue, and payout (no profit)
        result = {
            "dates": [e["date"] for e in daily_data],
            "daily_sales": [e["total_sales"] for e in daily_data],
            "daily_revenue": [e["total_revenue"] for e in daily_data],
            "daily_payout": [e["total_payout"] for e in daily_data],
        }

    return Response(result)

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def performance_table_view(request):
    user = request.user
    company_user = CompanyUser.objects.select_related("role").filter(user=user).first()
    role = company_user.role.name if company_user and company_user.role else None

    qs = CampaignPerformance.objects.all()

    # -------------------------------
    # Filters - SUPPORT MULTIPLE VALUES
    # -------------------------------
    advertiser_ids = request.GET.getlist("advertiser_id")
    partner_ids = request.GET.getlist("partner_id")
    coupon_codes = request.GET.getlist("coupon_code")
    partner_type = request.GET.get("partner_type")
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")

    # -------------------------------
    # Department scoping (only for OpsManager with department)
    # BUT: Skip if explicit partner_type filter is provided (user's choice takes precedence)
    # TeamMembers will be filtered by AccountAssignment later
    # -------------------------------
    if company_user and company_user.department and role == "OpsManager" and not partner_type:
        dept = company_user.department
        if dept == "media_buying":
            qs = qs.filter(partner__partner_type="MB")
        elif dept == "affiliate":
            qs = qs.filter(partner__partner_type="AFF")
        elif dept == "influencer":
            qs = qs.filter(partner__partner_type="INF")

    if advertiser_ids:
        qs = qs.filter(advertiser_id__in=advertiser_ids)

    if partner_ids:
        qs = qs.filter(partner_id__in=partner_ids)
    
    if partner_type:
        qs = qs.filter(partner__partner_type=partner_type)

    if coupon_codes:
        qs = qs.filter(coupon__code__in=coupon_codes)

    if date_from:
        d = parse_date(date_from)
        if d:
            qs = qs.filter(date__gte=d)

    if date_to:
        d = parse_date(date_to)
        if d:
            qs = qs.filter(date__lte=d)

    # -------------------------------
    # Role-based access
    # -------------------------------
    if role not in {"Admin", "OpsManager"}:
        assignments = AccountAssignment.objects.filter(company_user=company_user)
        advertiser_ids = set()
        partner_ids = set()

        for a in assignments:
            advertiser_ids.update(a.advertisers.values_list("id", flat=True))
            partner_ids.update(a.partners.values_list("id", flat=True))

        # If TeamMember has NO assignments, return empty queryset
        if not advertiser_ids and not partner_ids:
            qs = qs.none()
        else:
            if advertiser_ids:
                qs = qs.filter(advertiser_id__in=list(advertiser_ids))
            if partner_ids:
                qs = qs.filter(partner_id__in=list(partner_ids))

    # ======================================================
    # ADMIN & OPS MANAGER RESPONSE
    # (includes partner + revenue + profit)
    # ======================================================
    if role in {"Admin", "OpsManager"}:
        data = qs.annotate(
            campaign=F("advertiser__name"),
            coupon_code=F("coupon__code"),
            partner_name=F("partner__name"),
            partner_type_value=F("partner__partner_type"),
        ).values(
            "date",
            "advertiser_id",
            "partner_id",
            "campaign",
            "coupon_code",
            "partner_name",
            "partner_type_value",
            "total_orders",
            "total_sales",
            "total_revenue",
            "total_payout",
        )

        # Build lookup dicts for MB spend allocation
        # Match by (date, advertiser, partner, coupon) and sum across all platforms
        mb_spend_lookup = {}  # {(date, advertiser_id, partner_id, coupon_code): total_spend}
        mb_revenue_totals = {}  # {(date, advertiser_id, partner_id, coupon_code): total_revenue}
        
        # If we have any MB records, fetch their spend data
        mb_records = [r for r in data if r["partner_type_value"] == "MB"]
        
        if mb_records:
            from django.db.models import Q
            
            # Get unique combinations for MB records
            mb_keys = set()
            for r in mb_records:
                mb_keys.add((r["date"], r["advertiser_id"], r["partner_id"]))
            
            # Get MB spend records
            spend_conditions = Q()
            for date, adv_id, part_id in mb_keys:
                spend_conditions |= Q(date=date, advertiser_id=adv_id, partner_id=part_id)
            
            if spend_conditions:
                # Get spend grouped by date/advertiser/partner (sum across platforms and coupons)
                spends = MediaBuyerDailySpend.objects.filter(spend_conditions)
                
                for s in spends:
                    key = (s.date, s.advertiser_id, s.partner_id)
                    mb_spend_lookup[key] = mb_spend_lookup.get(key, 0) + float(s.amount_spent or 0)
            
            # Calculate total revenue per (date, advertiser, partner) for MB records
            for r in mb_records:
                key = (r["date"], r["advertiser_id"], r["partner_id"])
                revenue = float(r["total_revenue"] or 0)
                mb_revenue_totals[key] = mb_revenue_totals.get(key, 0) + revenue

        result = []
        for r in data:
            revenue = float(r["total_revenue"] or 0)
            original_payout = float(r["total_payout"] or 0)
            
            # For MB partners, payout = MB spend (cost) matched by date/advertiser/partner
            # For AFF/INF partners, payout = their actual payout
            if r["partner_type_value"] == "MB":
                # Match spend by (date, advertiser, partner) - distributed across all coupons
                key = (r["date"], r["advertiser_id"], r["partner_id"])
                total_spend = mb_spend_lookup.get(key, 0)
                total_revenue_for_key = mb_revenue_totals.get(key, 1)  # Avoid division by zero
                
                # Allocate spend proportionally based on this row's revenue
                # (distributed across all partner's coupons for this advertiser/date)
                if total_revenue_for_key > 0:
                    payout = total_spend * (revenue / total_revenue_for_key)
                else:
                    payout = 0
            else:
                payout = original_payout
            
            # Now profit = revenue - payout works for all types
            profit = revenue - payout
            
            # For all partner types: spend column shows their cost
            # MB: spend = media buying cost, AFF/INF: spend = payout (what we pay them)
            result.append({
                "date": r["date"],
                "advertiser_id": r["advertiser_id"],
                "partner_id": r["partner_id"],
                "campaign": r["campaign"],
                "coupon": r["coupon_code"],
                "partner": r["partner_name"],
                "partner_type": r["partner_type_value"],
                "orders": int(r["total_orders"] or 0),
                "sales": float(r["total_sales"] or 0),
                "revenue": revenue,
                "payout": payout,
                "spend": payout,  # Spend = cost for all types (MB spend or AFF/INF payout)
                "profit": profit,
            })
        
        # Apply pagination for Admin/OpsManager
        paginator = PerformanceTablePagination()
        paginated_result = paginator.paginate_queryset(result, request)
        return paginator.get_paginated_response(paginated_result)

    # ======================================================
    # MEMBER RESPONSE
    # ======================================================
    data = qs.annotate(
        campaign=F("advertiser__name"),
        coupon_code=F("coupon__code"),
        partner_type_value=F("partner__partner_type"),
    ).values(
        "date",
        "advertiser_id",
        "partner_id",
        "campaign",
        "coupon_code",
        "partner_type_value",
        "total_orders",
        "total_sales",
        "total_revenue",
        "total_payout",
    )

    # Get spend data for media buyers (by date, advertiser, partner, coupon)
    is_media_buyer = company_user and company_user.department == "media_buying"
    spend_dict = {}
    daily_revenue_dict = {}
    
    if is_media_buyer:
        # Get spend data - filter by the exact date/advertiser/partner combinations
        spend_keys = qs.values_list('date', 'advertiser_id', 'partner_id').distinct()
        
        from django.db.models import Q
        spend_conditions = Q()
        for date, adv_id, part_id in spend_keys:
            spend_conditions |= Q(date=date, advertiser_id=adv_id, partner_id=part_id)
        
        if spend_conditions:
            spend_qs = MediaBuyerDailySpend.objects.filter(spend_conditions)
            # Build lookup dict: (date, advertiser_id, partner_id) -> total spend
            for spend in spend_qs:
                key = (str(spend.date), spend.advertiser_id, spend.partner_id)
                spend_dict[key] = spend_dict.get(key, 0) + float(spend.amount_spent or 0)
        
        # Calculate total revenue per day/advertiser/partner for proportional distribution
        for r in data:
            key = (str(r["date"]), r["advertiser_id"], r["partner_id"])
            revenue = float(r["total_revenue"] or 0)
            if key not in daily_revenue_dict:
                daily_revenue_dict[key] = 0
            daily_revenue_dict[key] += revenue

    result = []
    for r in data:
        company_revenue = float(r["total_revenue"] or 0)
        partner_payout = float(r["total_payout"] or 0)
        
        # For media buyers, show company revenue and their spend distributed across all coupons
        if is_media_buyer:
            key = (str(r["date"]), r["advertiser_id"], r["partner_id"])
            daily_spend = spend_dict.get(key, 0)
            daily_revenue = daily_revenue_dict.get(key, 0)
            
            # Proportionally distribute spend based on this row's revenue
            if daily_revenue > 0:
                allocated_spend = (company_revenue / daily_revenue) * daily_spend
            else:
                allocated_spend = 0
            
            row = {
                "date": r["date"],
                "advertiser_id": r["advertiser_id"],
                "campaign": r["campaign"],
                "coupon": r["coupon_code"],
                "partner_type": "MB",
                "orders": int(r["total_orders"] or 0),
                "sales": float(r["total_sales"] or 0),
                "revenue": company_revenue,
                "payout": allocated_spend,
                "spend": allocated_spend,
                "profit": company_revenue - allocated_spend,
            }
        else:
            # For affiliates/influencers, show their payout (what they earn)
            row = {
                "date": r["date"],
                "advertiser_id": r["advertiser_id"],
                "campaign": r["campaign"],
                "coupon": r["coupon_code"],
                "partner_type": r.get("partner_type_value", "AFF"),  # AFF or INF
                "orders": int(r["total_orders"] or 0),
                "sales": float(r["total_sales"] or 0),
                "payout": partner_payout,
                "spend": partner_payout,  # Show their commission as spend (cost to company)
            }
        
        result.append(row)

    # Apply pagination
    paginator = PerformanceTablePagination()
    paginated_result = paginator.paginate_queryset(result, request)
    
    return paginator.get_paginated_response(paginated_result)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def dashboard_filter_options_view(request):
    """
    Returns all available filter options for the dashboard based on user permissions.
    This ensures dropdowns show all available options, not just what's in the current page.
    """
    user = request.user
    company_user = CompanyUser.objects.select_related("role").filter(user=user).first()
    role = company_user.role.name if company_user and company_user.role else None
    department = company_user.department if company_user else None

    # Get base queryset with same logic as performance_table_view
    qs = CampaignPerformance.objects.all()

    # Check if explicit partner_type filter is provided
    partner_type = request.GET.get("partner_type")

    # Department scoping (only for OpsManager with department)
    # BUT: Skip if explicit partner_type filter is provided (user's choice takes precedence)
    # ViewOnly with department and TeamMembers will be filtered by AccountAssignment later
    if company_user and department and role == "OpsManager" and not partner_type:
        if department == "affiliate":
            qs = qs.filter(partner__partner_type="AFF")
        elif department == "influencer":
            qs = qs.filter(partner__partner_type="INF")
        elif department == "media_buying":
            qs = qs.filter(partner__partner_type="MB")

    # Role-based filtering
    # Full access: Admin (no dept), OpsManager (no dept), ViewOnly (no dept)
    full_access_roles = {"Admin", "OpsManager"}
    has_full_access = (role in full_access_roles and not department) or (role == "ViewOnly" and not department)
    
    if not has_full_access:
        assignments = AccountAssignment.objects.filter(
            company_user=company_user
        ).prefetch_related("advertisers", "partners")

        advertiser_ids = set()
        partner_ids = set()
        for a in assignments:
            advertiser_ids.update(a.advertisers.values_list("id", flat=True))
            partner_ids.update(a.partners.values_list("id", flat=True))

        # If TeamMember has NO assignments, return empty queryset
        if not advertiser_ids and not partner_ids:
            qs = qs.none()
        else:
            if advertiser_ids:
                qs = qs.filter(advertiser__id__in=advertiser_ids)
            if partner_ids:
                qs = qs.filter(partner__id__in=partner_ids)

    # Extract unique filter options
    advertisers_map = {}
    partners_map = {}
    coupons_map = {}

    for cp in qs.select_related('advertiser', 'partner', 'coupon'):
        # Advertisers
        if cp.advertiser_id not in advertisers_map:
            advertisers_map[cp.advertiser_id] = {
                "advertiser_id": cp.advertiser_id,
                "campaign": cp.advertiser.name if cp.advertiser else "Unknown"
            }
        
        # Partners
        if cp.partner_id and cp.partner_id not in partners_map:
            partners_map[cp.partner_id] = {
                "partner_id": cp.partner_id,
                "partner": cp.partner.name if cp.partner else "Unknown"
            }
        
        # Coupons
        if cp.coupon and cp.coupon.code not in coupons_map:
            coupons_map[cp.coupon.code] = {
                "coupon": cp.coupon.code,
                "advertiser_id": cp.advertiser_id,
                "partner_id": cp.partner_id,
                "partner_type": cp.partner.partner_type if cp.partner else None
            }

    result = {
        "advertisers": list(advertisers_map.values()),
        "partners": list(partners_map.values()),
        "coupons": list(coupons_map.values())
    }

    return Response(result)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def dashboard_pie_chart_data_view(request):
    """
    Returns top campaigns data for pie chart across ALL records (not paginated).
    Applies same filters as performance table but aggregates across full dataset.
    """
    user = request.user
    company_user = CompanyUser.objects.select_related("role").filter(user=user).first()
    role = company_user.role.name if company_user and company_user.role else None

    # Get filters from request - SUPPORT MULTIPLE VALUES
    # Support both naming conventions: date_from/date_to and start_date/end_date
    start_date_str = request.GET.get("date_from") or request.GET.get("start_date")
    end_date_str = request.GET.get("date_to") or request.GET.get("end_date")
    advertiser_ids = request.GET.getlist("advertiser_id")  # Support multiple
    partner_ids = request.GET.getlist("partner_id")        # Support multiple
    coupon = request.GET.get("coupon")
    partner_type = request.GET.get("partner_type")

    qs = CampaignPerformance.objects.all()

    # Department scoping (only for OpsManager with department)
    # BUT: Skip if explicit partner_type filter is provided (user's choice takes precedence)
    # ViewOnly with department and TeamMembers will be filtered by AccountAssignment later
    department = company_user.department if company_user else None
    if company_user and department and role == "OpsManager" and not partner_type:
        if department == "affiliate":
            qs = qs.filter(partner__partner_type="AFF")
        elif department == "influencer":
            qs = qs.filter(partner__partner_type="INF")
        elif department == "media_buying":
            qs = qs.filter(partner__partner_type="MB")

    # Role-based filtering
    # Full access: Admin (no dept), OpsManager (no dept), ViewOnly (no dept)
    full_access_roles = {"Admin", "OpsManager"}
    has_full_access = (role in full_access_roles and not department) or (role == "ViewOnly" and not department)
    
    if not has_full_access:
        assignments = AccountAssignment.objects.filter(
            company_user=company_user
        ).prefetch_related("advertisers", "partners")

        assigned_advertiser_ids = set()
        assigned_partner_ids = set()
        for a in assignments:
            assigned_advertiser_ids.update(a.advertisers.values_list("id", flat=True))
            assigned_partner_ids.update(a.partners.values_list("id", flat=True))

        # If TeamMember has NO assignments, return empty queryset
        if not assigned_advertiser_ids and not assigned_partner_ids:
            qs = qs.none()
        else:
            if assigned_advertiser_ids:
                qs = qs.filter(advertiser__id__in=assigned_advertiser_ids)
            if assigned_partner_ids:
                qs = qs.filter(partner__id__in=assigned_partner_ids)

    # Apply user filters - SUPPORT MULTIPLE VALUES
    if start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
            qs = qs.filter(date__gte=start_date)
        except:
            pass

    if end_date_str:
        try:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
            qs = qs.filter(date__lte=end_date)
        except:
            pass

    if advertiser_ids:
        qs = qs.filter(advertiser_id__in=advertiser_ids)

    if partner_ids:
        qs = qs.filter(partner_id__in=partner_ids)

    if coupon:
        qs = qs.filter(coupon=coupon)

    # Aggregate by campaign
    campaign_totals = {}
    for cp in qs.select_related('advertiser'):
        # Skip records with NULL advertiser
        if not cp.advertiser:
            continue
        campaign_name = cp.advertiser.name
        if campaign_name not in campaign_totals:
            campaign_totals[campaign_name] = {
                "campaign": campaign_name,
                "total_revenue": 0
            }
        campaign_totals[campaign_name]["total_revenue"] += float(cp.total_revenue or 0)

    # Sort by total_revenue and take top 10
    sorted_campaigns = sorted(
        campaign_totals.values(),
        key=lambda x: x["total_revenue"],
        reverse=True
    )[:10]

    return Response(sorted_campaigns)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def advertiser_detail_summary_view(request):
    """
    Returns detailed summary for a specific advertiser when user clicks on pie chart slice.
    Includes: KPIs, partner breakdown by type, top 5 coupons, and daily revenue trend.
    Respects role-based access control and department filtering.
    """
    user = request.user
    company_user = CompanyUser.objects.select_related("role").filter(user=user).first()
    role = company_user.role.name if company_user and company_user.role else None
    department = company_user.department if company_user else None

    # Get filters from request
    advertiser_id = request.GET.get('advertiser_id')
    date_from = request.GET.get('date_from')
    date_to = request.GET.get('date_to')

    if not advertiser_id:
        return Response({"detail": "advertiser_id is required"}, status=status.HTTP_400_BAD_REQUEST)

    try:
        advertiser = Advertiser.objects.get(id=advertiser_id)
    except Advertiser.DoesNotExist:
        return Response({"detail": "Advertiser not found"}, status=status.HTTP_404_NOT_FOUND)

    # Base queryset filtered by advertiser
    qs = CampaignPerformance.objects.filter(advertiser_id=advertiser_id)

    # Apply date filters
    if date_from:
        try:
            start_date = datetime.strptime(date_from, "%Y-%m-%d").date()
            qs = qs.filter(date__gte=start_date)
        except:
            pass

    if date_to:
        try:
            end_date = datetime.strptime(date_to, "%Y-%m-%d").date()
            qs = qs.filter(date__lte=end_date)
        except:
            pass

    # Department scoping (for OpsManager with department)
    if company_user and department and role == "OpsManager":
        if department == "media_buying":
            qs = qs.filter(partner__partner_type="MB")
        elif department == "affiliate":
            qs = qs.filter(partner__partner_type="AFF")
        elif department == "influencer":
            qs = qs.filter(partner__partner_type="INF")

    # Role-based access control
    full_access_roles = {"Admin", "OpsManager"}
    has_full_access = (role in full_access_roles and not department) or (role == "ViewOnly" and not department)

    if not has_full_access:
        # TeamMembers and OpsManager/ViewOnly with department: filter by assignments
        assignments = AccountAssignment.objects.filter(
            company_user=company_user
        ).prefetch_related("advertisers", "partners")

        assigned_advertiser_ids = set()
        assigned_partner_ids = set()
        for a in assignments:
            assigned_advertiser_ids.update(a.advertisers.values_list("id", flat=True))
            assigned_partner_ids.update(a.partners.values_list("id", flat=True))

        # Verify user has access to this advertiser
        if int(advertiser_id) not in assigned_advertiser_ids:
            return Response({"detail": "Access denied to this advertiser"}, status=status.HTTP_403_FORBIDDEN)

        # Filter by assigned partners
        if assigned_partner_ids:
            qs = qs.filter(partner__id__in=assigned_partner_ids)
        else:
            qs = qs.none()

    # Calculate KPIs
    kpis = qs.aggregate(
        total_orders=Sum('total_orders'),
        total_sales=Sum('total_sales'),
        total_revenue=Sum('total_revenue'),
        total_payout=Sum('total_payout'),
        total_profit=Sum('total_profit')
    )

    # Determine if user can see profit
    can_see_profit = (
        role == "Admin" or 
        role == "OpsManager" or 
        (role == "TeamMember" and department == "media_buying")
    )

    # Partner breakdown by type
    partner_breakdown = []
    partner_types = [
        {'type': 'MB', 'label': 'Media Buyers'},
        {'type': 'AFF', 'label': 'Affiliates'},
        {'type': 'INF', 'label': 'Influencers'}
    ]

    for pt in partner_types:
        type_qs = qs.filter(partner__partner_type=pt['type'])
        type_revenue = type_qs.aggregate(total=Sum('total_revenue'))['total'] or 0
        type_count = type_qs.values('partner').distinct().count()
        
        if type_revenue > 0 or type_count > 0:
            partner_breakdown.append({
                'type': pt['type'],
                'label': pt['label'],
                'revenue': float(type_revenue),
                'count': type_count
            })

    # Top 5 performing coupons
    coupon_performance = {}
    for cp in qs.select_related('coupon', 'partner').exclude(coupon__isnull=True):
        coupon_code = cp.coupon.code
        if coupon_code not in coupon_performance:
            coupon_performance[coupon_code] = {
                'code': coupon_code,
                'partner': cp.partner.name if cp.partner else 'N/A',
                'orders': 0,
                'revenue': 0
            }
        coupon_performance[coupon_code]['orders'] += cp.total_orders or 0
        coupon_performance[coupon_code]['revenue'] += float(cp.total_revenue or 0)

    top_coupons = sorted(
        coupon_performance.values(),
        key=lambda x: x['revenue'],
        reverse=True
    )[:5]

    # Daily revenue trend
    daily_data = qs.values('date').annotate(
        revenue=Sum('total_revenue')
    ).order_by('date')

    daily_trend = {
        'dates': [str(d['date']) for d in daily_data],
        'revenues': [float(d['revenue'] or 0) for d in daily_data]
    }

    # Build response
    response_data = {
        'advertiser_id': advertiser.id,
        'advertiser_name': advertiser.name,
        'kpis': {
            'orders': kpis['total_orders'] or 0,
            'sales': float(kpis['total_sales'] or 0),
            'revenue': float(kpis['total_revenue'] or 0),
            'payout': float(kpis['total_payout'] or 0),
        },
        'partner_breakdown': partner_breakdown,
        'top_coupons': top_coupons,
        'daily_trend': daily_trend,
        'can_see_profit': can_see_profit
    }

    # Only include profit if user can see it
    if can_see_profit:
        response_data['kpis']['profit'] = float(kpis['total_profit'] or 0)

    return Response(response_data)


# --- Coupon Management ---
@api_view(["GET", "POST"])
@permission_classes([IsAuthenticated])
def coupons_view(request):
    if request.method == "GET":
        user = request.user
        company_user = CompanyUser.objects.select_related("role").filter(user=user).first()
        
        # Start with all coupons
        coupons = Coupon.objects.all()
        
        # Filter by user assignments for non-admin roles
        if company_user and company_user.role:
            role = company_user.role.name
            department = company_user.department
            
            # Full access: Admin (no dept), OpsManager (no dept), ViewOnly (no dept)
            full_access_roles = {"Admin", "OpsManager"}
            has_full_access = (role in full_access_roles and not department) or (role == "ViewOnly" and not department)
            
            if not has_full_access:
                # Get user's assigned advertisers and partners
                assignments = AccountAssignment.objects.filter(
                    company_user=company_user
                ).prefetch_related("advertisers", "partners")
                
                advertiser_ids = set()
                partner_ids = set()
                
                for a in assignments:
                    advertiser_ids.update(a.advertisers.values_list("id", flat=True))
                    partner_ids.update(a.partners.values_list("id", flat=True))
                
                # Filter coupons by both advertiser AND partner assignments
                if advertiser_ids and partner_ids:
                    # Team member sees coupons that:
                    # 1. Match their advertiser assignment AND
                    # 2. Are currently assigned to them OR were assigned to them in history
                    from django.db.models import Q
                    
                    # Get coupon IDs from history where this partner was ever assigned
                    historical_coupon_ids = CouponAssignmentHistory.objects.filter(
                        partner_id__in=list(partner_ids)
                    ).values_list('coupon_id', flat=True).distinct()
                    
                    coupons = coupons.filter(
                        advertiser_id__in=list(advertiser_ids)
                    ).filter(
                        Q(partner_id__in=list(partner_ids)) |  # Currently assigned
                        Q(id__in=list(historical_coupon_ids))   # Ever assigned in history
                    )
                elif advertiser_ids:
                    # Has advertisers but no partners - filter by advertiser only
                    coupons = coupons.filter(advertiser_id__in=list(advertiser_ids))
                else:
                    # If no advertiser assignments, return empty list
                    coupons = Coupon.objects.none()
        
        coupons = coupons.order_by("advertiser__name", "code")
        data = []
        for c in coupons:
            data.append({
                "id": c.id,
                "code": c.code,
                "advertiser": c.advertiser.name,
                "advertiser_id": c.advertiser.id,  # Add for filtering
                "partner": c.partner.name if c.partner else None,
                "partner_id": c.partner.id if c.partner else None,  # Add for filtering
                "geo": c.geo or None,  # Return None instead of "—" for empty geo
                "discount": float(c.discount_percent) if c.discount_percent else None,  # Return None instead of 0.0
            })
        return Response(data)

    if request.method == "POST":
        # Only OpsManager and Admin can create coupons
        user = request.user
        company_user = CompanyUser.objects.select_related("role").filter(user=user).first()
        if not company_user or not company_user.role or company_user.role.name not in ["Admin", "OpsManager"]:
            return Response({"error": "Access denied. Only OpsManager and Admin can create coupons."}, status=403)
        
        data = request.data
        code = data.get("code")
        advertiser_id = data.get("advertiser")
        partner_id = data.get("partner")
        geo = data.get("geo")
        discount = data.get("discount_percent")

        # Validate required fields
        if not code or not advertiser_id:
            return Response({"error": "Missing required fields: code and advertiser."}, status=400)

        try:
            advertiser = Advertiser.objects.get(id=advertiser_id)
        except Advertiser.DoesNotExist:
            return Response({"error": "Invalid advertiser ID."}, status=400)

        partner = None
        if partner_id:
            try:
                partner = Partner.objects.get(id=partner_id)
            except Partner.DoesNotExist:
                return Response({"error": "Invalid partner ID."}, status=400)

        discount_decimal = None
        if discount:
            try:
                discount_decimal = Decimal(discount)
            except:
                return Response({"error": "Invalid discount_percent format."}, status=400)

        # Check if the coupon already exists for THIS advertiser (same code can exist for different advertisers)
        if Coupon.objects.filter(code=code, advertiser=advertiser).exists():
            return Response({"error": f"Coupon {code} already exists for {advertiser.name}. Use PATCH to update."}, status=400)

        # Create a new coupon
        coupon = Coupon.objects.create(
            code=code,
            advertiser=advertiser,
            partner=partner,
            geo=geo,
            discount_percent=discount_decimal
        )

        # Log assignment history if partner is provided
        if partner:
            CouponAssignmentHistory.objects.create(
                coupon=coupon,
                partner=partner,
                assigned_by=request.user,
                discount_percent=discount_decimal,
            )

        return Response({"success": f"Coupon {coupon.code} created successfully."}, status=201)

# PATCH /api/coupons/<code>/ endpoint
# Supports optional advertiser_id query parameter to disambiguate coupons with same code
@api_view(["PATCH"])
@permission_classes([IsAuthenticated])
def coupon_detail_view(request, code):
    """
    Update an existing coupon's partner, geo, or discount.
    
    Query parameters:
    - advertiser_id (optional): If provided, only updates coupon for that advertiser.
                                Required when same coupon code exists for multiple advertisers.
    """
    # Only OpsManager and Admin can update coupons
    user = request.user
    company_user = CompanyUser.objects.select_related("role").filter(user=user).first()
    if not company_user or not company_user.role or company_user.role.name not in ["Admin", "OpsManager"]:
        return Response({"error": "Access denied. Only OpsManager and Admin can update coupons."}, status=403)
    
    from django.core.exceptions import MultipleObjectsReturned
    
    data = request.data
    advertiser_id = request.GET.get("advertiser_id") or data.get("advertiser_id")
    
    try:
        # If advertiser_id is provided, use it to find the exact coupon
        if advertiser_id:
            try:
                advertiser_id = int(advertiser_id)
                coupon = Coupon.objects.get(code=code, advertiser_id=advertiser_id)
            except (ValueError, TypeError):
                return Response({"error": "Invalid advertiser_id format."}, status=400)
        else:
            # Try to get coupon by code only
            # If multiple exist, we need advertiser_id
            try:
                coupon = Coupon.objects.get(code=code)
            except MultipleObjectsReturned:
                return Response({
                    "error": f"Multiple coupons exist with code '{code}'. Please specify advertiser_id parameter."
                }, status=400)
    except Coupon.DoesNotExist:
        return Response({"error": f"Coupon {code} not found."}, status=404)

    partner_id = data.get("partner")
    geo = data.get("geo")
    discount = data.get("discount_percent")

    updated_fields = []

    # Update geo (allow clearing by setting to None or empty string)
    if "geo" in data:
        new_geo = geo if geo and geo.strip() else None
        if coupon.geo != new_geo:
            coupon.geo = new_geo
            updated_fields.append("geo")

    # Update discount (allow clearing)
    if "discount_percent" in data:
        if discount is None or discount == "":
            if coupon.discount_percent is not None:
                coupon.discount_percent = None
                updated_fields.append("discount_percent")
        else:
            try:
                new_discount = Decimal(str(discount))
                if coupon.discount_percent != new_discount:
                    coupon.discount_percent = new_discount
                    updated_fields.append("discount_percent")
            except (ValueError, TypeError):
                return Response({"error": "Invalid discount_percent format."}, status=400)

    # Update partner (and log assignment) - allow clearing partner
    if "partner" in data:
        if partner_id is None or partner_id == "":
            # Clear partner assignment
            if coupon.partner:
                old_partner = coupon.partner
                coupon.partner = None # type: ignore
                updated_fields.append("partner")
                print(f"✅ Coupon {code}: partner cleared (was {old_partner.name})")
        else:
            try:
                partner = Partner.objects.get(id=int(partner_id))
                
                # Only update if partner is changing
                if not coupon.partner or coupon.partner.id != partner.id: # type: ignore
                    old_partner = coupon.partner
                    coupon.partner = partner # type: ignore
                    updated_fields.append("partner")

                    # Log the assignment history
                    CouponAssignmentHistory.objects.create(
                        coupon=coupon,
                        partner=partner,
                        assigned_by=request.user,
                        discount_percent=coupon.discount_percent,
                    )
                    
                    print(f"✅ Coupon {code}: partner changed from {old_partner} to {partner.name}")
                
            except Partner.DoesNotExist:
                return Response({"error": "Invalid partner ID."}, status=400)
            except (ValueError, TypeError):
                return Response({"error": "Invalid partner ID format."}, status=400)

    if updated_fields:
        coupon.save()
        return Response({
            "success": f"Coupon {coupon.code} updated: {', '.join(updated_fields)}."
        }, status=200)
    else:
        return Response({
            "info": f"No changes made to coupon {coupon.code}."
        }, status=200)

    partner_id = data.get("partner")
    geo = data.get("geo")
    discount = data.get("discount_percent")

    updated_fields = []

    # Update geo (allow clearing by setting to None or empty string)
    if "geo" in data:
        new_geo = geo if geo and geo.strip() else None
        if coupon.geo != new_geo:
            coupon.geo = new_geo
            updated_fields.append("geo")

    # Update discount (allow clearing)
    if "discount_percent" in data:
        if discount is None or discount == "":
            if coupon.discount_percent is not None:
                coupon.discount_percent = None
                updated_fields.append("discount_percent")
        else:
            try:
                new_discount = Decimal(str(discount))
                if coupon.discount_percent != new_discount:
                    coupon.discount_percent = new_discount
                    updated_fields.append("discount_percent")
            except (ValueError, TypeError):
                return Response({"error": "Invalid discount_percent format."}, status=400)

    # Update partner (and log assignment) - allow clearing partner
    if "partner" in data:
        if partner_id is None or partner_id == "":
            # Clear partner assignment
            if coupon.partner:
                old_partner = coupon.partner
                coupon.partner = None # type: ignore
                updated_fields.append("partner")
                print(f"✅ Coupon {code}: partner cleared (was {old_partner.name})")
        else:
            try:
                partner = Partner.objects.get(id=int(partner_id))
                
                # Only update if partner is changing
                if not coupon.partner or coupon.partner.id != partner.id: # type: ignore
                    old_partner = coupon.partner
                    coupon.partner = partner # type: ignore
                    updated_fields.append("partner")

                    # Log the assignment history
                    CouponAssignmentHistory.objects.create(
                        coupon=coupon,
                        partner=partner,
                        assigned_by=request.user,
                        discount_percent=coupon.discount_percent,
                    )
                    
                    print(f"✅ Coupon {code}: partner changed from {old_partner} to {partner.name}")
                
            except Partner.DoesNotExist:
                return Response({"error": "Invalid partner ID."}, status=400)
            except (ValueError, TypeError):
                return Response({"error": "Invalid partner ID format."}, status=400)

    if updated_fields:
        coupon.save()
        return Response({
            "success": f"Coupon {coupon.code} updated: {', '.join(updated_fields)}."
        }, status=200)
    else:
        return Response({
            "info": f"No changes made to coupon {coupon.code}."
        }, status=200)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def coupon_history_view(request, code):
    """
    Get assignment history for a specific coupon.
    Returns list of partner assignments with timestamps.
    """
    try:
        coupon = Coupon.objects.get(code=code)
    except Coupon.DoesNotExist:
        return Response({"error": f"Coupon {code} not found."}, status=404)
    
    history = CouponAssignmentHistory.objects.filter(
        coupon=coupon
    ).select_related('partner', 'assigned_by').order_by('-assigned_date')
    
    data = []
    for h in history:
        data.append({
            "partner": h.partner.name,
            "partner_type": h.partner.partner_type,
            "assigned_date": h.assigned_date.isoformat(),
            "assigned_by": h.assigned_by.username if h.assigned_by else None,
            "discount_percent": float(h.discount_percent) if h.discount_percent else None,
            "notes": h.notes or None,
        })
    
    return Response(data)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def partner_list_view(request):
    """
    Get list of partners with optional filtering by partner_type.
    Query params:
    - partner_type: Filter by type (MB, AFF, INF)
    """
    partner_type = request.query_params.get('partner_type')
    
    partners = Partner.objects.all()
    
    if partner_type:
        partners = partners.filter(partner_type=partner_type)
    
    results = [
        {
            "id": partner.id,
            "name": partner.name,
            "type": partner.get_partner_type_display(),
            "partner_type": partner.partner_type
        }
        for partner in partners.order_by('name')
    ]
    return Response(results)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def advertiser_list_view(request):
    advertisers = Advertiser.objects.all()
    results = []
    
    for adv in advertisers:
        # Count total partners assigned through coupons
        total_partners = Coupon.objects.filter(
            advertiser=adv,
            partner__isnull=False
        ).values('partner').distinct().count()
        
        # Count active partners (those with performance data)
        active_partners = CampaignPerformance.objects.filter(
            advertiser=adv,
            partner__isnull=False
        ).values('partner').distinct().count()
        
        results.append({
            "id": adv.id,  # type: ignore
            "name": adv.name,
            "attribution": adv.get_attribution_display(),  # type: ignore
            "rev_rate_type": adv.rev_rate_type,
            "rev_ftu_rate": adv.rev_ftu_rate,
            "rev_rtu_rate": adv.rev_rtu_rate,
            "rev_ftu_fixed_bonus": adv.rev_ftu_fixed_bonus,
            "rev_rtu_fixed_bonus": adv.rev_rtu_fixed_bonus,
            "currency": adv.currency,
            "exchange_rate": adv.exchange_rate,
            "default_payout_rate_type": adv.default_payout_rate_type,
            "default_ftu_payout": adv.default_ftu_payout,
            "default_rtu_payout": adv.default_rtu_payout,
            "default_ftu_fixed_bonus": adv.default_ftu_fixed_bonus,
            "default_rtu_fixed_bonus": adv.default_rtu_fixed_bonus,
            "partner_payouts": [
                {
                    "partner_id": pp.partner.id,  # type: ignore
                    "partner_name": pp.partner.name,  # type: ignore
                    "rate_type": pp.rate_type,
                    "ftu_payout": pp.ftu_payout,
                    "rtu_payout": pp.rtu_payout,
                    "ftu_fixed_bonus": pp.ftu_fixed_bonus,
                    "rtu_fixed_bonus": pp.rtu_fixed_bonus,
                }
                for pp in adv.partner_payouts.select_related('partner').all()  # type: ignore
            ],
            "total_partners": total_partners,
            "active_partners": active_partners,
        })
    
    return Response(results)

# --- Partner Payout Management ---
@api_view(["GET", "POST"])
@permission_classes([IsAuthenticated])
def partner_payouts_view(request):
    if request.method == "GET":
        advertiser_id = request.GET.get("advertiser_id")
        partner_id = request.GET.get("partner_id")

        qs = PartnerPayout.objects.select_related("advertiser", "partner").order_by("-id")

        if advertiser_id:
            qs = qs.filter(advertiser_id=advertiser_id)
        if partner_id:
            qs = qs.filter(partner_id=partner_id)

        payouts = []
        for p in qs:
            payouts.append({
                "id": p.id,# type: ignore
                "advertiser": p.advertiser.name,
                "advertiser_id": p.advertiser.id,# type: ignore
                "partner": p.partner.name if p.partner else "Default",
                "partner_id": p.partner.id if p.partner else None,# type: ignore
                "ftu_payout": float(p.ftu_payout) if p.ftu_payout else None,
                "rtu_payout": float(p.rtu_payout) if p.rtu_payout else None,
                "ftu_fixed_bonus": float(p.ftu_fixed_bonus) if p.ftu_fixed_bonus else None,
                "rtu_fixed_bonus": float(p.rtu_fixed_bonus) if p.rtu_fixed_bonus else None,
                "exchange_rate": float(p.exchange_rate) if p.exchange_rate else None,
                "currency": p.currency,
                "rate_type": p.rate_type,
                "condition": p.condition,
                "start_date": str(p.start_date) if p.start_date else None,
                "end_date": str(p.end_date) if p.end_date else None,
            })
        return Response(payouts)

    if request.method == "POST":
        data = request.data
        advertiser_id = data.get("advertiser")
        partner_id = data.get("partner")
        ftu_payout = data.get("ftu_payout")
        rtu_payout = data.get("rtu_payout")
        exchange_rate = data.get("exchange_rate")
        currency = data.get("currency")
        rate_type = data.get("rate_type", "percent")
        condition = data.get("condition")
        start_date = data.get("start_date")
        end_date = data.get("end_date")

        if not advertiser_id:
            return Response({"error": "Advertiser is required."}, status=400)

        try:
            advertiser = Advertiser.objects.get(id=advertiser_id)
        except Advertiser.DoesNotExist:
            return Response({"error": "Invalid advertiser ID."}, status=400)

        partner = None
        if partner_id:
            try:
                partner = Partner.objects.get(id=partner_id)
            except Partner.DoesNotExist:
                return Response({"error": "Invalid partner ID."}, status=400)

        ftu_fixed_bonus = data.get("ftu_fixed_bonus")
        rtu_fixed_bonus = data.get("rtu_fixed_bonus")

        payout_obj, created = PartnerPayout.objects.update_or_create(
            advertiser=advertiser,
            partner=partner,
            start_date=start_date or None,
            defaults={
                "ftu_payout": ftu_payout or None,
                "rtu_payout": rtu_payout or None,
                "ftu_fixed_bonus": ftu_fixed_bonus or None,
                "rtu_fixed_bonus": rtu_fixed_bonus or None,
                "exchange_rate": exchange_rate or None,
                "currency": currency,
                "rate_type": rate_type,
                "condition": condition,
                "end_date": end_date or None,
            }
        )

        action = "created" if created else "updated"
        return Response({"success": f"Payout {action} successfully for {advertiser.name} → {partner.name if partner else 'Default'}."})


@api_view(["DELETE"])
@permission_classes([IsAuthenticated])
def partner_payout_detail_view(request, pk):
    try:
        payout = PartnerPayout.objects.get(pk=pk)
        payout.delete()
        return Response({"success": "Partner payout deleted successfully."})
    except PartnerPayout.DoesNotExist:
        return Response({"error": "Partner payout not found."}, status=404)


# ============ DEPARTMENT TARGETS API ============

from .models import DepartmentTarget
from .serializers import DepartmentTargetSerializer
from rest_framework import status # type: ignore

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def team_members_list(request):
    """
    Get list of team members (CompanyUser) for dropdown selection.
    Returns list of team members with ID and username.
    Optional query parameter: department (MB, AFF, INF) to filter by partner_type
    """
    try:
        user = request.user
        company_user = CompanyUser.objects.select_related("role").filter(user=user).first()
        
        if not company_user:
            return Response({"error": "User not found"}, status=404)
        
        # Check if filtering by department/partner_type
        department_filter = request.query_params.get('department')
        
        # Map partner_type abbreviations to department names
        department_map = {
            "MB": "media_buying",
            "AFF": "affiliate",
            "INF": "influencer"
        }
        
        # Determine which department to filter by
        if department_filter and department_filter in department_map:
            filter_dept = department_map[department_filter]
        elif company_user.department:
            filter_dept = company_user.department
        else:
            # Admin with no department - return all
            filter_dept = None
        
        # Get team members
        if filter_dept:
            team_members = CompanyUser.objects.filter(
                department=filter_dept
            ).select_related("user").values("id", "user__username", "user__first_name", "user__last_name")
        else:
            # Return all company users (for admin)
            team_members = CompanyUser.objects.select_related("user").values("id", "user__username", "user__first_name", "user__last_name")
        
        members_list = []
        for member in team_members:
            full_name = f"{member['user__first_name']} {member['user__last_name']}".strip()
            members_list.append({
                "id": member['id'],
                "username": member['user__username'],
                "name": full_name or member['user__username']
            })
        
        return Response(members_list, status=200)
    except Exception as e:
        print(f"Error fetching team members: {e}")
        return Response({"error": str(e)}, status=400)




@api_view(['GET', 'POST'])
@permission_classes([IsAuthenticated])
def targets_list(request):
    """
    GET: List all targets (with optional filtering)
    POST: Create new target
    """
    if request.method == 'GET':
        targets = DepartmentTarget.objects.all().select_related('advertiser').order_by('-month', 'advertiser__name', 'partner_type')
        
        # Role-based filtering
        user = request.user
        company_user = CompanyUser.objects.select_related("role").filter(user=user).first()
        
        if company_user and company_user.role:
            role = company_user.role.name
            
            # For non-admin roles, filter by department and assigned_to
            if role not in {"Admin", "OpsManager"}:
                from django.db.models import Q
                if company_user.department:
                    # Team member sees:
                    # 1. Department-level targets for their department (assigned_to is null)
                    # 2. Individual targets assigned to them
                    targets = targets.filter(
                        partner_type__in={
                            "media_buying": "MB",
                            "affiliate": "AFF", 
                            "influencer": "INF"
                        }.get(company_user.department, company_user.department)
                    ).filter(
                        Q(assigned_to__isnull=True) | Q(assigned_to=company_user)
                    )
                else:
                    targets = DepartmentTarget.objects.none()
        
        # Optional filters
        advertiser_id = request.query_params.get('advertiser_id')
        partner_type = request.query_params.get('partner_type')
        month = request.query_params.get('month')
        
        if advertiser_id:
            targets = targets.filter(advertiser_id=advertiser_id)
        if partner_type:
            targets = targets.filter(partner_type=partner_type)
        if month:
            targets = targets.filter(month=month)
        
        serializer = DepartmentTargetSerializer(targets, many=True)
        return Response(serializer.data)
    
    elif request.method == 'POST':
        # Only OpsManager and Admin can create targets
        user = request.user
        company_user = CompanyUser.objects.select_related("role").filter(user=user).first()
        if not company_user or not company_user.role or company_user.role.name not in ["Admin", "OpsManager"]:
            return Response({"error": "Access denied. Only OpsManager and Admin can create targets."}, status=status.HTTP_403_FORBIDDEN)
        
        serializer = DepartmentTargetSerializer(data=request.data)
        if serializer.is_valid():
            instance = serializer.save()
            # Re-fetch the instance to include related data in response
            instance = DepartmentTarget.objects.select_related('advertiser').get(pk=instance.pk)
            response_serializer = DepartmentTargetSerializer(instance)
            return Response(response_serializer.data, status=status.HTTP_201_CREATED)
        print(f"Serializer errors: {serializer.errors}")
        print(f"Request data: {request.data}")
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET', 'PUT', 'DELETE'])
@permission_classes([IsAuthenticated])
def target_detail(request, pk):
    """
    GET: Retrieve target by ID
    PUT: Update target
    DELETE: Delete target
    """
    try:
        target = DepartmentTarget.objects.select_related('advertiser').get(pk=pk)
    except DepartmentTarget.DoesNotExist:
        return Response({"error": "Target not found"}, status=status.HTTP_404_NOT_FOUND)
    
    if request.method == 'GET':
        serializer = DepartmentTargetSerializer(target)
        return Response(serializer.data)
    
    elif request.method == 'PUT':
        # Only OpsManager and Admin can update targets
        user = request.user
        company_user = CompanyUser.objects.select_related("role").filter(user=user).first()
        if not company_user or not company_user.role or company_user.role.name not in ["Admin", "OpsManager"]:
            return Response({"error": "Access denied. Only OpsManager and Admin can update targets."}, status=status.HTTP_403_FORBIDDEN)
        
        serializer = DepartmentTargetSerializer(target, data=request.data, partial=True)
        if serializer.is_valid():
            instance = serializer.save()
            # Re-fetch with related data
            instance = DepartmentTarget.objects.select_related('advertiser').get(pk=instance.pk)
            response_serializer = DepartmentTargetSerializer(instance)
            return Response(response_serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    elif request.method == 'DELETE':
        # Only OpsManager and Admin can delete targets
        user = request.user
        company_user = CompanyUser.objects.select_related("role").filter(user=user).first()
        if not company_user or not company_user.role or company_user.role.name not in ["Admin", "OpsManager"]:
            return Response({"error": "Access denied. Only OpsManager and Admin can delete targets."}, status=status.HTTP_403_FORBIDDEN)
        
        target.delete()
        return Response({"success": "Target deleted"}, status=status.HTTP_204_NO_CONTENT)
    
    
    
    


# ============ PERFORMANCE ANALYTICS API ============

def get_department_breakdown(month_start, month_end, advertiser_ids=None, partner_ids=None):
    """Calculate performance breakdown by department"""
    departments = {
        "MB": {"partner_type": "MB", "name": "Media Buying"},
        "AFF": {"partner_type": "AFF", "name": "Affiliate"},
        "INF": {"partner_type": "INF", "name": "Influencer"}
    }
    
    breakdown = []
    
    for dept_code, dept_info in departments.items():
        dept_qs = CampaignPerformance.objects.filter(
            date__gte=month_start,
            date__lte=month_end,
            partner__partner_type=dept_info["partner_type"]
        )
        
        if advertiser_ids:
            dept_qs = dept_qs.filter(advertiser_id__in=advertiser_ids)
        if partner_ids:
            dept_qs = dept_qs.filter(partner_id__in=partner_ids)
        
        dept_agg = dept_qs.aggregate(
            total_orders=Sum("total_orders"),
            total_revenue=Sum("total_revenue"),
            total_payout=Sum("total_payout")
        )
        
        orders = dept_agg["total_orders"] or 0
        revenue = float(dept_agg["total_revenue"] or 0)
        original_payout = float(dept_agg["total_payout"] or 0)
        
        # For media buyers, payout = spend and profit = revenue - spend
        # For AFF/INF, payout = actual payout and profit = revenue - payout
        if dept_code == "MB":
            # Get MB spend for this department
            spend_keys = dept_qs.values_list('date', 'advertiser_id', 'partner_id').distinct()
            
            from django.db.models import Q
            spend_conditions = Q()
            for date_val, adv_id, part_id in spend_keys:
                spend_conditions |= Q(date=date_val, advertiser_id=adv_id, partner_id=part_id)
            
            if spend_conditions:
                spend_qs = MediaBuyerDailySpend.objects.filter(spend_conditions)
                spend_agg = spend_qs.aggregate(total_spend=Sum("amount_spent"))
                payout = float(spend_agg["total_spend"] or 0)
            else:
                payout = 0
            profit = revenue - payout
        else:
            # For AFF/INF, profit = revenue - payout
            payout = original_payout
            profit = revenue - payout
        
        # Get targets for this department - sum ALL targets (both department-level and individual)
        target_filters = {
            "month": month_start,
            "partner_type": dept_info["partner_type"]
        }
        if advertiser_ids:
            dept_targets = DepartmentTarget.objects.filter(**target_filters, advertiser_id__in=advertiser_ids)
        else:
            dept_targets = DepartmentTarget.objects.filter(**target_filters)
        
        if dept_targets.exists():
            target_agg = dept_targets.aggregate(
                total_orders=Sum('orders_target'),
                total_revenue=Sum('revenue_target'),
                total_profit=Sum('profit_target')
            )
            orders_target = int(target_agg['total_orders']) if target_agg['total_orders'] is not None else None
            revenue_target = float(target_agg['total_revenue']) if target_agg['total_revenue'] is not None else None
            profit_target = float(target_agg['total_profit']) if target_agg['total_profit'] is not None else None
            
            orders_pct = (orders / orders_target * 100) if orders_target and orders_target > 0 else 0
            revenue_pct = (revenue / revenue_target * 100) if revenue_target and revenue_target > 0 else 0
            profit_pct = (profit / profit_target * 100) if profit_target and profit_target > 0 else 0
        else:
            orders_target = None
            revenue_target = None
            profit_target = None
            orders_pct = None
            revenue_pct = None
            profit_pct = None
        
        breakdown.append({
            "code": dept_code,
            "name": dept_info["name"],
            "orders": int(orders),
            "revenue": round(revenue, 2),
            "profit": round(profit, 2),
            "payout": round(payout, 2),
            "targets": {
                "orders": orders_target,
                "revenue": round(revenue_target, 2) if revenue_target is not None else None,
                "profit": round(profit_target, 2) if profit_target is not None else None
            },
            "achievement": {
                "orders_pct": round(orders_pct, 2) if orders_pct is not None else None,
                "revenue_pct": round(revenue_pct, 2) if revenue_pct is not None else None,
                "profit_pct": round(profit_pct, 2) if profit_pct is not None else None
            }
        })
    
    return breakdown


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def performance_analytics_view(request):
    """
    Advanced performance analytics with MTD, run rate, pacing, and ROI calculations.
    Query params:
    - advertiser_id: Filter by advertiser
    - partner_id: Filter by partner
    - partner_type: Filter by partner type (MB, AFF, INF)
    - month: Target month (YYYY-MM-DD format, first day of month)
    """
    user = request.user
    company_user = CompanyUser.objects.select_related("role").filter(user=user).first()
    
    # Get filter parameters - support multiple values
    advertiser_ids = request.GET.getlist('advertiser_id')
    advertiser_ids = [int(aid) for aid in advertiser_ids if aid.isdigit()] if advertiser_ids else None
    
    partner_ids = request.GET.getlist('partner_id')
    partner_ids = [int(pid) for pid in partner_ids if pid.isdigit()] if partner_ids else None
    
    partner_type = request.GET.get('partner_type')
    month_param = request.GET.get('month')
    
    print(f"🔍 ANALYTICS DEBUG: advertiser_ids={advertiser_ids}, partner_ids={partner_ids}, partner_type={partner_type}")
    
    # Determine target month (default to current month)
    if month_param:
        try:
            # Try YYYY-MM format first (from month selector)
            if len(month_param) == 7 and month_param[4] == '-':
                target_month = datetime.strptime(month_param, '%Y-%m').date()
            else:
                # Fall back to YYYY-MM-DD format
                target_month = datetime.strptime(month_param, '%Y-%m-%d').date()
            target_month = target_month.replace(day=1)
        except:
            target_month = date.today().replace(day=1)
    else:
        target_month = date.today().replace(day=1)
    
    # Calculate month boundaries
    days_in_month = monthrange(target_month.year, target_month.month)[1]
    month_start = target_month
    month_end = target_month.replace(day=days_in_month)
    today = date.today()
    
    # Days elapsed (capped at days_in_month)
    if today.month == target_month.month and today.year == target_month.year:
        days_elapsed = today.day
    elif today < month_start:
        days_elapsed = 0
    else:
        days_elapsed = days_in_month
    
    days_remaining = days_in_month - days_elapsed
    
    # Base queryset for performance data
    perf_qs = CampaignPerformance.objects.filter(
        date__gte=month_start,
        date__lte=month_end
    )
    
    # Apply filters first
    if advertiser_ids:
        perf_qs = perf_qs.filter(advertiser_id__in=advertiser_ids)
    if partner_ids:
        perf_qs = perf_qs.filter(partner_id__in=partner_ids)
    if partner_type:
        perf_qs = perf_qs.filter(partner__partner_type=partner_type)
    
    # Apply department scoping (only if no explicit partner_type filter)
    # User's explicit filter choice takes precedence over automatic scoping
    if company_user and company_user.department and not partner_type:
        dept = company_user.department
        if dept == "media_buying":
            perf_qs = perf_qs.filter(partner__partner_type="MB")
            partner_type = "MB"
        elif dept == "affiliate":
            perf_qs = perf_qs.filter(partner__partner_type="AFF")
            partner_type = "AFF"
        elif dept == "influencer":
            perf_qs = perf_qs.filter(partner__partner_type="INF")
            partner_type = "INF"
    
    # Role-based access control
    if company_user and company_user.role:
        role = company_user.role.name
        department = company_user.department
        
        # Full access: Admin (no dept), OpsManager (no dept), ViewOnly (no dept)
        full_access_roles = {"Admin", "OpsManager"}
        has_full_access = (role in full_access_roles and not department) or (role == "ViewOnly" and not department)
        
        if not has_full_access:
            assignments = AccountAssignment.objects.filter(company_user=company_user).prefetch_related("advertisers", "partners")
            adv_ids = set()
            part_ids = set()
            
            for a in assignments:
                adv_ids.update(a.advertisers.values_list("id", flat=True))
                part_ids.update(a.partners.values_list("id", flat=True))
            
            # If TeamMember has NO assignments, return empty queryset
            if not adv_ids and not part_ids:
                perf_qs = perf_qs.none()
            else:
                if adv_ids:
                    perf_qs = perf_qs.filter(advertiser_id__in=list(adv_ids))
                if part_ids:
                    perf_qs = perf_qs.filter(partner_id__in=list(part_ids))
    
    # Calculate MTD actuals
    mtd_agg = perf_qs.aggregate(
        total_orders=Sum("total_orders"),
        total_revenue=Sum("total_revenue"),
        total_payout=Sum("total_payout"),
        total_sales=Sum("total_sales")
    )
    
    mtd_orders = mtd_agg["total_orders"] or 0
    mtd_revenue = float(mtd_agg["total_revenue"] or 0)
    mtd_sales = float(mtd_agg["total_sales"] or 0)
    mtd_payout = float(mtd_agg["total_payout"] or 0)
    
    # Get MTD spend for media buyers
    is_media_buyer = company_user and company_user.department == "media_buying"
    if is_media_buyer:
        # Filter spend by the exact date/advertiser/partner combinations in the filtered data
        spend_keys = perf_qs.values_list('date', 'advertiser_id', 'partner_id').distinct()
        
        from django.db.models import Q
        spend_conditions = Q()
        for date_val, adv_id, part_id in spend_keys:
            spend_conditions |= Q(date=date_val, advertiser_id=adv_id, partner_id=part_id)
        
        if spend_conditions:
            spend_qs = MediaBuyerDailySpend.objects.filter(spend_conditions)
            spend_agg = spend_qs.aggregate(total_spend=Sum("amount_spent"))
            mtd_spend = float(spend_agg["total_spend"] or 0)
        else:
            mtd_spend = 0
        mtd_profit = mtd_revenue - mtd_spend
    else:
        mtd_spend = 0
        mtd_profit = mtd_revenue - mtd_payout
    
    # Get today's performance
    today_agg = perf_qs.filter(date=today).aggregate(
        total_orders=Sum("total_orders"),
        total_revenue=Sum("total_revenue"),
        total_payout=Sum("total_payout")
    )
    
    today_orders = today_agg["total_orders"] or 0
    today_revenue = float(today_agg["total_revenue"] or 0)
    today_payout = float(today_agg["total_payout"] or 0)
    
    # Get today's spend for media buyers
    if is_media_buyer:
        # Filter spend by the exact advertiser/partner combinations in today's filtered data
        today_keys = perf_qs.filter(date=today).values_list('advertiser_id', 'partner_id').distinct()
        
        from django.db.models import Q
        today_spend_conditions = Q(date=today)
        if today_keys:
            partner_conditions = Q()
            for adv_id, part_id in today_keys:
                partner_conditions |= Q(advertiser_id=adv_id, partner_id=part_id)
            today_spend_conditions &= partner_conditions
            
            today_spend_qs = MediaBuyerDailySpend.objects.filter(today_spend_conditions)
            today_spend_agg = today_spend_qs.aggregate(total_spend=Sum("amount_spent"))
            today_spend = float(today_spend_agg["total_spend"] or 0)
        else:
            today_spend = 0
        today_profit = today_revenue - today_spend
    else:
        today_profit = today_revenue - today_payout
    
    # Get targets
    target_qs = DepartmentTarget.objects.filter(
        month=month_start
    )
    
    # Apply department scoping to targets (same as performance data)
    if company_user and company_user.department:
        dept = company_user.department
        dept_map = {
            "media_buying": "MB",
            "affiliate": "AFF",
            "influencer": "INF"
        }
        if dept in dept_map:
            target_qs = target_qs.filter(partner_type=dept_map[dept])
    
    if advertiser_ids:
        target_qs = target_qs.filter(advertiser_id__in=advertiser_ids)
    if partner_type:
        target_qs = target_qs.filter(partner_type=partner_type)
    
    # For non-admin users, prioritize individual targets
    # Admin/OpsManager: 
    #   - When filtering by specific advertiser/partner, show all targets (department + individual)
    #   - Otherwise show department-level targets only
    monthly_orders_target = None
    monthly_revenue_target = None
    monthly_profit_target = None
    monthly_spend_target = None
    
    if company_user and company_user.role:
        role = company_user.role.name
        if role not in {"Admin", "OpsManager"}:
            # Sum ALL targets assigned to this user (individual targets) + department-level targets
            from django.db.models import Q
            user_targets = target_qs.filter(
                Q(assigned_to=company_user) | Q(assigned_to__isnull=True)
            )
            
            if user_targets.exists():
                target_agg = user_targets.aggregate(
                    total_orders=Sum('orders_target'),
                    total_revenue=Sum('revenue_target'),
                    total_profit=Sum('profit_target'),
                    total_spend=Sum('spend_target')
                )
                if target_agg['total_orders'] is not None:
                    monthly_orders_target = int(target_agg['total_orders'])
                if target_agg['total_revenue'] is not None:
                    monthly_revenue_target = float(target_agg['total_revenue'])
                if target_agg['total_profit'] is not None:
                    monthly_profit_target = float(target_agg['total_profit'])
                if target_agg['total_spend'] is not None:
                    monthly_spend_target = float(target_agg['total_spend'])
        else:
            # Admin/OpsManager
            # Sum ALL targets (both department-level and individual) for accurate totals
            targets = target_qs.all()
            
            # Sum up all matching targets
            if targets.exists():
                target_agg = targets.aggregate(
                    total_orders=Sum('orders_target'),
                    total_revenue=Sum('revenue_target'),
                    total_profit=Sum('profit_target'),
                    total_spend=Sum('spend_target')
                )
                # Only set if aggregation returned non-null values
                if target_agg['total_orders'] is not None:
                    monthly_orders_target = int(target_agg['total_orders'])
                if target_agg['total_revenue'] is not None:
                    monthly_revenue_target = float(target_agg['total_revenue'])
                if target_agg['total_profit'] is not None:
                    monthly_profit_target = float(target_agg['total_profit'])
                if target_agg['total_spend'] is not None:
                    monthly_spend_target = float(target_agg['total_spend'])
    else:
        # No user/role: aggregate all department-level targets
        dept_targets = target_qs.filter(assigned_to__isnull=True)
        if dept_targets.exists():
            target_agg = dept_targets.aggregate(
                total_orders=Sum('orders_target'),
                total_revenue=Sum('revenue_target'),
                total_profit=Sum('profit_target'),
                total_spend=Sum('spend_target')
            )
            if target_agg['total_orders'] is not None:
                monthly_orders_target = int(target_agg['total_orders'])
            if target_agg['total_revenue'] is not None:
                monthly_revenue_target = float(target_agg['total_revenue'])
            if target_agg['total_profit'] is not None:
                monthly_profit_target = float(target_agg['total_profit'])
            if target_agg['total_spend'] is not None:
                monthly_spend_target = float(target_agg['total_spend'])
    
    # Calculate percentages - return None if no target
    if monthly_orders_target is not None:
        mtd_orders_pct = (mtd_orders / monthly_orders_target * 100) if monthly_orders_target > 0 else 0
    else:
        mtd_orders_pct = None
    
    if monthly_revenue_target is not None:
        mtd_revenue_pct = (mtd_revenue / monthly_revenue_target * 100) if monthly_revenue_target > 0 else 0
    else:
        mtd_revenue_pct = None
    
    if monthly_profit_target is not None:
        mtd_profit_pct = (mtd_profit / monthly_profit_target * 100) if monthly_profit_target > 0 else 0
    else:
        mtd_profit_pct = None
    
    if monthly_spend_target is not None:
        mtd_spend_pct = (mtd_spend / monthly_spend_target * 100) if monthly_spend_target > 0 else 0
    else:
        mtd_spend_pct = None
    
    # Calculate run rate (projected month-end)
    if days_elapsed > 0:
        daily_avg_orders = mtd_orders / days_elapsed
        daily_avg_revenue = mtd_revenue / days_elapsed
        daily_avg_profit = mtd_profit / days_elapsed
        daily_avg_spend = mtd_spend / days_elapsed
        
        projected_orders = int(daily_avg_orders * days_in_month)
        projected_revenue = daily_avg_revenue * days_in_month
        projected_profit = daily_avg_profit * days_in_month
        projected_spend = daily_avg_spend * days_in_month
    else:
        daily_avg_orders = 0
        daily_avg_revenue = 0
        daily_avg_profit = 0
        daily_avg_spend = 0
        projected_orders = 0
        projected_revenue = 0
        projected_profit = 0
        projected_spend = 0
    
    run_rate_orders_pct = (projected_orders / monthly_orders_target * 100) if (monthly_orders_target and monthly_orders_target > 0) else None
    run_rate_revenue_pct = (projected_revenue / monthly_revenue_target * 100) if (monthly_revenue_target and monthly_revenue_target > 0) else None
    run_rate_profit_pct = (projected_profit / monthly_profit_target * 100) if (monthly_profit_target and monthly_profit_target > 0) else None
    
    # Calculate pacing
    expected_progress_pct = (days_elapsed / days_in_month * 100) if days_in_month > 0 else 0
    orders_pacing = (mtd_orders_pct - expected_progress_pct) if mtd_orders_pct is not None else None
    revenue_pacing = (mtd_revenue_pct - expected_progress_pct) if mtd_revenue_pct is not None else None
    profit_pacing = (mtd_profit_pct - expected_progress_pct) if mtd_profit_pct is not None else None
    
    # Determine pacing status
    def get_pacing_status(pacing_value):
        if pacing_value is None:
            return "On Track"
        if pacing_value >= 5:
            return "Ahead"
        elif pacing_value <= -5:
            return "Behind"
        else:
            return "On Track"
    
    pacing_status = get_pacing_status(revenue_pacing)
    
    # Calculate required daily performance
    if days_remaining > 0:
        required_daily_orders = (monthly_orders_target - mtd_orders) / days_remaining if monthly_orders_target is not None else 0
        required_daily_revenue = (monthly_revenue_target - mtd_revenue) / days_remaining if monthly_revenue_target is not None else 0
        required_daily_profit = (monthly_profit_target - mtd_profit) / days_remaining if monthly_profit_target is not None else 0
    else:
        required_daily_orders = (monthly_orders_target / days_in_month) if (monthly_orders_target and days_in_month > 0) else 0
        required_daily_revenue = (monthly_revenue_target / days_in_month) if (monthly_revenue_target and days_in_month > 0) else 0
        required_daily_profit = (monthly_profit_target / days_in_month) if (monthly_profit_target and days_in_month > 0) else 0
    
    # Daily achievement
    daily_orders_achievement = (today_orders / required_daily_orders * 100) if required_daily_orders > 0 else 0
    daily_revenue_achievement = (today_revenue / required_daily_revenue * 100) if required_daily_revenue > 0 else 0
    
    # ROI/ROAS (MB only)
    roas = (mtd_revenue / mtd_spend) if mtd_spend > 0 else 0
    roi_pct = ((mtd_revenue - mtd_spend) / mtd_spend * 100) if mtd_spend > 0 else 0
    cpa = (mtd_spend / mtd_orders) if mtd_orders > 0 else 0
    
    # Efficiency metrics
    avg_order_value = (mtd_revenue / mtd_orders) if mtd_orders > 0 else 0
    profit_margin_pct = (mtd_profit / mtd_revenue * 100) if mtd_revenue > 0 else 0
    
    # Build response data
    response_data = {
        "month": str(month_start),
        "days_in_month": days_in_month,
        "days_elapsed": days_elapsed,
        "days_remaining": days_remaining,
        
        "mtd": {
            "orders": int(mtd_orders),
            "revenue": round(mtd_revenue, 2),
            "profit": round(mtd_profit, 2),
            "payout": round(mtd_payout, 2),
            "spend": round(mtd_spend, 2)
        },
        
        "targets": {
            "orders": monthly_orders_target,
            "revenue": round(monthly_revenue_target, 2) if monthly_revenue_target is not None else None,
            "profit": round(monthly_profit_target, 2) if monthly_profit_target is not None else None,
            "spend": round(monthly_spend_target, 2) if monthly_spend_target is not None else None
        },
        
        "achievement_pct": {
            "orders": round(mtd_orders_pct, 2) if mtd_orders_pct is not None else None,
            "revenue": round(mtd_revenue_pct, 2) if mtd_revenue_pct is not None else None,
            "profit": round(mtd_profit_pct, 2) if mtd_profit_pct is not None else None,
            "spend": round(mtd_spend_pct, 2) if mtd_spend_pct is not None else None
        },
        
        "run_rate": {
            "projected_orders": projected_orders,
            "projected_revenue": round(projected_revenue, 2),
            "projected_profit": round(projected_profit, 2),
            "projected_spend": round(projected_spend, 2),
            "orders_pct": round(run_rate_orders_pct, 2) if run_rate_orders_pct is not None else None,
            "revenue_pct": round(run_rate_revenue_pct, 2) if run_rate_revenue_pct is not None else None,
            "profit_pct": round(run_rate_profit_pct, 2) if run_rate_profit_pct is not None else None
        },
        
        "pacing": {
            "expected_progress_pct": round(expected_progress_pct, 2),
            "orders_pacing": round(orders_pacing, 2) if orders_pacing is not None else None,
            "revenue_pacing": round(revenue_pacing, 2) if revenue_pacing is not None else None,
            "profit_pacing": round(profit_pacing, 2) if profit_pacing is not None else None,
            "status": pacing_status
        },
        
        "daily": {
            "today_orders": int(today_orders),
            "today_revenue": round(today_revenue, 2),
            "today_profit": round(today_profit, 2),
            "required_daily_orders": round(required_daily_orders, 2),
            "required_daily_revenue": round(required_daily_revenue, 2),
            "required_daily_profit": round(required_daily_profit, 2),
            "orders_achievement_pct": round(daily_orders_achievement, 2),
            "revenue_achievement_pct": round(daily_revenue_achievement, 2)
        },
        
        "roi": {
            "roas": round(roas, 2),
            "roi_pct": round(roi_pct, 2),
            "cpa": round(cpa, 2)
        },
        
        "efficiency": {
            "avg_order_value": round(avg_order_value, 2),
            "profit_margin_pct": round(profit_margin_pct, 2)
        }
    }
    
    # Add department breakdown for admin/ops manager (only when not filtering by department)
    if company_user and company_user.role and company_user.role.name in {"Admin", "OpsManager"} and not partner_type:
        try:
            print(f"🔍 Calling get_department_breakdown with advertiser_ids={advertiser_ids}, partner_ids={partner_ids}")
            response_data["department_breakdown"] = get_department_breakdown(month_start, month_end, advertiser_ids, partner_ids)
        except Exception as e:
            print(f"❌ Error getting department breakdown: {e}")
            import traceback
            traceback.print_exc()
            response_data["department_breakdown"] = None
    else:
        response_data["department_breakdown"] = None
    
    # Check if user is AFF/INF TeamMember - provide simplified analytics
    is_department_restricted = False
    if company_user and company_user.role and company_user.role.name == "TeamMember":
        dept = company_user.department
        print(f"🔍 TeamMember department check: dept={dept}, role={company_user.role.name}")
        if dept in ["affiliate", "influencer"]:
            is_department_restricted = True
            print(f"✅ is_department_restricted set to True for {dept}")
            
            # For AFF/INF, calculate simplified analytics focused on their earnings
            # Get previous month's data for comparison
            prev_month_start = (month_start - timedelta(days=1)).replace(day=1)
            prev_days_in_month = monthrange(prev_month_start.year, prev_month_start.month)[1]
            prev_month_end = prev_month_start.replace(day=prev_days_in_month)
            
            prev_perf_qs = CampaignPerformance.objects.filter(
                date__gte=prev_month_start,
                date__lte=prev_month_end
            )
            
            # Apply same filters as current month
            if advertiser_ids:
                prev_perf_qs = prev_perf_qs.filter(advertiser_id__in=advertiser_ids)
            if partner_ids:
                prev_perf_qs = prev_perf_qs.filter(partner_id__in=partner_ids)
            if partner_type:
                prev_perf_qs = prev_perf_qs.filter(partner__partner_type=partner_type)
            
            # Apply same access control
            if company_user:
                assignments = AccountAssignment.objects.filter(company_user=company_user).prefetch_related("advertisers", "partners")
                adv_ids = set()
                part_ids = set()
                
                for a in assignments:
                    adv_ids.update(a.advertisers.values_list("id", flat=True))
                    part_ids.update(a.partners.values_list("id", flat=True))
                
                if adv_ids or part_ids:
                    if adv_ids:
                        prev_perf_qs = prev_perf_qs.filter(advertiser_id__in=list(adv_ids))
                    if part_ids:
                        prev_perf_qs = prev_perf_qs.filter(partner_id__in=list(part_ids))
                else:
                    prev_perf_qs = prev_perf_qs.none()
            
            prev_agg = prev_perf_qs.aggregate(
                total_orders=Sum("total_orders"),
                total_revenue=Sum("total_revenue"),
                total_payout=Sum("total_payout")
            )
            
            prev_orders = prev_agg["total_orders"] or 0
            prev_revenue = float(prev_agg["total_revenue"] or 0)
            prev_payout = float(prev_agg["total_payout"] or 0)
            
            # Calculate growth vs last month
            payout_growth_pct = ((mtd_payout - prev_payout) / prev_payout * 100) if prev_payout > 0 else 0
            orders_growth_pct = ((mtd_orders - prev_orders) / prev_orders * 100) if prev_orders > 0 else 0
            revenue_growth_pct = ((mtd_revenue - prev_revenue) / prev_revenue * 100) if prev_revenue > 0 else 0
            
            # Calculate average commission per order
            avg_commission = (mtd_payout / mtd_orders) if mtd_orders > 0 else 0
            prev_avg_commission = (prev_payout / prev_orders) if prev_orders > 0 else 0
            commission_growth_pct = ((avg_commission - prev_avg_commission) / prev_avg_commission * 100) if prev_avg_commission > 0 else 0
            
            # Get yesterday's performance
            yesterday = today - timedelta(days=1)
            yesterday_agg = perf_qs.filter(date=yesterday).aggregate(
                total_orders=Sum("total_orders"),
                total_revenue=Sum("total_revenue"),
                total_payout=Sum("total_payout")
            )
            
            yesterday_orders = yesterday_agg["total_orders"] or 0
            yesterday_revenue = float(yesterday_agg["total_revenue"] or 0)
            yesterday_payout = float(yesterday_agg["total_payout"] or 0)
            
            # Calculate run rate projection for end of month
            if days_elapsed > 0:
                daily_avg_payout = mtd_payout / days_elapsed
                daily_avg_orders = mtd_orders / days_elapsed
                projected_payout = daily_avg_payout * days_in_month
                projected_orders_runrate = int(daily_avg_orders * days_in_month)
                print(f"📊 Run Rate Calc: mtd_payout={mtd_payout}, mtd_orders={mtd_orders}, days_elapsed={days_elapsed}, days_in_month={days_in_month}")
                print(f"📊 Daily avg: payout=${daily_avg_payout:.2f}, orders={daily_avg_orders:.2f}")
                print(f"📊 Projected: payout=${projected_payout:.2f}, orders={projected_orders_runrate}")
            else:
                projected_payout = 0
                projected_orders_runrate = 0
            
            # Determine if on track vs last month
            vs_last_month_pct = ((projected_payout - prev_payout) / prev_payout * 100) if prev_payout > 0 else 0
            
            if vs_last_month_pct >= 10:
                run_rate_status = "Ahead"
            elif vs_last_month_pct <= -10:
                run_rate_status = "Behind"
            else:
                run_rate_status = "On Track"
            
            # Build simplified analytics structure
            print(f"📊 Simplified Analytics: mtd_sales=${mtd_sales:.2f}, mtd_revenue=${mtd_revenue:.2f}, mtd_payout=${mtd_payout:.2f}, mtd_orders={mtd_orders}")
            response_data["simplified_analytics"] = {
                "is_department_restricted": True,
                "earnings": {
                    "total_payout": round(mtd_payout, 2),
                    "total_orders": int(mtd_orders),
                    "avg_commission": round(avg_commission, 2),
                    "sales_volume": round(mtd_sales, 2)
                },
                "yesterday": {
                    "payout": round(yesterday_payout, 2),
                    "orders": int(yesterday_orders),
                    "sales": round(yesterday_revenue, 2)
                },
                "growth": {
                    "payout_vs_last_month_pct": round(payout_growth_pct, 2),
                    "orders_vs_last_month_pct": round(orders_growth_pct, 2),
                    "commission_vs_last_month_pct": round(commission_growth_pct, 2)
                },
                "run_rate": {
                    "projected_payout": round(projected_payout, 2),
                    "projected_orders": projected_orders_runrate,
                    "vs_last_month_pct": round(vs_last_month_pct, 2),
                    "status": run_rate_status,
                    "last_month_payout": round(prev_payout, 2)
                }
            }
    
    response_data["is_department_restricted"] = is_department_restricted
    
    return Response(response_data)


# =============================================================
# PIPELINE MANAGEMENT ENDPOINTS
# =============================================================

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def trigger_pipeline_upload(request):
    """
    Trigger pipeline execution after CSV upload to S3
    
    Expected POST data:
    {
        "pipeline": "nn" or "styli",
        "start_date": "2025-11-01",
        "end_date": "2025-11-23"
    }
    
    Workflow:
    1. Validates file exists in S3
    2. Starts pipeline execution in background thread
    3. Returns success immediately
    """
    import logging
    import threading
    from django.core.management import call_command
    from django.conf import settings
    from api.services.s3_service import s3_service
    from datetime import datetime
    
    logger = logging.getLogger('django')
    
    # Check permission (only Admin and OpsManager can trigger)
    try:
        company_user = CompanyUser.objects.get(user=request.user)
        if company_user.role.name not in {"Admin", "OpsManager"}:
            logger.warning(f"Insufficient permissions for user {request.user}")
            return Response(
                {"status": "error", "message": "Insufficient permissions"},
                status=403
            )
    except CompanyUser.DoesNotExist:
        logger.error(f"CompanyUser not found for user {request.user}")
        return Response(
            {"status": "error", "message": "User not found"},
            status=403
        )
    
    pipeline_arg = request.data.get("pipeline", "").lower()
    start_date = request.data.get("start_date")
    end_date = request.data.get("end_date")
    
    logger.info(f"Pipeline trigger request: pipeline={pipeline_arg}, start={start_date}, end={end_date}")
    
    # Map short names to full pipeline keys
    pipeline_map = {
        "nn": "noon_namshi",
        "styli": "styli",
        "drn": "drnutrition",
        "spr": "springrose"
    }
    
    if pipeline_arg not in pipeline_map:
        return Response(
            {"status": "error", "message": "Invalid pipeline. Must be 'nn', 'styli', 'drn', or 'spr'"},
            status=400
        )
    
    pipeline = pipeline_map[pipeline_arg]  # Get full pipeline name
    
    if not start_date or not end_date:
        return Response(
            {"status": "error", "message": "start_date and end_date are required"},
            status=400
        )
    
    # Validate date format
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return Response(
            {"status": "error", "message": "Dates must be in YYYY-MM-DD format"},
            status=400
        )
    
    try:
        # Only check S3 for S3-based pipelines (NN, Styli)
        s3_based_pipelines = ["noon_namshi", "styli"]
        
        if pipeline in s3_based_pipelines:
            s3_key = settings.S3_PIPELINE_FILES.get(pipeline)
            if not s3_key:
                logger.error(f"S3_PIPELINE_FILES not configured for pipeline: {pipeline}")
                return Response(
                    {"status": "error", "message": f"Pipeline {pipeline} not configured"},
                    status=500
                )
            
            logger.info(f"Checking S3 file: {s3_key}")
            
            # Check if file was uploaded to S3
            if not s3_service.file_exists(s3_key):
                logger.error(f"CSV file not found in S3: {s3_key}")
                return Response(
                    {"status": "error", "message": f"CSV file not found in S3: {s3_key}"},
                    status=400
                )
            
            logger.info(f"✓ File exists in S3: {s3_key}")
        
        logger.info(f"🚀 TRIGGERING {pipeline.upper()} PIPELINE IN BACKGROUND | Date Range: {start_date} → {end_date}")
        
        # Define function to run pipeline in background
        def run_pipeline_bg():
            try:
                logger.info(f"[BACKGROUND] Starting {pipeline.upper()} pipeline execution")
                if pipeline == "noon_namshi":
                    logger.info(f"[BACKGROUND] Calling: run_nn --start {start_date} --end {end_date}")
                    call_command('run_nn', start=start_date, end=end_date, verbosity=2)
                elif pipeline == "styli":
                    logger.info(f"[BACKGROUND] Calling: run_styli --start {start_date} --end {end_date}")
                    call_command('run_styli', start=start_date, end=end_date, verbosity=2)
                elif pipeline == "drnutrition":
                    logger.info(f"[BACKGROUND] Calling: run_drn --start {start_date} --end {end_date}")
                    call_command('run_drn', start=start_date, end=end_date, verbosity=2)
                elif pipeline == "springrose":
                    logger.info(f"[BACKGROUND] Calling: run_spr --start {start_date} --end {end_date}")
                    call_command('run_spr', start=start_date, end=end_date, verbosity=2)
                logger.info(f"[BACKGROUND] ✅ PIPELINE {pipeline.upper()} COMPLETED SUCCESSFULLY")
            except Exception as e:
                logger.error(f"[BACKGROUND] ❌ PIPELINE ERROR: {str(e)}", exc_info=True)
        
        # Start pipeline in background thread
        thread = threading.Thread(target=run_pipeline_bg, daemon=True)
        thread.start()
        
        # Return immediately
        return Response({
            "status": "queued",
            "message": f"{pipeline.upper()} pipeline queued for execution",
            "pipeline": pipeline,
            "date_range": {"start": start_date, "end": end_date}
        })
        
    except Exception as e:
        logger.error(f"❌ REQUEST VALIDATION ERROR: {str(e)}", exc_info=True)
        
        return Response(
            {"status": "error", "message": f"Request validation failed: {str(e)}"},
            status=500
        )