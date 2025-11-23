
from rest_framework.response import Response # type: ignore

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

from datetime import datetime, date
from calendar import monthrange


# Pagination class for performance table
class PerformanceTablePagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 100






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
    print("🌐 SENDING ROLE IN CONTEXT:", role)
    base = {
        "username": user.username,
        "role": role,
    }

    # CEO or OpsManager → see all
    if role in ["Admin", "OpsManager"]:
        base["can_see_all"] = True
        return Response(base)

    # Account Manager / Department Head
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
    # Department scoping
    # -------------------------------
    if company_user and company_user.department:
        dept = company_user.department
        if dept == "media_buying":
            qs = qs.filter(partner__partner_type="MB")
        elif dept == "affiliate":
            qs = qs.filter(partner__partner_type="AFF")
        elif dept == "influencer":
            qs = qs.filter(partner__partner_type="INF")

    # -------------------------------
    # Filters (NOW INCLUDES COUPON) - SUPPORT MULTIPLE VALUES
    # -------------------------------
    advertiser_ids = request.GET.getlist("advertiser_id")
    partner_ids = request.GET.getlist("partner_id")
    coupon_codes = request.GET.getlist("coupon_code")
    partner_type = request.GET.get("partner_type")
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")

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
        full_access_roles = {"Admin", "OpsManager"}

        if role not in full_access_roles:
            assignments = AccountAssignment.objects.filter(company_user=company_user).prefetch_related("advertisers")
            advertiser_ids = set()
            partner_ids = set()

            for a in assignments:
                advertiser_ids.update(a.advertisers.values_list("id", flat=True))
                partner_ids.update(a.partners.values_list("id", flat=True))

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

    # Department scope: MB, AFF, INF
    if company_user and company_user.department:
        dept = company_user.department
        if dept == "media_buying":
            qs = qs.filter(partner__partner_type="MB")
        elif dept == "affiliate":
            qs = qs.filter(partner__partner_type="AFF")
        elif dept == "influencer":
            qs = qs.filter(partner__partner_type="INF")

    # Optional filters - SUPPORT MULTIPLE VALUES
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")
    coupon_codes = request.GET.getlist("coupon_code")
    partner_ids = request.GET.getlist("partner_id")
    advertiser_ids = request.GET.getlist("advertiser_id")

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
        full_access_roles = {"Admin", "OpsManager"}

        if role not in full_access_roles:
            assignments = AccountAssignment.objects.filter(
                company_user=company_user
            ).prefetch_related("advertisers", "partners")

            advertiser_ids = set()
            partner_ids = set()

            for a in assignments:
                advertiser_ids.update(a.advertisers.values_list("id", flat=True))
                partner_ids.update(a.partners.values_list("id", flat=True))

            if advertiser_ids:
                qs = qs.filter(advertiser_id__in=list(advertiser_ids))
            if partner_ids:
                qs = qs.filter(partner_id__in=list(partner_ids))

    # Detect if user is full access (Admin / OpsManager)
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
    # Department scoping
    # -------------------------------
    if company_user and company_user.department:
        dept = company_user.department
        if dept == "media_buying":
            qs = qs.filter(partner__partner_type="MB")
        elif dept == "affiliate":
            qs = qs.filter(partner__partner_type="AFF")
        elif dept == "influencer":
            qs = qs.filter(partner__partner_type="INF")

    # -------------------------------
    # Filters
    # -------------------------------
# -------------------------------
# Filters - SUPPORT MULTIPLE VALUES
# -------------------------------
    advertiser_ids = request.GET.getlist("advertiser_id")
    partner_ids = request.GET.getlist("partner_id")
    coupon_codes = request.GET.getlist("coupon_code")
    partner_type = request.GET.get("partner_type")
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")

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
                # Get spend grouped by date/advertiser/partner/coupon (sum across platforms)
                spends = MediaBuyerDailySpend.objects.filter(spend_conditions).select_related('coupon')
                
                for s in spends:
                    coupon_code = s.coupon.code if s.coupon else None
                    key = (s.date, s.advertiser_id, s.partner_id, coupon_code)
                    mb_spend_lookup[key] = mb_spend_lookup.get(key, 0) + float(s.amount_spent or 0)
            
            # Calculate total revenue per (date, advertiser, partner, coupon) for MB records
            for r in mb_records:
                key = (r["date"], r["advertiser_id"], r["partner_id"], r["coupon_code"])
                revenue = float(r["total_revenue"] or 0)
                mb_revenue_totals[key] = mb_revenue_totals.get(key, 0) + revenue

        result = []
        for r in data:
            revenue = float(r["total_revenue"] or 0)
            original_payout = float(r["total_payout"] or 0)
            
            # For MB partners, payout = MB spend (cost) matched by coupon
            # For AFF/INF partners, payout = their actual payout
            if r["partner_type_value"] == "MB":
                # Match spend by (date, advertiser, partner, coupon)
                key = (r["date"], r["advertiser_id"], r["partner_id"], r["coupon_code"])
                total_spend = mb_spend_lookup.get(key, 0)
                total_revenue_for_key = mb_revenue_totals.get(key, 1)  # Avoid division by zero
                
                # Allocate spend proportionally based on this row's revenue
                # (in case there are multiple orders with same coupon on same day)
                if total_revenue_for_key > 0:
                    payout = total_spend * (revenue / total_revenue_for_key)
                else:
                    payout = 0
            else:
                payout = original_payout
            
            # Now profit = revenue - payout works for all types
            profit = revenue - payout
            
            result.append({
                "date": r["date"],
                "advertiser_id": r["advertiser_id"],
                "partner_id": r["partner_id"],
                "campaign": r["campaign"],
                "coupon": r["coupon_code"],
                "partner": r["partner_name"],
                "orders": int(r["total_orders"] or 0),
                "sales": float(r["total_sales"] or 0),
                "revenue": revenue,
                "payout": payout,
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
    ).values(
        "date",
        "advertiser_id",
        "partner_id",
        "campaign",
        "coupon_code",
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
            spend_qs = MediaBuyerDailySpend.objects.filter(spend_conditions).select_related('coupon')
            # Build lookup dict: (date, advertiser_id, partner_id, coupon_code) -> total spend
            for spend in spend_qs:
                coupon_code = spend.coupon.code if spend.coupon else None
                key = (str(spend.date), spend.advertiser_id, spend.partner_id, coupon_code)
                spend_dict[key] = spend_dict.get(key, 0) + float(spend.amount_spent or 0)
        
        # Calculate total revenue per day/advertiser/partner/coupon for proportional distribution
        for r in data:
            key = (str(r["date"]), r["advertiser_id"], r["partner_id"], r["coupon_code"])
            revenue = float(r["total_revenue"] or 0)
            if key not in daily_revenue_dict:
                daily_revenue_dict[key] = 0
            daily_revenue_dict[key] += revenue

    result = []
    for r in data:
        company_revenue = float(r["total_revenue"] or 0)
        partner_payout = float(r["total_payout"] or 0)
        
        # For media buyers, show company revenue and their spend matched by coupon
        if is_media_buyer:
            key = (str(r["date"]), r["advertiser_id"], r["partner_id"], r["coupon_code"])
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
                "orders": int(r["total_orders"] or 0),
                "sales": float(r["total_sales"] or 0),
                "revenue": company_revenue,
                "payout": allocated_spend,
                "spend": allocated_spend,  # Add spend field explicitly for MB
                "profit": company_revenue - allocated_spend,
            }
        else:
            # For affiliates/influencers, show their payout (what they earn)
            row = {
                "date": r["date"],
                "advertiser_id": r["advertiser_id"],
                "campaign": r["campaign"],
                "coupon": r["coupon_code"],
                "orders": int(r["total_orders"] or 0),
                "sales": float(r["total_sales"] or 0),
                "payout": partner_payout,  # This is what they earn
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

    # Get base queryset with same logic as performance_table_view
    qs = CampaignPerformance.objects.all()

    # Department scoping
    if company_user and company_user.department:
        dept = company_user.department
        if dept == "affiliate":
            qs = qs.filter(partner__partner_type="AFF")
        elif dept == "influencer":
            qs = qs.filter(partner__partner_type="INF")
        elif dept == "media_buying":
            qs = qs.filter(partner__partner_type="MB")

    # Role-based filtering
    full_access_roles = {"Admin", "OpsManager"}
    if role not in full_access_roles:
        assignments = AccountAssignment.objects.filter(
            company_user=company_user
        ).prefetch_related("advertisers", "partners")

        advertiser_ids = set()
        partner_ids = set()
        for a in assignments:
            advertiser_ids.update(a.advertisers.values_list("id", flat=True))
            partner_ids.update(a.partners.values_list("id", flat=True))

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
                "campaign": cp.advertiser.name
            }
        
        # Partners
        if cp.partner_id and cp.partner_id not in partners_map:
            partners_map[cp.partner_id] = {
                "partner_id": cp.partner_id,
                "partner": cp.partner.name
            }
        
        # Coupons
        if cp.coupon and cp.coupon.code not in coupons_map:
            coupons_map[cp.coupon.code] = {
                "coupon": cp.coupon.code,
                "advertiser_id": cp.advertiser_id,
                "partner_id": cp.partner_id
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

    # Get filters from request
    start_date_str = request.GET.get("start_date")
    end_date_str = request.GET.get("end_date")
    advertiser_id = request.GET.get("advertiser")
    partner_id = request.GET.get("partner")
    coupon = request.GET.get("coupon")

    qs = CampaignPerformance.objects.all()

    # Department scoping
    if company_user and company_user.department:
        dept = company_user.department
        if dept == "affiliate":
            qs = qs.filter(partner__partner_type="AFF")
        elif dept == "influencer":
            qs = qs.filter(partner__partner_type="INF")
        elif dept == "media_buying":
            qs = qs.filter(partner__partner_type="MB")

    # Role-based filtering
    full_access_roles = {"Admin", "OpsManager"}
    if role not in full_access_roles:
        assignments = AccountAssignment.objects.filter(
            company_user=company_user
        ).prefetch_related("advertisers", "partners")

        advertiser_ids = set()
        partner_ids = set()
        for a in assignments:
            advertiser_ids.update(a.advertisers.values_list("id", flat=True))
            partner_ids.update(a.partners.values_list("id", flat=True))

        if advertiser_ids:
            qs = qs.filter(advertiser__id__in=advertiser_ids)
        if partner_ids:
            qs = qs.filter(partner__id__in=partner_ids)

    # Apply user filters
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

    if advertiser_id:
        qs = qs.filter(advertiser_id=advertiser_id)

    if partner_id:
        qs = qs.filter(partner_id=partner_id)

    if coupon:
        qs = qs.filter(coupon=coupon)

    # Aggregate by campaign
    campaign_totals = {}
    for cp in qs.select_related('advertiser'):
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
            full_access_roles = {"Admin", "OpsManager"}
            
            if role not in full_access_roles:
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

        # Check if the coupon already exists
        if Coupon.objects.filter(code=code).exists():
            return Response({"error": f"Coupon {code} already exists. Use PATCH to update."}, status=400)

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
@api_view(["PATCH"])
@permission_classes([IsAuthenticated])
def coupon_detail_view(request, code):
    """
    Update an existing coupon's partner, geo, or discount.
    """
    try:
        coupon = Coupon.objects.get(code=code)
    except Coupon.DoesNotExist:
        return Response({"error": f"Coupon {code} not found."}, status=404)

    data = request.data
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
    partners = Partner.objects.all()
    results = [
        {
            "id": partner.id,# type: ignore
            "name": partner.name,
            "type": partner.get_partner_type_display(),  # For "Affiliate", "Influencer", etc.# type: ignore
        }
        for partner in partners
    ]
    return Response(results)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def advertiser_list_view(request):
    advertisers = Advertiser.objects.all()
    results = [
        {
            "id": adv.id,# type: ignore
            "name": adv.name,
            "attribution": adv.get_attribution_display(),  # Returns "Coupon" or "Link" # type: ignore
        }
        for adv in advertisers
    ]
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

@api_view(['GET', 'POST'])
@permission_classes([IsAuthenticated])
def targets_list(request):
    """
    GET: List all targets (with optional filtering)
    POST: Create new target
    """
    if request.method == 'GET':
        targets = DepartmentTarget.objects.all().select_related('advertiser').order_by('-month', 'advertiser__name', 'partner_type')
        
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
        serializer = DepartmentTargetSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
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
        target = DepartmentTarget.objects.get(pk=pk)
    except DepartmentTarget.DoesNotExist:
        return Response({"error": "Target not found"}, status=status.HTTP_404_NOT_FOUND)
    
    if request.method == 'GET':
        serializer = DepartmentTargetSerializer(target)
        return Response(serializer.data)
    
    elif request.method == 'PUT':
        serializer = DepartmentTargetSerializer(target, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    elif request.method == 'DELETE':
        target.delete()
        return Response({"success": "Target deleted"}, status=status.HTTP_204_NO_CONTENT)
    
    
    
    


# ============ PERFORMANCE ANALYTICS API ============

def get_department_breakdown(month_start, month_end, advertiser_id=None, partner_id=None):
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
        
        if advertiser_id:
            dept_qs = dept_qs.filter(advertiser_id=advertiser_id)
        if partner_id:
            dept_qs = dept_qs.filter(partner_id=partner_id)
        
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
        
        # Get targets for this department
        dept_target = DepartmentTarget.objects.filter(
            month=month_start,
            partner_type=dept_info["partner_type"]
        ).first()
        
        if dept_target:
            orders_target = dept_target.orders_target
            revenue_target = float(dept_target.revenue_target)
            profit_target = float(dept_target.profit_target)
            
            orders_pct = (orders / orders_target * 100) if orders_target > 0 else 0
            revenue_pct = (revenue / revenue_target * 100) if revenue_target > 0 else 0
            profit_pct = (profit / profit_target * 100) if profit_target > 0 else 0
        else:
            orders_target = 0
            revenue_target = 0
            profit_target = 0
            orders_pct = 0
            revenue_pct = 0
            profit_pct = 0
        
        breakdown.append({
            "code": dept_code,
            "name": dept_info["name"],
            "orders": int(orders),
            "revenue": round(revenue, 2),
            "profit": round(profit, 2),
            "payout": round(payout, 2),
            "targets": {
                "orders": orders_target,
                "revenue": round(revenue_target, 2),
                "profit": round(profit_target, 2)
            },
            "achievement": {
                "orders_pct": round(orders_pct, 2),
                "revenue_pct": round(revenue_pct, 2),
                "profit_pct": round(profit_pct, 2)
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
    
    # Get filter parameters
    advertiser_id = request.GET.get('advertiser_id')
    if advertiser_id:
        try:
            advertiser_id = int(advertiser_id)
        except:
            advertiser_id = None
    
    partner_id = request.GET.get('partner_id')
    if partner_id:
        try:
            partner_id = int(partner_id)
        except:
            partner_id = None
    
    partner_type = request.GET.get('partner_type')
    month_param = request.GET.get('month')
    
    # Determine target month (default to current month)
    if month_param:
        try:
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
    
    # Apply department scoping
    if company_user and company_user.department:
        dept = company_user.department
        if dept == "media_buying":
            perf_qs = perf_qs.filter(partner__partner_type="MB")
            if not partner_type:
                partner_type = "MB"
        elif dept == "affiliate":
            perf_qs = perf_qs.filter(partner__partner_type="AFF")
            if not partner_type:
                partner_type = "AFF"
        elif dept == "influencer":
            perf_qs = perf_qs.filter(partner__partner_type="INF")
            if not partner_type:
                partner_type = "INF"
    
    # Apply filters
    if advertiser_id:
        perf_qs = perf_qs.filter(advertiser_id=advertiser_id)
    if partner_id:
        perf_qs = perf_qs.filter(partner_id=partner_id)
    if partner_type:
        perf_qs = perf_qs.filter(partner__partner_type=partner_type)
    
    # Role-based access control
    if company_user and company_user.role:
        role = company_user.role.name
        full_access_roles = {"Admin", "OpsManager"}
        
        if role not in full_access_roles:
            assignments = AccountAssignment.objects.filter(company_user=company_user).prefetch_related("advertisers", "partners")
            adv_ids = set()
            part_ids = set()
            
            for a in assignments:
                adv_ids.update(a.advertisers.values_list("id", flat=True))
                part_ids.update(a.partners.values_list("id", flat=True))
            
            if adv_ids:
                perf_qs = perf_qs.filter(advertiser_id__in=list(adv_ids))
            if part_ids:
                perf_qs = perf_qs.filter(partner_id__in=list(part_ids))
    
    # Calculate MTD actuals
    mtd_agg = perf_qs.aggregate(
        total_orders=Sum("total_orders"),
        total_revenue=Sum("total_revenue"),
        total_payout=Sum("total_payout")
    )
    
    mtd_orders = mtd_agg["total_orders"] or 0
    mtd_revenue = float(mtd_agg["total_revenue"] or 0)
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
    
    if advertiser_id:
        target_qs = target_qs.filter(advertiser_id=advertiser_id)
    if partner_type:
        target_qs = target_qs.filter(partner_type=partner_type)
    
    target = target_qs.first()
    
    if target:
        monthly_orders_target = int(target.orders_target)
        monthly_revenue_target = float(target.revenue_target)
        monthly_profit_target = float(target.profit_target)
        monthly_spend_target = float(target.spend_target or 0)
    else:
        monthly_orders_target = 0
        monthly_revenue_target = 0
        monthly_profit_target = 0
        monthly_spend_target = 0
    
    # Calculate percentages
    mtd_orders_pct = (mtd_orders / monthly_orders_target * 100) if monthly_orders_target > 0 else 0
    mtd_revenue_pct = (mtd_revenue / monthly_revenue_target * 100) if monthly_revenue_target > 0 else 0
    mtd_profit_pct = (mtd_profit / monthly_profit_target * 100) if monthly_profit_target > 0 else 0
    mtd_spend_pct = (mtd_spend / monthly_spend_target * 100) if monthly_spend_target > 0 else 0
    
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
    
    run_rate_orders_pct = (projected_orders / monthly_orders_target * 100) if monthly_orders_target > 0 else 0
    run_rate_revenue_pct = (projected_revenue / monthly_revenue_target * 100) if monthly_revenue_target > 0 else 0
    run_rate_profit_pct = (projected_profit / monthly_profit_target * 100) if monthly_profit_target > 0 else 0
    
    # Calculate pacing
    expected_progress_pct = (days_elapsed / days_in_month * 100) if days_in_month > 0 else 0
    orders_pacing = mtd_orders_pct - expected_progress_pct
    revenue_pacing = mtd_revenue_pct - expected_progress_pct
    profit_pacing = mtd_profit_pct - expected_progress_pct
    
    # Determine pacing status
    def get_pacing_status(pacing_value):
        if pacing_value >= 5:
            return "Ahead"
        elif pacing_value <= -5:
            return "Behind"
        else:
            return "On Track"
    
    pacing_status = get_pacing_status(revenue_pacing)
    
    # Calculate required daily performance
    if days_remaining > 0:
        required_daily_orders = (monthly_orders_target - mtd_orders) / days_remaining
        required_daily_revenue = (monthly_revenue_target - mtd_revenue) / days_remaining
        required_daily_profit = (monthly_profit_target - mtd_profit) / days_remaining
    else:
        required_daily_orders = monthly_orders_target / days_in_month if days_in_month > 0 else 0
        required_daily_revenue = monthly_revenue_target / days_in_month if days_in_month > 0 else 0
        required_daily_profit = monthly_profit_target / days_in_month if days_in_month > 0 else 0
    
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
            "revenue": round(monthly_revenue_target, 2),
            "profit": round(monthly_profit_target, 2),
            "spend": round(monthly_spend_target, 2)
        },
        
        "achievement_pct": {
            "orders": round(mtd_orders_pct, 2),
            "revenue": round(mtd_revenue_pct, 2),
            "profit": round(mtd_profit_pct, 2),
            "spend": round(mtd_spend_pct, 2)
        },
        
        "run_rate": {
            "projected_orders": projected_orders,
            "projected_revenue": round(projected_revenue, 2),
            "projected_profit": round(projected_profit, 2),
            "projected_spend": round(projected_spend, 2),
            "orders_pct": round(run_rate_orders_pct, 2),
            "revenue_pct": round(run_rate_revenue_pct, 2),
            "profit_pct": round(run_rate_profit_pct, 2)
        },
        
        "pacing": {
            "expected_progress_pct": round(expected_progress_pct, 2),
            "orders_pacing": round(orders_pacing, 2),
            "revenue_pacing": round(revenue_pacing, 2),
            "profit_pacing": round(profit_pacing, 2),
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
            response_data["department_breakdown"] = get_department_breakdown(month_start, month_end, advertiser_id, partner_id)
        except Exception as e:
            print(f"Error getting department breakdown: {e}")
            import traceback
            traceback.print_exc()
            response_data["department_breakdown"] = None
    else:
        response_data["department_breakdown"] = None
    
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