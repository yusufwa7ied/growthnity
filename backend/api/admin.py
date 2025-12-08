from django.contrib import admin
from import_export.admin import ImportExportModelAdmin

from .models import (
    Advertiser, Partner, Coupon, CampaignPerformance,
    CompanyRole, CompanyUser, AccountAssignment,
    DrNutritionTransaction, PartnerizeConversion,
    AdvertiserRate, PartnerPayout,
    MediaBuyerDailySpend, DepartmentTarget,
    RawAdvertiserRecord,
    SpringRoseTransaction,
    NoonNamshiTransaction,
    NoonTransaction,
    PayoutRuleHistory,
    RevenueRuleHistory,
    AdvertiserCancellationRate,
)
from .models import StyliTransaction
from django.utils import timezone

@admin.register(Advertiser)
class AdvertiserAdmin(ImportExportModelAdmin):
    list_display = (
        "id", "name", "attribution", "rev_rate_type", 
        "rev_ftu_rate", "rev_rtu_rate", "currency", "exchange_rate",
        "default_payout_rate_type", "default_ftu_payout", "default_rtu_payout"
    )
    search_fields = ("name", "attribution")
    list_filter = ("attribution", "rev_rate_type", "currency", "default_payout_rate_type")
    list_per_page = 50
    ordering = ("name",)
    readonly_fields = ("id",)
    
    fieldsets = (
        ("Basic Information", {
            "fields": ("id", "name", "attribution")
        }),
        ("Revenue Configuration", {
            "fields": (
                "rev_rate_type", "rev_ftu_rate", "rev_rtu_rate",
                "rev_ftu_fixed_bonus", "rev_rtu_fixed_bonus"
            )
        }),
        ("Currency", {
            "fields": ("currency", "exchange_rate")
        }),
        ("Default Partner Payouts", {
            "fields": (
                "default_payout_rate_type", "default_ftu_payout", "default_rtu_payout",
                "default_ftu_fixed_bonus", "default_rtu_fixed_bonus"
            )
        }),
    )
    
    def save_model(self, request, obj, form, change):
        """Override save to create RevenueRuleHistory record when revenue fields change."""
        super().save_model(request, obj, form, change)
        
        # Create history record when advertiser revenue rules are created/updated
        RevenueRuleHistory.objects.create(
            advertiser=obj,
            effective_date=timezone.now(),
            rev_rate_type=obj.rev_rate_type,
            rev_ftu_rate=obj.rev_ftu_rate,
            rev_rtu_rate=obj.rev_rtu_rate,
            rev_ftu_fixed_bonus=obj.rev_ftu_fixed_bonus,
            rev_rtu_fixed_bonus=obj.rev_rtu_fixed_bonus,
            currency=obj.currency,
            exchange_rate=obj.exchange_rate,
            assigned_by=request.user,
            notes=f"Updated via admin by {request.user.username}"
        )

@admin.register(AdvertiserCancellationRate)
class AdvertiserCancellationRateAdmin(ImportExportModelAdmin):
    list_display = (
        "id", "advertiser", "start_date", "end_date", 
        "cancellation_rate", "created_by", "created_at"
    )
    search_fields = ("advertiser__name", "notes")
    list_filter = ("advertiser", "start_date", "created_at")
    list_per_page = 50
    ordering = ("-start_date",)
    readonly_fields = ("id", "created_at")
    
    fieldsets = (
        ("Rate Information", {
            "fields": ("id", "advertiser", "cancellation_rate")
        }),
        ("Date Range", {
            "fields": ("start_date", "end_date")
        }),
        ("Additional Info", {
            "fields": ("notes", "created_by", "created_at")
        }),
    )
    
    def save_model(self, request, obj, form, change):
        """Set created_by to current user if not set"""
        if not obj.created_by:
            obj.created_by = request.user
        super().save_model(request, obj, form, change)

@admin.register(CompanyRole)
class CompanyRoleAdmin(ImportExportModelAdmin):
    list_display = ("id", "name")
    search_fields = ("name",)
    list_per_page = 50
    ordering = ("name",)

@admin.register(CompanyUser)
class CompanyUserAdmin(ImportExportModelAdmin):
    list_display = ("get_username", "role", "phone")
    search_fields = ("user__username", "role__name")
    list_per_page = 50
    ordering = ("user__username",)

    def get_username(self, obj):
        return obj.user.username if obj.user else "(No user)"
    get_username.short_description = "User"

@admin.register(Partner)
class PartnerAdmin(ImportExportModelAdmin):
    list_display = ("id", "name", "partner_type")
    search_fields = ("name", "partner_type")
    list_per_page = 50
    ordering = ("name",)

@admin.register(MediaBuyerDailySpend)
class MediaBuyerDailySpendAdmin(ImportExportModelAdmin):
    list_display = ("date", "advertiser", "partner", "amount_spent", "currency")
    list_filter = ("advertiser", "partner", "date")
    search_fields = ("advertiser__name", "partner__name")
    list_per_page = 50
    ordering = ("-date",)
    date_hierarchy = "date"

@admin.register(DepartmentTarget)
class DepartmentTargetAdmin(ImportExportModelAdmin):
    list_display = ("month", "advertiser", "partner_type", "orders_target", "revenue_target", "profit_target", "spend_target")
    list_filter = ("partner_type", "advertiser")
    list_per_page = 50
    ordering = ("-month",)
    date_hierarchy = "month"

@admin.register(AccountAssignment)
class AccountAssignmentAdmin(ImportExportModelAdmin):
    list_display = ("company_user", "get_advertisers", "get_partners")
    list_filter = ("advertisers", "partners")
    search_fields = ("company_user__user__username",)
    list_per_page = 50
    ordering = ("company_user__user__username",)

    def get_advertisers(self, obj):
        return ", ".join(a.name for a in obj.advertisers.all())
    get_advertisers.short_description = "Advertisers"

    def get_partners(self, obj):
        return ", ".join(p.name for p in obj.partners.all())
    get_partners.short_description = "Partners"

@admin.register(DrNutritionTransaction)
class DrNutritionTransactionAdmin(ImportExportModelAdmin):
    list_display = (
        "order_id",
        "created_date",
        "delivery_status",
        "country",
        "coupon",
        "user_type",
        "partner_name",
        "partner_type",
        "advertiser_name",
        "currency",
        "rate_type",
        "ftu_orders",
        "rtu_orders",
        "orders",
        "sales",
        "commission",
        "our_rev",
        "payout",
        "profit",
        "payout_usd",
        "profit_usd",
    )
    search_fields = (
        "order_id",
        "coupon",
        "partner_name",
        "advertiser_name",
    )
    list_filter = (
        "delivery_status",
        "country",
        "partner_type",
        "advertiser_name",
        "currency",
        "rate_type",
        "user_type",
    )
    list_per_page = 50
    ordering = ("-created_date",)
    date_hierarchy = "created_date"
    readonly_fields = ("order_id", "created_date")

@admin.register(StyliTransaction)
class StyliTransactionAdmin(ImportExportModelAdmin):
    list_display = (
        "order_id",
        "created_date",
        "delivery_status",
        "country",
        "coupon",
        "user_type",
        "partner_name",
        "partner_type",
        "advertiser_name",
        "currency",
        "rate_type",
        "ftu_orders",
        "rtu_orders",
        "orders",
        "sales",
        "commission",
        "our_rev",
        "payout",
        "profit",
        "payout_usd",
        "profit_usd",
    )
    search_fields = ("order_id", "coupon", "partner_name", "advertiser_name")
    list_filter = ("delivery_status", "country", "partner_type", "advertiser_name", "currency", "rate_type", "user_type")
    list_per_page = 50
    ordering = ("-created_date",)
    date_hierarchy = "created_date"
    readonly_fields = ("order_id", "created_date")

@admin.register(SpringRoseTransaction)
class SpringRoseTransactionAdmin(ImportExportModelAdmin):
    list_display = (
        "order_id",
        "created_date",
        "delivery_status",
        "country",
        "coupon",
        "user_type",
        "partner_name",
        "partner_type",
        "advertiser_name",
        "currency",
        "rate_type",
        "ftu_orders",
        "rtu_orders",
        "orders",
        "sales",
        "commission",
        "our_rev",
        "payout",
        "profit",
        "payout_usd",
        "profit_usd",
    )
    search_fields = ("order_id", "coupon", "partner_name", "advertiser_name")
    list_filter = (
        "delivery_status",
        "country",
        "partner_type",
        "advertiser_name",
        "currency",
        "rate_type",
        "user_type",
    )
    list_per_page = 50
    ordering = ("-created_date",)
    date_hierarchy = "created_date"
    readonly_fields = ("order_id", "created_date")

@admin.register(PartnerizeConversion)
class PartnerizeConversionAdmin(ImportExportModelAdmin):
    list_display = ("conversion_id", "campaign_title", "conversion_time", "country", "total_order_value", "total_commission", "conversion_status", "first_time_user")
    search_fields = ("conversion_id", "campaign_title", "voucher")
    list_filter = ("country", "conversion_status", "first_time_user")
    list_per_page = 50
    ordering = ("-conversion_time",)
    date_hierarchy = "conversion_time"
    readonly_fields = ("conversion_id", "conversion_time")

# AdvertiserRate admin registration
@admin.register(AdvertiserRate)
class AdvertiserRateAdmin(ImportExportModelAdmin):
    list_display = ("advertiser", "geo", "user_type", "rate_type", "currency")
    list_filter = ("advertiser", "geo", "user_type", "rate_type", "currency")
    search_fields = ("advertiser__name", "geo")
    list_per_page = 50
    ordering = ("advertiser__name",)

# PartnerPayout admin registration
@admin.register(PartnerPayout)
class PartnerPayoutAdmin(ImportExportModelAdmin):
    list_display = (
        "advertiser",
        "partner",
        "ftu_payout",
        "rtu_payout",
        "ftu_fixed_bonus",
        "rtu_fixed_bonus",
        "exchange_rate",
        "currency",
        "rate_type",
        "start_date",
        "end_date",
        "condition",
    )
    list_filter = (
        "advertiser",
        "partner",
        "rate_type",
        "currency",
        "condition",
    )
    search_fields = (
        "advertiser__name",
        "partner__name",
        "condition",
    )
    list_per_page = 50
    ordering = ("advertiser__name", "partner__name")
    # Grouping fields in fieldsets for better admin usability
    fieldsets = (
        ("Partner & Advertiser Info", {
            "fields": ("advertiser", "partner", "condition")
        }),
        ("FTU/RTU Rates", {
            "fields": ("ftu_payout", "rtu_payout", "ftu_fixed_bonus", "rtu_fixed_bonus")
        }),
        ("Currency Info", {
            "fields": ("currency", "exchange_rate", "rate_type")
        }),
        ("Date Range (Optional - leave blank for permanent)", {
            "fields": ("start_date", "end_date"),
            "classes": ("collapse",)
        }),
    )
    
    def save_model(self, request, obj, form, change):
        """Override save to create PayoutRuleHistory record."""
        super().save_model(request, obj, form, change)
        
        # Create history record when payout is created/updated
        PayoutRuleHistory.objects.create(
            advertiser=obj.advertiser,
            partner=obj.partner,
            effective_date=timezone.now(),
            ftu_payout=obj.ftu_payout,
            rtu_payout=obj.rtu_payout,
            ftu_fixed_bonus=obj.ftu_fixed_bonus,
            rtu_fixed_bonus=obj.rtu_fixed_bonus,
            rate_type=obj.rate_type,
            assigned_by=request.user,
            notes=f"Updated via admin by {request.user.username}"
        )

@admin.register(Coupon)
class CouponAdmin(ImportExportModelAdmin):
    list_display = ("code", "advertiser", "partner", "geo", "discount_percent")
    search_fields = ("code", "advertiser__name", "partner__name")
    list_per_page = 50
    ordering = ("code",)

# CouponAssignmentHistory admin registration
from .models import CouponAssignmentHistory

@admin.register(CouponAssignmentHistory)
class CouponAssignmentHistoryAdmin(ImportExportModelAdmin):
    list_display = ("coupon", "partner", "assigned_date", "assigned_by", "discount_percent")
    list_filter = ("partner", "assigned_by", "assigned_date")
    search_fields = ("coupon__code", "partner__name")
    list_per_page = 50
    ordering = ("-assigned_date",)
    date_hierarchy = "assigned_date"
    readonly_fields = ("assigned_date",)

@admin.register(PayoutRuleHistory)
class PayoutRuleHistoryAdmin(ImportExportModelAdmin):
    list_display = ("advertiser", "partner", "effective_date", "ftu_payout", "rtu_payout", "rate_type", "assigned_by")
    list_filter = ("advertiser", "partner", "rate_type", "effective_date")
    search_fields = ("advertiser__name", "partner__name", "notes")
    list_per_page = 50
    ordering = ("-effective_date",)
    date_hierarchy = "effective_date"
    readonly_fields = ("effective_date", "created_at")

@admin.register(RevenueRuleHistory)
class RevenueRuleHistoryAdmin(ImportExportModelAdmin):
    list_display = ("advertiser", "effective_date", "rev_ftu_rate", "rev_rtu_rate", "rev_rate_type", "currency", "assigned_by")
    list_filter = ("advertiser", "rev_rate_type", "currency", "effective_date")
    search_fields = ("advertiser__name", "notes")
    list_per_page = 50
    ordering = ("-effective_date",)
    date_hierarchy = "effective_date"
    readonly_fields = ("effective_date", "created_at")

@admin.register(RawAdvertiserRecord)
class RawAdvertiserRecordAdmin(ImportExportModelAdmin):
    list_display = ("advertiser", "source", "date_from", "date_to", "date_fetched")
    list_filter = ("advertiser", "source", "date_from", "date_to")
    search_fields = ("advertiser__name", "source")
    list_per_page = 50
    ordering = ("-date_fetched",)
    date_hierarchy = "date_fetched"
    readonly_fields = ("date_fetched",)

@admin.register(CampaignPerformance)
class PerformanceRecordAdmin(ImportExportModelAdmin):
    list_display = (
        "date", "advertiser", "partner", "coupon", "geo",
        "ftu_orders", "rtu_orders", "total_orders",
        "ftu_sales", "rtu_sales", "total_sales",
        "ftu_revenue", "rtu_revenue", "total_revenue",
        "ftu_payout", "rtu_payout", "total_payout"
    )
    list_filter = ("advertiser", "partner", "date", "geo")
    search_fields = ("advertiser__name", "partner__name", "coupon__code")
    list_per_page = 50
    ordering = ("-date",)
    date_hierarchy = "date"
    readonly_fields = ("date",)

@admin.register(NoonNamshiTransaction)
class NoonNamshiTransactionAdmin(ImportExportModelAdmin):
    list_display = (
        "created_date",
        "advertiser_name",
        "country",
        "coupon",
        "user_type",
        "partner_name",
        "partner_type",
        "currency",
        "rate_type",
        "orders",
        "ftu_orders",
        "rtu_orders",
        "sales",
        "commission",
        "our_rev",
        "payout",
        "profit",
        "payout_usd",
        "profit_usd",
    )
    search_fields = ("coupon", "partner_name", "advertiser_name", "country")
    list_filter = ("advertiser_name", "country", "user_type", "partner_type", "currency", "rate_type")

@admin.register(NoonTransaction)
class NoonTransactionAdmin(ImportExportModelAdmin):
    list_display = (
        "order_date",
        "region",
        "country",
        "coupon_code",
        "partner_name",
        "tier_bracket",
        "payable_orders",
        "ftu_orders",
        "rtu_orders",
        "revenue_usd",
        "payout_usd",
        "our_rev_usd",
        "user_type",
    )
    search_fields = ("coupon_code", "partner_name", "country", "tier_bracket")
    list_filter = ("region", "is_gcc", "country", "order_date", "user_type")
    list_per_page = 50
    ordering = ("-order_date",)
    date_hierarchy = "order_date"
    readonly_fields = ("order_id", "created_at", "updated_at")

