from django.db import models
from django.contrib.auth.models import User
import uuid
import datetime


class RawAdvertiserRecord(models.Model):
    advertiser = models.ForeignKey(
        "Advertiser",
        on_delete=models.CASCADE,
        related_name="raw_records"
    )

    source = models.CharField(
        max_length=100,
        help_text="Where the raw data came from (API, CSV, Sheet, etc.)"
    )

    date_from = models.DateField()
    date_to = models.DateField()

    data = models.JSONField(
        help_text="Full raw data snapshot in JSON format (unmodified)."
    )

    date_fetched = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-date_fetched"]  # newest first
        verbose_name = "Raw Advertiser Snapshot"
        verbose_name_plural = "Raw Advertiser Snapshots"

    def __str__(self):
        return f"{self.advertiser.name} | {self.source} | {self.date_from} → {self.date_to}"




class Advertiser(models.Model):
    ATTRIBUTION_CHOICES = [
        ("Coupon", "Coupon"),
        ("Link", "Link"),
    ]

    RATE_TYPE_CHOICES = [
        ("percent", "Percent"),
        ("fixed", "Fixed"),
    ]

    CURRENCY_CHOICES = [
        ("AED", "AED"),
        ("SAR", "SAR"),
        ("EGP", "EGP"),
        ("USD", "USD"),
    ]

    name = models.CharField(max_length=200)
    attribution = models.CharField(max_length=20, choices=ATTRIBUTION_CHOICES)

    # ✅ How advertiser pays us (revenue rules)
    rev_rate_type = models.CharField(
        max_length=10,
        choices=RATE_TYPE_CHOICES,
        default="percent",
        help_text="Percent or Fixed per order"
    )
    rev_ftu_rate = models.DecimalField(
        max_digits=6, decimal_places=2, null=True, blank=True,
        help_text="FTU revenue rate (% or fixed amount)"
    )
    rev_rtu_rate = models.DecimalField(
        max_digits=6, decimal_places=2, null=True, blank=True,
        help_text="RTU revenue rate (% or fixed amount)"
    )
    rev_ftu_fixed_bonus = models.DecimalField(
        max_digits=8, decimal_places=2, null=True, blank=True,
        help_text="Fixed bonus per FTU order (e.g., 3 AED)"
    )
    rev_rtu_fixed_bonus = models.DecimalField(
        max_digits=8, decimal_places=2, null=True, blank=True,
        help_text="Fixed bonus per RTU order"
    )

    # ✅ NEW: Advertiser payout currency (their currency)
    currency = models.CharField(
        max_length=10,
        choices=CURRENCY_CHOICES,
        default="AED"
    )

    # ✅ NEW: Exchange rate to USD (we'll convert revenue using this)
    exchange_rate = models.DecimalField(
        max_digits=10,
        decimal_places=4,
        null=True,
        blank=True,
        help_text="How many USD per 1 unit of advertiser's currency"
    )

    # ✅ Default payout rates (what we pay partners by default)
    default_payout_rate_type = models.CharField(
        max_length=10,
        choices=RATE_TYPE_CHOICES,
        default="percent",
        help_text="Default payout type for all partners"
    )
    default_ftu_payout = models.DecimalField(
        max_digits=6,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Default FTU payout for all partners (% or fixed)"
    )
    default_rtu_payout = models.DecimalField(
        max_digits=6,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Default RTU payout for all partners (% or fixed)"
    )
    default_ftu_fixed_bonus = models.DecimalField(
        max_digits=8,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Default fixed bonus per FTU order for all partners (e.g., 3 AED)"
    )
    default_rtu_fixed_bonus = models.DecimalField(
        max_digits=8,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Default fixed bonus per RTU order for all partners"
    )

    def __str__(self):
        return self.name

class AdvertiserRate(models.Model):
    USER_TYPE_CHOICES = [
        ("FTU", "First Time User"),
        ("RTU", "Returning User"),
    ]
    RATE_TYPE_CHOICES = [
        ("percent", "Percent"),
        ("flat", "Flat"),
    ]

    advertiser = models.ForeignKey(
        "Advertiser",
        on_delete=models.CASCADE,
        related_name="rates",
        db_column="advertiser_id"
    )
    geo = models.CharField(max_length=50)
    user_type = models.CharField(max_length=10, default="FTU")
    rate_type = models.CharField(max_length=10, choices=RATE_TYPE_CHOICES)
    currency = models.CharField(max_length=10, blank=True, null=True)

    def __str__(self):
        return f"{self.advertiser.name} | {self.geo} | {self.user_type} | {self.rate_type}"

class Partner(models.Model):
    PARTNER_TYPES = [
        ("AFF", "Affiliate"),
        ("INF", "Influencer"),
        ("MB", "Media Buying"),
    ]

    uuid = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    name = models.CharField(max_length=200)
    partner_type = models.CharField(max_length=10, choices=PARTNER_TYPES)

    # contact info
    email = models.EmailField(max_length=254, blank=True, null=True)
    phone = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        verbose_name = "Partner"
        verbose_name_plural = "Partners"

    def __str__(self):
        return f"{self.name} ({self.partner_type})"

class CompanyRole(models.Model):
    name = models.CharField(max_length=100)  # CEO, OpsManager, TeamLeader, etc.

    def __str__(self):
        return self.name

class CompanyUser(models.Model):
    DEPARTMENT_CHOICES = [
        ("affiliate", "Affiliate"),
        ("influencer", "Influencer"),
        ("media_buying", "Media Buying"),
    ]

    user = models.OneToOneField(User, on_delete=models.CASCADE, null=True, blank=True)
    role = models.ForeignKey(CompanyRole, on_delete=models.SET_NULL, null=True)
    department = models.CharField(max_length=20, choices=DEPARTMENT_CHOICES, blank=True, null=True)
    department_head = models.ForeignKey("self", on_delete=models.SET_NULL, null=True, blank=True, related_name="team_members")
    ops_manager = models.ForeignKey("self", on_delete=models.SET_NULL, null=True, blank=True, related_name="ops_team_members")
    phone = models.CharField(max_length=20, blank=True, null=True)

    def __str__(self):
        username = self.user.username if self.user else "NoUser"
        role = self.role.name if self.role else "NoRole"
        return f"{username} ({role})"

class AccountAssignment(models.Model):
    """
    Connects a CompanyUser to one or more Advertisers and/or Affiliates or Influencers.
    For example: AccountManager X manages Advertiser A, Advertiser B, Affiliates Y and Z, and Influencers P and Q.
    """
    company_user = models.ForeignKey(CompanyUser, null=True, blank=True, on_delete=models.CASCADE)
    advertisers = models.ManyToManyField(Advertiser, blank=True)
    partners = models.ManyToManyField("Partner", blank=True)

    def __str__(self):
        advertisers = ", ".join(a.name for a in self.advertisers.all()) if self.advertisers.exists() else "NoAdvertiser"
        partners = ", ".join(p.name for p in self.partners.all()) if self.partners.exists() else "NoPartner"
        return f"{self.company_user or 'NoUser'} -> {advertisers} / {partners}"

RATE_TYPE_CHOICES = [
    ("percent", "Percent"),
    ("flat", "Flat"),
]

class PartnerPayout(models.Model):
    advertiser = models.ForeignKey(
        "Advertiser",
        on_delete=models.CASCADE,
        related_name="payouts"
    )
    partner = models.ForeignKey(
        "Partner",
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="payouts"
    )

    ftu_payout = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    rtu_payout = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    ftu_fixed_bonus = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    rtu_fixed_bonus = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    exchange_rate = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    currency = models.CharField(max_length=10, null=True, blank=True)
    rate_type = models.CharField(max_length=10, choices=RATE_TYPE_CHOICES, default="percent")
    condition = models.CharField(max_length=255, null=True, blank=True)
    start_date = models.DateField(null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["advertiser", "partner", "start_date"], name="uniq_advertiser_partner_payout_period")
        ]
        verbose_name = "Partner Payout"
        verbose_name_plural = "Partner Payouts"

    def __str__(self):
        partner_name = self.partner.name if self.partner else "Default"
        return f"{self.advertiser.name} → {partner_name} | FTU: {self.ftu_payout} / RTU: {self.rtu_payout}"

class ClickRecord(models.Model):
    """
    Stores raw clicks with identifiers for attribution.
    Can be extended later with user agent, IP, geo, etc.
    """
    uuid = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    advertiser = models.ForeignKey(Advertiser, on_delete=models.CASCADE)
    partner = models.ForeignKey("Partner", on_delete=models.SET_NULL, null=True, blank=True)
    department = models.CharField(max_length=20, choices=CompanyUser.DEPARTMENT_CHOICES, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    destination_url = models.URLField(max_length=500)
    company_user = models.ForeignKey(CompanyUser, on_delete=models.SET_NULL, null=True, blank=True)
    user_agent = models.TextField(blank=True, null=True)
    ip_address = models.GenericIPAddressField(blank=True, null=True)
    timestamp = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.created_at.date()} - {self.advertiser.name} - {self.partner or '—'}"
        
class DrNutritionTransaction(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # 1) Identifiers
    order_id = models.BigIntegerField()
    created_date = models.DateTimeField(null=True, blank=True)

    # 2) Status
    delivery_status = models.CharField(max_length=100, null=True, blank=True, default="")

    # 3) Core metadata
    country = models.CharField(max_length=10)
    coupon = models.CharField(max_length=50)
    user_type = models.CharField(max_length=10)

    partner_name = models.CharField(max_length=150, null=True, blank=True)
    partner_type = models.CharField(max_length=20, null=True, blank=True)

    advertiser_name = models.CharField(max_length=150)
    currency = models.CharField(max_length=10)
    rate_type = models.CharField(max_length=20)

    # 4) Money
    sales = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    commission = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    our_rev = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    # 5) Order flags
    ftu_orders = models.IntegerField(default=0)
    rtu_orders = models.IntegerField(default=0)
    orders = models.IntegerField(default=1)

    # 6) Rates
    ftu_rate = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    rtu_rate = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    # 7) Calculated payout
    payout = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    profit = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    payout_usd = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    profit_usd = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    def __str__(self):
        return f"{self.order_id} - {self.created_date}"
    
class StyliTransaction(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # 1) Identifiers
    order_id = models.BigIntegerField()
    created_date = models.DateTimeField(null=True, blank=True)

    # 2) Status (Styli always treated as delivered)
    delivery_status = models.CharField(max_length=100, null=True, blank=True, default="delivered")

    # 3) Core metadata
    country = models.CharField(max_length=10)
    coupon = models.CharField(max_length=50)
    user_type = models.CharField(max_length=10)  # FTU or RTU

    partner_name = models.CharField(max_length=150, null=True, blank=True)
    partner_type = models.CharField(max_length=20, null=True, blank=True)

    advertiser_name = models.CharField(max_length=150)
    currency = models.CharField(max_length=10)   # AED always
    rate_type = models.CharField(max_length=20)  # percent / fixed

    # 4) Money (AED)
    sales = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    commission = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    our_rev = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    # 5) Order flags
    ftu_orders = models.IntegerField(default=0)
    rtu_orders = models.IntegerField(default=0)
    orders = models.IntegerField(default=1)

    # 6) Payout rule rates
    ftu_rate = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    rtu_rate = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    # 7) Calculated payout & profit
    payout = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    profit = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    # 8) USD conversions
    payout_usd = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    profit_usd = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    def __str__(self):
        return f"{self.order_id} | {self.created_date} | {self.coupon}"
    
class SpringRoseTransaction(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # 1) Identifiers
    order_id = models.CharField(max_length=50)
    created_date = models.DateTimeField(null=True, blank=True)

    # 2) Status
    delivery_status = models.CharField(max_length=100, null=True, blank=True, default="delivered")

    # 3) Core metadata
    country = models.CharField(max_length=10)
    coupon = models.CharField(max_length=50)
    user_type = models.CharField(max_length=10)  # FTU or RTU

    partner_name = models.CharField(max_length=150, null=True, blank=True)
    partner_type = models.CharField(max_length=20, null=True, blank=True)

    advertiser_name = models.CharField(max_length=150)
    currency = models.CharField(max_length=10)   # e.g. SAR, AED, USD
    rate_type = models.CharField(max_length=20)  # percent / fixed

    # 4) Money (local currency)
    sales = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    commission = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    our_rev = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    # 5) Order flags
    ftu_orders = models.IntegerField(default=0)
    rtu_orders = models.IntegerField(default=0)
    orders = models.IntegerField(default=1)

    # 6) Payout rule rates
    ftu_rate = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    rtu_rate = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    # 7) Calculated payout & profit
    payout = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    profit = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    # 8) USD conversions
    payout_usd = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    profit_usd = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    def __str__(self):
        return f"{self.order_id} | {self.created_date} | {self.coupon}"

class NoonNamshiTransaction(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # 1) Identifiers (aggregated source → no per-order ID; store 0)
    order_id = models.BigIntegerField(default=0)
    created_date = models.DateTimeField(null=True, blank=True)

    # 2) Status
    delivery_status = models.CharField(max_length=100, null=True, blank=True, default="delivered")

    # 3) Core metadata
    country = models.CharField(max_length=10)
    coupon = models.CharField(max_length=50)
    user_type = models.CharField(max_length=10)  # FTU / RTU

    partner_name = models.CharField(max_length=150, null=True, blank=True)
    partner_type = models.CharField(max_length=20, null=True, blank=True)

    advertiser_name = models.CharField(max_length=150)
    currency = models.CharField(max_length=10)          # pulled from Advertiser
    rate_type = models.CharField(max_length=20)         # pulled from Advertiser (percent/fixed)

    # 4) Money (AED/SAR/etc per Advertiser)
    sales = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    commission = models.DecimalField(max_digits=12, decimal_places=4, default=0)   # from client (often 0 in CSV)
    our_rev = models.DecimalField(max_digits=12, decimal_places=4, default=0)      # calculated from Advertiser rev rules

    # 5) Order flags
    ftu_orders = models.IntegerField(default=0)
    rtu_orders = models.IntegerField(default=0)
    orders = models.IntegerField(default=1)

    # 6) Payout rule rates (PartnerPayout → %, fixed, + bonus)
    ftu_rate = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    rtu_rate = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    # 7) Calculated payout & profit
    payout = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    profit = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    # 8) USD conversions
    payout_usd = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    profit_usd = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    def __str__(self):
        return f"{self.advertiser_name} | {self.created_date} | {self.coupon} | {self.user_type}"
    
class PartnerizeConversion(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Original fields
    conversion_id = models.CharField(max_length=100, unique=True)
    campaign_title = models.CharField(max_length=255, null=True, blank=True)
    conversion_time = models.DateTimeField()
    country = models.CharField(max_length=10, null=True, blank=True)
    
    total_order_value = models.FloatField(null=True, blank=True)
    total_commission = models.FloatField(null=True, blank=True)
    conversion_status = models.CharField(max_length=50, null=True, blank=True)
    
    voucher = models.CharField(max_length=100, null=True, blank=True)
    first_time_user = models.BooleanField(null=True, blank=True)

    # Pipeline enriched fields (same as other transaction models)
    partner_name = models.CharField(max_length=150, null=True, blank=True)
    partner_type = models.CharField(max_length=20, null=True, blank=True)
    advertiser_name = models.CharField(max_length=150, null=True, blank=True)
    currency = models.CharField(max_length=10, null=True, blank=True)
    rate_type = models.CharField(max_length=20, null=True, blank=True)
    
    # Money fields
    sales = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    commission = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    our_rev = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    
    # Order tracking
    ftu_orders = models.IntegerField(default=0)
    rtu_orders = models.IntegerField(default=0)
    orders = models.IntegerField(default=0)
    
    # Rates & payouts
    ftu_rate = models.DecimalField(max_digits=6, decimal_places=2, default=0)
    rtu_rate = models.DecimalField(max_digits=6, decimal_places=2, default=0)
    payout = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    profit = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    payout_usd = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    profit_usd = models.DecimalField(max_digits=12, decimal_places=4, default=0)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.conversion_id} | {self.campaign_title}"
    
class MediaBuyerDailySpend(models.Model):
    PLATFORM_CHOICES = [
        ("Meta", "Meta (Facebook/Instagram)"),
        ("Snapchat", "Snapchat"),
        ("TikTok", "TikTok"),
        ("Google", "Google Ads"),
        ("Twitter", "Twitter/X"),
        ("LinkedIn", "LinkedIn"),
        ("YouTube", "YouTube"),
        ("Other", "Other"),
    ]
    
    date = models.DateField()
    advertiser = models.ForeignKey("Advertiser", on_delete=models.CASCADE, related_name="daily_spends")
    partner = models.ForeignKey("Partner", on_delete=models.CASCADE, related_name="daily_spends", null=True, blank=True)
    coupon = models.ForeignKey("Coupon", on_delete=models.SET_NULL, related_name="daily_spends", null=True, blank=True)
    platform = models.CharField(max_length=50, choices=PLATFORM_CHOICES, default="Meta")
    amount_spent = models.DecimalField(max_digits=10, decimal_places=2)
    currency = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        unique_together = ("date", "advertiser", "partner", "coupon", "platform")
        verbose_name = "Media Buyer Daily Spend"

    def __str__(self):
        coupon_str = f" | {self.coupon.code}" if self.coupon else ""
        return f"{self.partner.name} → {self.advertiser.name} on {self.date} | {self.platform}{coupon_str}: ${self.amount_spent}" # type: ignore
        
class Coupon(models.Model):
    code = models.CharField(max_length=50)
    advertiser = models.ForeignKey("Advertiser", on_delete=models.CASCADE, related_name="coupons")
    partner = models.ForeignKey("Partner", on_delete=models.CASCADE, related_name="coupons", null=True, blank=True)
    geo = models.CharField(max_length=10, null=True, blank=True)
    discount_percent = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)

    class Meta:
        unique_together = [['code', 'advertiser']]

    def __str__(self):
        advertiser_name = self.advertiser.name if self.advertiser else "(No Advertiser)"
        partner_name = self.partner.name if self.partner else "(No Partner)"
        geo = self.geo if self.geo else "-"
        return f"{self.code}"

class DepartmentTarget(models.Model):
    PARTNER_TYPE_CHOICES = [
        ("MB", "Media Buyer"),
        ("AFF", "Affiliate"),
        ("INF", "Influencer"),
    ]

    month = models.DateField(help_text="First day of month, e.g. 2025-10-01")
    advertiser = models.ForeignKey(
        "Advertiser",
        on_delete=models.CASCADE,
        related_name="partner_targets"
    )
    partner_type = models.CharField(max_length=3, choices=PARTNER_TYPE_CHOICES)
    
    # Optional: Assign to specific team member (for Affiliate/Influencer individual targets)
    assigned_to = models.ForeignKey(
        CompanyUser,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="individual_targets",
        help_text="Leave blank for department-level target, select for individual team member target"
    )

    # Common targets
    orders_target = models.IntegerField()
    revenue_target = models.DecimalField(max_digits=12, decimal_places=2)
    profit_target = models.DecimalField(max_digits=12, decimal_places=2)

    # Media Buyers only
    spend_target = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    class Meta:
        unique_together = ("month", "advertiser", "partner_type", "assigned_to")
        verbose_name = "Target"
        verbose_name_plural = "Targets"

    def __str__(self):
        target_type = f" | {self.assigned_to.user.username}" if self.assigned_to else f" | {self.get_partner_type_display()}"
        return f"{self.advertiser.name}{target_type} - {self.month.strftime('%B %Y')}" # type: ignore
    
class CampaignPerformance(models.Model):
    date = models.DateField()
    advertiser = models.ForeignKey("Advertiser", on_delete=models.CASCADE, null=True, blank=True, related_name="performance_records")
    partner = models.ForeignKey("Partner", on_delete=models.SET_NULL, null=True, blank=True, related_name="performance_records")
    coupon = models.ForeignKey("Coupon", on_delete=models.SET_NULL, null=True, blank=True, related_name="performance_records")
    geo = models.CharField(max_length=10, null=True, blank=True)

    # Orders
    ftu_orders = models.IntegerField(default=0)
    rtu_orders = models.IntegerField(default=0)
    total_orders = models.IntegerField(default=0)

    # Sales
    ftu_sales = models.DecimalField(max_digits=12, decimal_places=2, default=0)# type: ignore
    rtu_sales = models.DecimalField(max_digits=12, decimal_places=2, default=0)# type: ignore
    total_sales = models.DecimalField(max_digits=12, decimal_places=2, default=0)# type: ignore

    # Revenue
    ftu_revenue = models.DecimalField(max_digits=12, decimal_places=2, default=0)# type: ignore
    rtu_revenue = models.DecimalField(max_digits=12, decimal_places=2, default=0)# type: ignore
    total_revenue = models.DecimalField(max_digits=12, decimal_places=2, default=0)# type: ignore
    ftu_our_rev = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    rtu_our_rev = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    total_our_rev = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    # Payout
    ftu_payout = models.DecimalField(max_digits=12, decimal_places=2, default=0)# type: ignore
    rtu_payout = models.DecimalField(max_digits=12, decimal_places=2, default=0)# type: ignore
    total_payout = models.DecimalField(max_digits=12, decimal_places=2, default=0) # type: ignore

    class Meta:
        pass

    def __str__(self):
        return f"{self.date} | {self.advertiser.name} | {self.partner.name if self.partner else 'No Partner'}" # type: ignore

class CouponAssignmentHistory(models.Model):
    coupon = models.ForeignKey("Coupon", on_delete=models.CASCADE, related_name="history")
    partner = models.ForeignKey("Partner", on_delete=models.CASCADE, related_name="coupon_assignments")
    assigned_date = models.DateTimeField(auto_now_add=True)  # previously DateField
    discount_percent = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    assigned_by = models.ForeignKey("auth.User", on_delete=models.SET_NULL, null=True, blank=True)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["-assigned_date"]

    def __str__(self):
        return f"{self.coupon.code} → {self.partner.name} on {self.assigned_date}"


class PayoutRuleHistory(models.Model):
    """
    Tracks all payout rule changes over time.
    When PartnerPayout is created/updated, a history record is saved.
    This allows correct payout calculation based on transaction date.
    """
    advertiser = models.ForeignKey("Advertiser", on_delete=models.CASCADE, related_name="payout_history")
    partner = models.ForeignKey("Partner", on_delete=models.CASCADE, null=True, blank=True, related_name="payout_history")
    
    # When this rule became effective
    effective_date = models.DateTimeField()
    
    # Payout configuration
    ftu_payout = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    rtu_payout = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    ftu_fixed_bonus = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    rtu_fixed_bonus = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    rate_type = models.CharField(max_length=10, choices=RATE_TYPE_CHOICES, default="percent")
    
    # Metadata
    assigned_by = models.ForeignKey("auth.User", on_delete=models.SET_NULL, null=True, blank=True)
    notes = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-effective_date"]
        verbose_name = "Payout Rule History"
        verbose_name_plural = "Payout Rule History"

    def __str__(self):
        partner_name = self.partner.name if self.partner else "Default"
        return f"{self.advertiser.name} → {partner_name} | FTU: {self.ftu_payout}% / RTU: {self.rtu_payout}% (from {self.effective_date.date()})"


class RevenueRuleHistory(models.Model):
    """
    Tracks all advertiser revenue rule changes over time.
    When Advertiser revenue fields are updated, a history record is saved.
    This ensures correct revenue calculation based on transaction date.
    """
    advertiser = models.ForeignKey("Advertiser", on_delete=models.CASCADE, related_name="revenue_history")
    
    # When this rule became effective
    effective_date = models.DateTimeField()
    
    # Revenue configuration
    rev_rate_type = models.CharField(max_length=10, choices=RATE_TYPE_CHOICES, default="percent")
    rev_ftu_rate = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    rev_rtu_rate = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    rev_ftu_fixed_bonus = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    rev_rtu_fixed_bonus = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    
    # Exchange rate at this time
    currency = models.CharField(max_length=10, default="AED")
    exchange_rate = models.DecimalField(max_digits=10, decimal_places=4, null=True, blank=True)
    
    # Metadata
    assigned_by = models.ForeignKey("auth.User", on_delete=models.SET_NULL, null=True, blank=True)
    notes = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-effective_date"]
        verbose_name = "Revenue Rule History"
        verbose_name_plural = "Revenue Rule History"

    def __str__(self):
        return f"{self.advertiser.name} | FTU: {self.rev_ftu_rate}% / RTU: {self.rev_rtu_rate}% (from {self.effective_date.date()})"


