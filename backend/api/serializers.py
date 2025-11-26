from rest_framework import serializers # type: ignore
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer # type: ignore
from .models import Advertiser, Partner, PartnerPayout, DepartmentTarget

class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    """
    Custom TokenObtainPair serializer that includes user context in the response
    """
    @classmethod
    def get_token(cls, user):
        token = super().get_token(user)
        # Add custom claims if needed
        return token
    
    def validate(self, attrs):
        data = super().validate(attrs)
        # data contains 'access' and 'refresh' tokens
        return data

class PartnerPayoutSerializer(serializers.ModelSerializer):
    partner_id = serializers.IntegerField(source='partner.id', read_only=True)
    partner_name = serializers.CharField(source='partner.name', read_only=True)
    
    class Meta:
        model = PartnerPayout
        fields = [
            "id", "partner_id", "partner_name", "ftu_payout", "rtu_payout",
            "ftu_fixed_bonus", "rtu_fixed_bonus", "exchange_rate", 
            "currency", "rate_type", "condition", "start_date", "end_date"
        ]

class AdvertiserSerializer(serializers.ModelSerializer):
    class Meta:
        model = Advertiser
        fields = ["id", "name"]

class AdvertiserDetailSerializer(serializers.ModelSerializer):
    partner_payouts = PartnerPayoutSerializer(source='payouts', many=True, read_only=True)
    
    class Meta:
        model = Advertiser
        fields = [
            "id", "name", "attribution", "rev_rate_type", 
            "rev_ftu_rate", "rev_rtu_rate", "rev_ftu_fixed_bonus", "rev_rtu_fixed_bonus",
            "currency", "exchange_rate", "default_payout_rate_type",
            "default_ftu_payout", "default_rtu_payout", 
            "default_ftu_fixed_bonus", "default_rtu_fixed_bonus",
            "partner_payouts"
        ]

class PartnerSerializer(serializers.ModelSerializer):
    class Meta:
        model = Partner
        fields = ["id", "uuid", "name", "partner_type", "email", "phone"]

class DepartmentTargetSerializer(serializers.ModelSerializer):
    advertiser_name = serializers.CharField(source='advertiser.name', read_only=True)
    partner_type_display = serializers.CharField(source='get_partner_type_display', read_only=True)
    assigned_to_username = serializers.CharField(source='assigned_to.user.username', read_only=True, required=False, allow_null=True)
    
    class Meta:
        model = DepartmentTarget
        fields = [
            "id", "month", "advertiser", "advertiser_name", 
            "partner_type", "partner_type_display",
            "assigned_to", "assigned_to_username",
            "orders_target", "revenue_target", "profit_target", "spend_target"
        ]