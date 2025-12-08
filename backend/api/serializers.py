from rest_framework import serializers # type: ignore
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer # type: ignore
from .models import Advertiser, Partner, PartnerPayout, DepartmentTarget, AdvertiserCancellationRate

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

class AdvertiserCancellationRateSerializer(serializers.ModelSerializer):
    class Meta:
        model = AdvertiserCancellationRate
        fields = [
            "id", "advertiser", "start_date", "end_date", 
            "cancellation_rate", "notes", "created_at"
        ]
        read_only_fields = ["created_at"]

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
    cancellation_rates = AdvertiserCancellationRateSerializer(many=True, read_only=True)
    
    class Meta:
        model = Advertiser
        fields = [
            "id", "name", "attribution", "rev_rate_type", 
            "rev_ftu_rate", "rev_rtu_rate", "rev_ftu_fixed_bonus", "rev_rtu_fixed_bonus",
            "currency", "exchange_rate", "default_payout_rate_type",
            "default_ftu_payout", "default_rtu_payout", 
            "default_ftu_fixed_bonus", "default_rtu_fixed_bonus",
            "partner_payouts", "cancellation_rates"
        ]
    
    def update(self, instance, validated_data):
        """Override update to create RevenueRuleHistory and PayoutRuleHistory when rates change"""
        from django.utils import timezone
        from .models import RevenueRuleHistory, PayoutRuleHistory
        
        # Track if revenue fields changed
        revenue_changed = any([
            'rev_rate_type' in validated_data and validated_data['rev_rate_type'] != instance.rev_rate_type,
            'rev_ftu_rate' in validated_data and validated_data['rev_ftu_rate'] != instance.rev_ftu_rate,
            'rev_rtu_rate' in validated_data and validated_data['rev_rtu_rate'] != instance.rev_rtu_rate,
            'rev_ftu_fixed_bonus' in validated_data and validated_data['rev_ftu_fixed_bonus'] != instance.rev_ftu_fixed_bonus,
            'rev_rtu_fixed_bonus' in validated_data and validated_data['rev_rtu_fixed_bonus'] != instance.rev_rtu_fixed_bonus,
            'currency' in validated_data and validated_data['currency'] != instance.currency,
            'exchange_rate' in validated_data and validated_data['exchange_rate'] != instance.exchange_rate,
        ])
        
        # Track if default payout fields changed
        payout_changed = any([
            'default_payout_rate_type' in validated_data and validated_data['default_payout_rate_type'] != instance.default_payout_rate_type,
            'default_ftu_payout' in validated_data and validated_data['default_ftu_payout'] != instance.default_ftu_payout,
            'default_rtu_payout' in validated_data and validated_data['default_rtu_payout'] != instance.default_rtu_payout,
            'default_ftu_fixed_bonus' in validated_data and validated_data['default_ftu_fixed_bonus'] != instance.default_ftu_fixed_bonus,
            'default_rtu_fixed_bonus' in validated_data and validated_data['default_rtu_fixed_bonus'] != instance.default_rtu_fixed_bonus,
        ])
        
        # Update the instance
        instance = super().update(instance, validated_data)
        
        # Create RevenueRuleHistory if revenue fields changed
        if revenue_changed:
            RevenueRuleHistory.objects.create(
                advertiser=instance,
                effective_date=timezone.now(),
                rev_rate_type=instance.rev_rate_type,
                rev_ftu_rate=instance.rev_ftu_rate,
                rev_rtu_rate=instance.rev_rtu_rate,
                rev_ftu_fixed_bonus=instance.rev_ftu_fixed_bonus,
                rev_rtu_fixed_bonus=instance.rev_rtu_fixed_bonus,
                currency=instance.currency,
                exchange_rate=instance.exchange_rate,
                assigned_by=self.context.get('request').user if self.context.get('request') else None,
                notes=f"Updated via API by {self.context.get('request').user.username if self.context.get('request') else 'system'}"
            )
        
        # Create PayoutRuleHistory if default payout fields changed
        if payout_changed:
            PayoutRuleHistory.objects.create(
                advertiser=instance,
                partner=None,  # NULL = default payouts for all partners
                effective_date=timezone.now(),
                ftu_payout=instance.default_ftu_payout,
                rtu_payout=instance.default_rtu_payout,
                ftu_fixed_bonus=instance.default_ftu_fixed_bonus,
                rtu_fixed_bonus=instance.default_rtu_fixed_bonus,
                rate_type=instance.default_payout_rate_type,
                assigned_by=self.context.get('request').user if self.context.get('request') else None,
                notes=f"Default payout updated via API by {self.context.get('request').user.username if self.context.get('request') else 'system'}"
            )
        
        return instance

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