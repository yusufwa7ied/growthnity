from django.shortcuts import redirect
from django.utils.timezone import now
from django.http import HttpResponseBadRequest
from rest_framework.decorators import api_view # type: ignore
from rest_framework.response import Response # type: ignore
from .models import ClickRecord, Advertiser, CompanyUser, Partner


@api_view(["GET"])
def redirect_tracking_click(request):
    advertiser_id = request.GET.get("advertiser_id")
    who = request.GET.get("who")  # affiliate, influencer, or media
    who_id = request.GET.get("who_id")
    final_url = request.GET.get("url")

    if not (advertiser_id and who and who_id and final_url):
        return HttpResponseBadRequest("Missing required parameters")

    # Resolve models based on 'who'
    advertiser = Advertiser.objects.filter(id=advertiser_id).first()
    company_user = None
    partner = None
    if who in ["affiliate", "influencer"]:
        partner = Partner.objects.filter(id=who_id, partner_type=who).first()
    elif who == "media":
        company_user = CompanyUser.objects.filter(id=who_id).first()
    else:
        return HttpResponseBadRequest("Invalid 'who' parameter")

    # Create click record
    ClickRecord.objects.create(
        advertiser=advertiser,
        partner=partner,
        company_user=company_user,
        destination_url=final_url,
        user_agent=request.META.get("HTTP_USER_AGENT", ""),
        ip_address=request.META.get("REMOTE_ADDR", ""),
        timestamp=now()
    )

    return redirect(final_url)


# New view to generate tracking link
@api_view(["GET"])
def generate_tracking_link(request):
    advertiser_id = request.GET.get("advertiser_id")
    who = request.GET.get("who")  # affiliate, influencer, or media
    who_id = request.GET.get("who_id")
    final_url = "https://www.google.com"  # e.g., "https://booking.com"

    if not (advertiser_id and who and who_id and final_url):
        return HttpResponseBadRequest("Missing required parameters")

    try:
        advertiser = Advertiser.objects.get(id=advertiser_id)
    except Advertiser.DoesNotExist:
        return HttpResponseBadRequest("Invalid advertiser ID")

    if who in ["affiliate", "influencer"]:
        if not Partner.objects.filter(id=who_id, partner_type=who).exists():
            return HttpResponseBadRequest("Invalid partner ID")

    # Construct redirect URL
    tracking_url = (
        f"https://yourdomain.com/api/track-click/"
        f"?advertiser_id={advertiser_id}&who={who}&who_id={who_id}&url={final_url}"
    )

    return Response({"tracking_url": tracking_url})