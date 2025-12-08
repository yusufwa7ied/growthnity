from django.urls import path
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView # type: ignore
from .views import (
    context_view,
    user_dashboard_context,
    kpis_view,
    graph_data_view,
    performance_table_view,
    dashboard_filter_options_view,
    dashboard_pie_chart_data_view,
    advertiser_detail_summary_view,
    coupons_view,
    partner_list_view,
    advertiser_list_view,
    partner_payouts_view,
    partner_payout_detail_view,
    targets_list,
    target_detail,
    performance_analytics_view,
    coupon_detail_view,
    coupon_history_view,
    trigger_pipeline_upload,
    token_refresh_view,
    team_members_list,
)
from .views_export import export_performance_report
from . import views_tracking
from .views_admin import (
    high_level_dashboard_view,
    list_advertisers_view,
    create_advertiser_view,
    update_advertiser_view,
    delete_advertiser_view,
    media_buyer_spend_view,
    delete_media_buyer_spend_view,
    media_buyer_spend_analytics_view,
    partners_view,
    partner_detail_view,
)

urlpatterns = [
    path("click/", views_tracking.redirect_tracking_click, name="tracking-click"),
    path("generate-link/", views_tracking.generate_tracking_link, name="generate-tracking-link"),
    path('context/', context_view),
    path('login/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    path('dashboard/context/', user_dashboard_context),
    path('dashboard/kpis/', kpis_view),
    path("dashboard/graph-data/", graph_data_view),
    path("dashboard/high-level/", high_level_dashboard_view),
    path("dashboard/performance-table/", performance_table_view),
    path("dashboard/export-report/", export_performance_report, name="export-performance-report"),
    path("dashboard/filter-options/", dashboard_filter_options_view),
    path("dashboard/pie-chart-data/", dashboard_pie_chart_data_view),
    path("dashboard/advertiser-detail-summary/", advertiser_detail_summary_view),
    path("coupons/", coupons_view, name="coupons"), # type: ignore
    path("coupons/<str:code>/", coupon_detail_view, name="coupon-detail"),
    path("coupons/<str:code>/history/", coupon_history_view, name="coupon-history"),
    path("partners/", partner_list_view, name="partner-list"),
    path("advertisers/", advertiser_list_view, name="advertiser-list"),
    path("team-members/", team_members_list, name="team-members-list"),
    path("payouts/", partner_payouts_view, name="partner-payouts"),# type: ignore
    path("payouts/<int:pk>/", partner_payout_detail_view, name="partner-payout-detail"),# type: ignore
    
    # Advertiser management endpoints
    path("admin/advertisers/", list_advertisers_view, name="admin-advertisers-list"),
    path("admin/advertisers/create/", create_advertiser_view, name="admin-advertisers-create"),
    path("admin/advertisers/<int:pk>/", update_advertiser_view, name="admin-advertisers-update"),
    path("admin/advertisers/<int:pk>/delete/", delete_advertiser_view, name="admin-advertisers-delete"),
    
    # Targets management endpoints
    path("targets/", targets_list, name="targets-list"),
    path("targets/<int:pk>/", target_detail, name="target-detail"),
    
    # Analytics endpoints
    path("analytics/performance/", performance_analytics_view, name="performance-analytics"),
    
    # Media Buyer Spend endpoints
    path("media-buyer-spend/", media_buyer_spend_view, name="media-buyer-spend"),
    path("media-buyer-spend/<int:pk>/delete/", delete_media_buyer_spend_view, name="media-buyer-spend-delete"),
    path("media-buyer-spend/analytics/", media_buyer_spend_analytics_view, name="media-buyer-spend-analytics"),
    
    # Partner management endpoints
    path("admin/partners/", partners_view, name="admin-partners"),
    path("admin/partners/<int:pk>/", partner_detail_view, name="admin-partner-detail"),
    
    # Pipeline management endpoints
    path("pipelines/trigger/", trigger_pipeline_upload, name="pipeline-trigger"),
]
