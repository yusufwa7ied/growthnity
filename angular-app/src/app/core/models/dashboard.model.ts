export interface DashboardFilters {
  advertiser_id?: number | number[] | null;
  partner_id?: number | number[] | null;
  team_member_id?: number | number[] | null;
  coupon_code?: string | string[] | null;
  partner_type?: string | null;
  geo?: string | string[] | null;
  date_from?: string;
  date_to?: string;
}

export interface Advertiser {
  id: number;
  name: string;
  attribution: string;
  geo?: string;
  compositeKey?: string;
}

export interface Partner {
  id: number;
  name: string;
  type: string;
}

export interface TeamMember {
  id: number;
  name: string;
  role: string;
}

export interface Coupon {
  code: string;
  advertiser: string;
  advertiser_id: number;
  partner: string;
  partner_id?: number;
  partner_type?: string;
  geo?: string;
  discount?: number;
}

export interface KPIData {
  total_orders: number;
  total_sales: number;
  total_revenue: number;
  total_payout: number;
  total_profit: number;
  records_count: number;
  trends?: {
    orders_change?: number;
    sales_change?: number;
    revenue_change?: number;
    payout_change?: number;
    profit_change?: number;
  };
}

export interface GraphData {
  dates: string[];
  daily_sales: number[];
  daily_revenue?: number[];
  daily_payout: number[];
  daily_profit?: number[];
}

export interface TableRow {
  date: string;
  advertiser_id: number;
  partner_id?: number;
  campaign: string;
  coupon: string;
  partner?: string;
  orders: number;
  sales: number;
  revenue?: number;
  spend?: number;
  payout: number;
  profit?: number;
}

export interface PaginatedTableResponse {
  count: number;
  next: string | null;
  previous: string | null;
  results: TableRow[];
}

export interface FilterOptions {
  advertisers: Array<{
    advertiser_id: number;
    campaign: string;
    geo?: string;
  }>;
  partners: Array<{
    partner_id: number;
    partner: string;
  }>;
  team_members?: Array<{
    company_user_id: number;
    username: string;
    role: string;
  }>;
  coupons: Array<{
    coupon: string;
    advertiser_id: number;
    partner_id?: number;
    partner_type?: string;
  }>;
}

export interface PieChartData {
  campaign: string;
  total_revenue: number;
}

export interface DashboardContext {
  username: string;
  role: string;
  can_see_all?: boolean;
  advertisers?: Advertiser[];
  affiliates?: Partner[];
  influencers?: Partner[];
  media_buyers?: Partner[];
  department?: string;
  advertiser_partner_map?: any;
}

export interface AdvertiserDetailSummary {
  advertiser_id: number;
  advertiser_name: string;
  kpis: {
    orders: number;
    sales: number;
    revenue: number;
    payout: number;
    profit?: number;
  };
  partner_breakdown: Array<{
    type: string;
    label: string;
    revenue: number;
    count: number;
  }>;
  top_coupons: Array<{
    code: string;
    partner: string;
    orders: number;
    revenue: number;
  }>;
  daily_trend: {
    dates: string[];
    revenues: number[];
  };
  can_see_profit: boolean;
}
