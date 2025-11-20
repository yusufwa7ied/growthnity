export interface DashboardFilters {
  advertiser_id?: number | number[] | null;
  partner_id?: number | number[] | null;
  coupon_code?: string | string[] | null;
  partner_type?: string | null;
  date_from?: string;
  date_to?: string;
}

export interface Advertiser {
  id: number;
  name: string;
  attribution: string;
}

export interface Partner {
  id: number;
  name: string;
  type: string;
}

export interface Coupon {
  code: string;
  advertiser: string;
  advertiser_id: number;
  partner: string;
  partner_id?: number;
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
