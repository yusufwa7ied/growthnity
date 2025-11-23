export interface DepartmentTarget {
    id?: number;
    month: string; // ISO date format "YYYY-MM-01"
    advertiser: number;
    advertiser_name?: string;
    partner_type: 'MB' | 'AFF' | 'INF';
    partner_type_display?: string;
    assigned_to?: number | null; // CompanyUser ID
    assigned_to_username?: string | null; // Team member username
    orders_target: number;
    revenue_target: number;
    profit_target: number;
    spend_target?: number | null;
}

export interface TargetWithActuals extends DepartmentTarget {
    orders_actual: number;
    revenue_actual: number;
    profit_actual: number;
    spend_actual?: number;
    orders_percent: number;
    revenue_percent: number;
    profit_percent: number;
    spend_percent?: number;
}
