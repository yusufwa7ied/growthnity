export interface Coupon {
    code: string;
    advertiser: string;
    advertiser_id: number;
    partner: string;
    partner_id: number | null;
    geo: string;
    discount: number;
}

export interface CouponStats {
    total_coupons: number;
    assigned_coupons: number;
    unassigned_coupons: number;
    advertisers_count: number;
    partners_count: number;
}

export interface CouponFormData {
    codes?: string;  // For new coupons (comma/newline separated)
    advertiser?: number;
    partner?: number;
    geo?: string;
    discount_percent?: number;
    existing_coupons?: string[];  // For assigning existing coupons
}

export interface CouponAssignmentHistory {
    coupon: string;
    partner: string;
    assigned_date: string;
    discount_percent: number | null;
    assigned_by: string | null;
}
