export interface User {
  username: string;
  role: string;
  partner_id?: number;
  department?: string;
}

export interface LoginCredentials {
  username: string;
  password: string;
}

export interface LoginResponse {
  access: string;
}

export interface DashboardContext {
  role: string;
  partner_id?: number;
  department?: string;
}
