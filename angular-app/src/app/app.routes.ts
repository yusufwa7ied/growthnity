import { Routes } from '@angular/router';
import { authGuard } from './core/guards/auth.guard';
import { mediaBuyerGuard } from './core/guards/media-buyer.guard';

export const routes: Routes = [
  {
    path: '',
    canActivate: [authGuard],
    loadComponent: () => {
      const userStr = localStorage.getItem('user');
      if (userStr) {
        const user = JSON.parse(userStr);
        if (user.role === 'team_member') {
          window.location.href = '/media-buyer-spend';
        } else {
          window.location.href = '/dashboard';
        }
      }
      return import('./features/auth/login/login.component').then(m => m.LoginComponent);
    }
  },
  {
    path: 'login',
    loadComponent: () => import('./features/auth/login/login.component').then(m => m.LoginComponent)
  },
  {
    path: 'dashboard',
    loadComponent: () => import('./features/dashboard/dashboard.component').then(m => m.DashboardComponent),
    canActivate: [authGuard]
  },
  {
    path: 'coupons',
    loadComponent: () => import('./features/coupons/coupons.component').then(m => m.CouponsComponent),
    canActivate: [authGuard]
  },
  {
    path: 'advertisers',
    loadComponent: () => import('./features/advertisers/advertisers.component').then(m => m.AdvertisersComponent),
    canActivate: [authGuard]
  },
  {
    path: 'targets',
    loadComponent: () => import('./features/targets/targets.component').then(m => m.TargetsComponent),
    canActivate: [authGuard]
  },
  {
    path: 'partners',
    loadComponent: () => import('./features/partners/partners.component').then(m => m.PartnersComponent),
    canActivate: [authGuard]
  },
  {
    path: 'media-buyer-spend',
    loadComponent: () => import('./features/media-buyer-spend/media-buyer-spend.component').then(m => m.MediaBuyerSpendComponent),
    canActivate: [mediaBuyerGuard]
  },
  {
    path: 'my-coupons',
    loadComponent: () => import('./features/my-coupons/my-coupons.component').then(m => m.MyCouponsComponent),
    canActivate: [authGuard]
  },
  {
    path: 'campaigns',
    loadComponent: () => import('./features/campaigns/campaigns.component').then(m => m.CampaignsComponent),
    canActivate: [authGuard]
  },
  {
    path: '**',
    redirectTo: '/login'
  }
];
