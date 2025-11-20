import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { AuthService } from '../services/auth.service';

export const mediaBuyerGuard: CanActivateFn = (route, state) => {
    const authService = inject(AuthService);
    const router = inject(Router);

    if (!authService.isAuthenticated()) {
        router.navigate(['/login']);
        return false;
    }

    const user = authService.currentUser();
    const role = user?.role || '';
    const department = user?.department || '';

    // Allow Admin, OpsManager, or TeamMembers in media_buying department
    if (role === 'Admin' || role === 'OpsManager') {
        return true;
    }

    if (role === 'TeamMember' && department === 'media_buying') {
        return true;
    }

    // Redirect others to dashboard
    router.navigate(['/dashboard']);
    return false;
};
