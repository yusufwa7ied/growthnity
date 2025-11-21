import { CommonModule } from '@angular/common';
import { Component, OnInit, signal } from '@angular/core';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { ButtonModule } from 'primeng/button';
import { CardModule } from 'primeng/card';
import { InputTextModule } from 'primeng/inputtext';
import { MessageModule } from 'primeng/message';
import { PasswordModule } from 'primeng/password';
import { AuthService } from '../../../core/services/auth.service';
import { FooterComponent } from '../../../shared/components/footer/footer.component';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    ButtonModule,
    InputTextModule,
    PasswordModule,
    CardModule,
    MessageModule,
    FooterComponent
  ],
  templateUrl: './login.component.html',
  styleUrl: './login.component.css'
})
export class LoginComponent implements OnInit {
  loginForm: FormGroup;
  errorMessage = signal<string>('');
  isLoading = signal<boolean>(false);
  returnUrl: string = '/dashboard';

  constructor(
    private fb: FormBuilder,
    private authService: AuthService,
    private router: Router,
    private route: ActivatedRoute
  ) {
    this.loginForm = this.fb.group({
      username: ['', [Validators.required]],
      password: ['', [Validators.required]]
    });
  }

  ngOnInit(): void {
    // Check if user is already logged in
    if (this.authService.isAuthenticated()) {
      this.router.navigate(['/dashboard']);
      return;
    }

    // Get return URL from route parameters or default to dashboard
    this.returnUrl = this.route.snapshot.queryParams['returnUrl'] || '/dashboard';
  }

  onSubmit(): void {
    // Prevent double submission
    if (this.isLoading() || this.loginForm.invalid) {
      console.log('🚫 Submit blocked - isLoading:', this.isLoading(), 'invalid:', this.loginForm.invalid);
      if (this.loginForm.invalid) {
        this.errorMessage.set('Please enter both username and password.');
      }
      return;
    }

    console.log('🔐 Starting login...');
    this.isLoading.set(true);
    this.errorMessage.set('');

    const credentials = this.loginForm.value;

    this.authService.login(credentials).subscribe({
      next: () => {
        console.log('✅ Login successful, redirecting...');
        this.isLoading.set(false);
        // Redirect to dashboard for all users
        this.router.navigate(['/dashboard']);
      },
      error: (error) => {
        console.log('❌ Login failed:', error);
        this.isLoading.set(false);
        this.errorMessage.set(error.error || 'Login failed. Please try again.');
      }
    });
  }

  get usernameControl() {
    return this.loginForm.get('username');
  }

  get passwordControl() {
    return this.loginForm.get('password');
  }
}
