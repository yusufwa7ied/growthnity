import { CommonModule } from '@angular/common';
import { Component, Input } from '@angular/core';

@Component({
    selector: 'app-skeleton-loader',
    standalone: true,
    imports: [CommonModule],
    template: `
    <div [ngSwitch]="type" class="skeleton-wrapper">
      <!-- KPI Card Skeleton -->
      <ng-container *ngSwitchCase="'kpi'">
        <div class="skeleton-kpi-card">
          <div class="skeleton-kpi-icon"></div>
          <div class="skeleton-kpi-content">
            <div class="skeleton-kpi-label"></div>
            <div class="skeleton-kpi-value"></div>
          </div>
        </div>
      </ng-container>

      <!-- Table Row Skeleton -->
      <ng-container *ngSwitchCase="'table-row'">
        <div class="skeleton-table-row">
          <div class="skeleton-cell"></div>
          <div class="skeleton-cell"></div>
          <div class="skeleton-cell"></div>
          <div class="skeleton-cell"></div>
          <div class="skeleton-cell"></div>
        </div>
      </ng-container>

      <!-- Chart Skeleton -->
      <ng-container *ngSwitchCase="'chart'">
        <div class="skeleton-chart">
          <div class="skeleton-chart-bar" *ngFor="let i of [1,2,3,4,5,6,7,8,9,10]"></div>
        </div>
      </ng-container>

      <!-- Generic Box Skeleton -->
      <ng-container *ngSwitchDefault>
        <div class="skeleton-box"></div>
      </ng-container>
    </div>
  `,
    styles: [`
    .skeleton-wrapper {
      animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
    }

    /* KPI Card Skeleton */
    .skeleton-kpi-card {
      display: flex;
      gap: 1rem;
      padding: 1rem;
      background: #f3f4f6;
      border-radius: 8px;
      border: 1px solid #e5e7eb;
    }

    .skeleton-kpi-icon {
      width: 48px;
      height: 48px;
      background: #e5e7eb;
      border-radius: 8px;
      flex-shrink: 0;
    }

    .skeleton-kpi-content {
      flex: 1;
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
    }

    .skeleton-kpi-label {
      width: 60%;
      height: 0.75rem;
      background: #e5e7eb;
      border-radius: 4px;
    }

    .skeleton-kpi-value {
      width: 40%;
      height: 1.5rem;
      background: #e5e7eb;
      border-radius: 4px;
    }

    /* Table Row Skeleton */
    .skeleton-table-row {
      display: flex;
      gap: 1rem;
      padding: 1rem;
      background: #f9fafb;
      border-bottom: 1px solid #e5e7eb;
    }

    .skeleton-cell {
      flex: 1;
      height: 1rem;
      background: #e5e7eb;
      border-radius: 4px;
    }

    .skeleton-cell:first-child {
      width: 20%;
    }

    .skeleton-cell:last-child {
      width: 15%;
    }

    /* Chart Skeleton */
    .skeleton-chart {
      display: flex;
      align-items: flex-end;
      gap: 0.5rem;
      height: 200px;
      padding: 1rem;
      background: #f9fafb;
      border-radius: 8px;
    }

    .skeleton-chart-bar {
      flex: 1;
      background: #e5e7eb;
      border-radius: 4px;
      min-height: 20px;
    }

    .skeleton-chart-bar:nth-child(1) { height: 60%; }
    .skeleton-chart-bar:nth-child(2) { height: 75%; }
    .skeleton-chart-bar:nth-child(3) { height: 45%; }
    .skeleton-chart-bar:nth-child(4) { height: 80%; }
    .skeleton-chart-bar:nth-child(5) { height: 55%; }
    .skeleton-chart-bar:nth-child(6) { height: 70%; }
    .skeleton-chart-bar:nth-child(7) { height: 50%; }
    .skeleton-chart-bar:nth-child(8) { height: 85%; }
    .skeleton-chart-bar:nth-child(9) { height: 65%; }
    .skeleton-chart-bar:nth-child(10) { height: 75%; }

    /* Generic Box Skeleton */
    .skeleton-box {
      width: 100%;
      height: 1rem;
      background: #e5e7eb;
      border-radius: 4px;
    }

    @keyframes pulse {
      0%, 100% {
        opacity: 1;
      }
      50% {
        opacity: 0.5;
      }
    }
  `]
})
export class SkeletonLoaderComponent {
    @Input() type: 'kpi' | 'table-row' | 'chart' | 'box' = 'box';
    @Input() count: number = 1;

    getRange(): number[] {
        return Array.from({ length: this.count }, (_, i) => i);
    }
}
