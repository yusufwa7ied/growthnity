import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
    name: 'trimDecimals',
    standalone: true
})
export class TrimDecimalsPipe implements PipeTransform {
    transform(value: number | null, decimalPlaces: number = 2): string {
        if (value === null || value === undefined) {
            return '';
        }

        // Round to specified decimal places
        const rounded = Math.round(value * Math.pow(10, decimalPlaces)) / Math.pow(10, decimalPlaces);

        // Convert to string and remove trailing zeros
        return rounded.toLocaleString('en-US', {
            minimumFractionDigits: 0,
            maximumFractionDigits: decimalPlaces
        });
    }
}
