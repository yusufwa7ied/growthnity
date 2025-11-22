#!/bin/bash
# Manual Backend Deployment Script
# Run this in your AWS EC2 terminal

set -e

echo "=== Backend Code Deployment ==="
echo "Current directory: $(pwd)"
echo ""

# Step 1: Backup current files
echo "Step 1: Backing up current backend files..."
cd /home/ubuntu/growthnity/backend/api
cp views.py views.py.backup-$(date +%Y%m%d-%H%M%S) || true
cp urls.py urls.py.backup-$(date +%Y%m%d-%H%M%S) || true
echo "✓ Backup complete"
echo ""

# Step 2: Show what we're about to update
echo "Step 2: Files to be updated:"
echo "  - /home/ubuntu/growthnity/backend/api/views.py"
echo "  - /home/ubuntu/growthnity/backend/api/urls.py"
echo ""

# Step 3: Instructions for manual update
echo "Step 3: UPDATE REQUIRED"
echo "----------------------------------------"
echo "You need to copy the updated Python files to the server."
echo ""
echo "Option A - If you have git configured:"
echo "  cd /home/ubuntu/growthnity"
echo "  git pull origin main"
echo ""
echo "Option B - Manual file copy (copy content from local files):"
echo "  1. Open local file: /Users/yusuf/Desktop/perf/my_project/backend/api/views.py"
echo "  2. Edit on server: nano /home/ubuntu/growthnity/backend/api/views.py"
echo "  3. Copy the content (especially lines 626+ and 703+)"
echo "  4. Repeat for urls.py (lines 45-46)"
echo ""
echo "After updating files, continue below..."
echo ""
read -p "Press ENTER after you've updated the files to continue..."

# Step 4: Verify the new functions exist
echo ""
echo "Step 4: Verifying new functions in views.py..."
if grep -q "def dashboard_filter_options_view" /home/ubuntu/growthnity/backend/api/views.py; then
    echo "✓ Found: dashboard_filter_options_view"
else
    echo "✗ MISSING: dashboard_filter_options_view"
    echo "ERROR: Function not found in views.py"
    exit 1
fi

if grep -q "def dashboard_pie_chart_data_view" /home/ubuntu/growthnity/backend/api/views.py; then
    echo "✓ Found: dashboard_pie_chart_data_view"
else
    echo "✗ MISSING: dashboard_pie_chart_data_view"
    echo "ERROR: Function not found in views.py"
    exit 1
fi
echo ""

echo "Step 5: Verifying URL patterns..."
if grep -q "dashboard/filter-options/" /home/ubuntu/growthnity/backend/api/urls.py; then
    echo "✓ Found: dashboard/filter-options/ URL"
else
    echo "✗ MISSING: dashboard/filter-options/ URL"
    echo "ERROR: URL pattern not found in urls.py"
    exit 1
fi

if grep -q "dashboard/pie-chart-data/" /home/ubuntu/growthnity/backend/api/urls.py; then
    echo "✓ Found: dashboard/pie-chart-data/ URL"
else
    echo "✗ MISSING: dashboard/pie-chart-data/ URL"
    echo "ERROR: URL pattern not found in urls.py"
    exit 1
fi
echo ""

# Step 6: Restart backend
echo "Step 6: Restarting backend container..."
cd /home/ubuntu/growthnity
docker compose restart backend
echo "✓ Backend restarted"
echo ""

# Step 7: Wait for backend to start
echo "Step 7: Waiting for backend to start..."
sleep 5
echo ""

# Step 8: Test endpoints
echo "Step 8: Testing new endpoints..."
echo "Testing: http://localhost:8000/api/dashboard/filter-options/"
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/api/dashboard/filter-options/ || echo "000")
if [ "$RESPONSE" = "200" ] || [ "$RESPONSE" = "401" ] || [ "$RESPONSE" = "403" ]; then
    echo "✓ Endpoint responding (HTTP $RESPONSE)"
else
    echo "✗ Endpoint not working (HTTP $RESPONSE)"
    echo "Check logs: docker compose logs backend | tail -30"
fi
echo ""

echo "Testing: http://localhost:8000/api/dashboard/pie-chart-data/"
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/api/dashboard/pie-chart-data/ || echo "000")
if [ "$RESPONSE" = "200" ] || [ "$RESPONSE" = "401" ] || [ "$RESPONSE" = "403" ]; then
    echo "✓ Endpoint responding (HTTP $RESPONSE)"
else
    echo "✗ Endpoint not working (HTTP $RESPONSE)"
    echo "Check logs: docker compose logs backend | tail -30"
fi
echo ""

echo "=== Deployment Complete ==="
echo "Test in browser: https://growthnity.com/dashboard"
echo ""
