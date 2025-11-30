#!/bin/bash
# Deployment script with database migration
# Run this directly on EC2 server via AWS Instance Connect

set -e

echo "======================================"
echo "DEPLOYING WITH DATABASE MIGRATION"
echo "======================================"
echo ""

# Step 1: Pull latest code
echo "Step 1: Pulling latest code from GitHub..."
cd /home/ubuntu/growthnity
git pull origin main
echo "✓ Code updated"
echo ""

# Step 2: Stop containers
echo "Step 2: Stopping containers..."
docker compose down
echo "✓ Containers stopped"
echo ""

# Step 3: Rebuild containers (to pick up code changes)
echo "Step 3: Rebuilding containers..."
docker compose build backend frontend
echo "✓ Containers rebuilt"
echo ""

# Step 4: Start containers
echo "Step 4: Starting containers..."
docker compose up -d
echo "✓ Containers started"
echo ""

# Step 5: Wait for backend to initialize
echo "Step 5: Waiting for backend to initialize..."
sleep 10
echo ""

# Step 6: Run migrations
echo "Step 6: Running database migrations..."
docker compose exec -T backend python manage.py migrate
echo "✓ Migrations applied"
echo ""

# Step 7: Verify backend is running
echo "Step 7: Verifying backend status..."
docker compose ps backend
echo ""

# Step 8: Test API endpoint
echo "Step 8: Testing API health..."
curl -s -o /dev/null -w "HTTP Status: %{http_code}\n" http://localhost:8000/api/dashboard/filter-options/ || echo "Backend may need authentication"
echo ""

echo "======================================"
echo "DEPLOYMENT COMPLETE!"
echo "======================================"
echo ""
echo "Next steps:"
echo "1. Check logs: docker compose logs backend | tail -50"
echo "2. Test in browser: https://growthnity.com"
echo "3. Verify Daily Spend page works without coupon dropdown"
echo ""
