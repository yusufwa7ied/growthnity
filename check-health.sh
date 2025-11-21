#!/bin/bash
# Health check script for Growthnity production

echo "🔍 Checking Growthnity Production Health..."
echo "=========================================="

# Check if SSH key exists
if [ ! -f ~/.ssh/growthnity-key.pem ]; then
    echo "❌ SSH key not found at ~/.ssh/growthnity-key.pem"
    exit 1
fi

SSH_CMD="ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248"

echo ""
echo "1️⃣  Container Status:"
$SSH_CMD 'cd ~/growthnity && docker compose ps'

echo ""
echo "2️⃣  Disk Space:"
$SSH_CMD 'df -h / | tail -1'

echo ""
echo "3️⃣  Memory Usage:"
$SSH_CMD 'free -h | grep Mem'

echo ""
echo "4️⃣  SSL Certificate Expiry:"
$SSH_CMD 'sudo certbot certificates 2>/dev/null | grep "Expiry Date"'

echo ""
echo "5️⃣  Recent Backend Errors (last 20 lines):"
$SSH_CMD 'cd ~/growthnity && docker compose logs --tail=20 backend | grep -i "error\|exception\|traceback" || echo "✅ No recent errors found"'

echo ""
echo "6️⃣  Nginx Access Log (last 5 requests):"
$SSH_CMD 'cd ~/growthnity && docker compose logs --tail=5 frontend | grep -E "GET|POST" || echo "No recent requests"'

echo ""
echo "7️⃣  Database Connection:"
$SSH_CMD 'cd ~/growthnity && docker compose exec -T db psql -U growthnity_user -d growthnity_db -c "SELECT COUNT(*) as total_users FROM auth_user;" 2>/dev/null || echo "❌ Database connection failed"'

echo ""
echo "8️⃣  Website Response:"
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" https://growthnity-app.com)
if [ "$RESPONSE" = "200" ]; then
    echo "✅ Website responding: HTTP $RESPONSE"
else
    echo "❌ Website issue: HTTP $RESPONSE"
fi

echo ""
echo "=========================================="
echo "✅ Health check complete!"
