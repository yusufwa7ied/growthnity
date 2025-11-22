#!/bin/bash

# Frontend Deployment Script for Growthnity
# This script builds Angular app and deploys to AWS server

set -e  # Exit on error

SERVER="ubuntu@3.87.184.207"
REMOTE_PATH="/home/ubuntu/growthnity/backend/staticfiles/"
LOCAL_BUILD_PATH="/Users/yusuf/Desktop/perf/my_project/angular-app/dist/angular-app/browser/"

echo "=========================================="
echo "Growthnity Frontend Deployment"
echo "=========================================="
echo ""

# Step 1: Build Angular app
echo "1. Building Angular application..."
cd /Users/yusuf/Desktop/perf/my_project/angular-app
npm run build --configuration=production

if [ $? -ne 0 ]; then
    echo "❌ Build failed!"
    exit 1
fi

echo "✅ Build successful"
echo ""

# Step 2: Test SSH connection
echo "2. Testing SSH connection..."
if ! ssh -o ConnectTimeout=10 -o BatchMode=yes $SERVER "echo 'SSH OK'" 2>/dev/null; then
    echo "❌ SSH connection failed!"
    echo ""
    echo "To fix this, run in AWS Session Manager:"
    echo "  sudo fail2ban-client set sshd unbanip \$(curl -s https://api.ipify.org)"
    echo ""
    echo "Or open: https://console.aws.amazon.com/systems-manager/session-manager"
    exit 1
fi

echo "✅ SSH connection working"
echo ""

# Step 3: Deploy files
echo "3. Deploying files to server..."
rsync -avz --delete \
    --exclude='*.map' \
    --progress \
    "$LOCAL_BUILD_PATH" \
    "$SERVER:$REMOTE_PATH"

if [ $? -ne 0 ]; then
    echo "❌ Deployment failed!"
    exit 1
fi

echo "✅ Files deployed successfully"
echo ""

# Step 4: Restart frontend container
echo "4. Restarting frontend container..."
ssh $SERVER "cd /home/ubuntu/growthnity && docker-compose restart frontend"

if [ $? -ne 0 ]; then
    echo "⚠️  Warning: Container restart failed, but files are deployed"
    echo "You may need to restart manually"
    exit 1
fi

echo "✅ Frontend container restarted"
echo ""

# Step 5: Verify deployment
echo "5. Verifying deployment..."
sleep 3
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" https://growthnity.com)

if [ "$HTTP_STATUS" = "200" ]; then
    echo "✅ Website is accessible (HTTP $HTTP_STATUS)"
else
    echo "⚠️  Warning: Website returned HTTP $HTTP_STATUS"
fi

echo ""
echo "=========================================="
echo "✅ Deployment Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Clear browser cache (Cmd+Shift+R on Mac)"
echo "2. Check browser console for errors"
echo "3. Test the new functionality"
echo ""
