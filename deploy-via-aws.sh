#!/bin/bash

# This script will help you deploy via AWS without SSH

echo "================================================"
echo "AWS Deployment Helper"
echo "================================================"
echo ""
echo "STEP 1: Copy this command and run it in AWS EC2 Instance Connect terminal:"
echo ""
echo "-------- COPY EVERYTHING BELOW THIS LINE --------"
cat << 'AWSCOMMANDS'
cd /home/ubuntu/growthnity/backend/staticfiles/

# Backup old files
mkdir -p ~/backup_$(date +%Y%m%d_%H%M%S)
cp chunk-*.js ~/backup_$(date +%Y%m%d_%H%M%S)/ 2>/dev/null || true

# Download all new chunks from your local build
# You'll need to paste each file content

echo "Ready to receive files. Paste the deployment bundle now..."
AWSCOMMANDS
echo "-------- END OF COPY --------"
echo ""
echo "STEP 2: After running above in AWS, come back here and press ENTER"
read -p ""

# Create deployment bundle
echo "Creating deployment bundle..."
cd /Users/yusuf/Desktop/perf/my_project/angular-app/dist/angular-app/browser

# Create a tar file with all chunks
tar -czf /tmp/frontend-chunks.tar.gz chunk-*.js *.js index.html 2>/dev/null

echo ""
echo "Bundle created: /tmp/frontend-chunks.tar.gz"
echo ""
echo "STEP 3: Now run this command in AWS terminal to download and extract:"
echo ""
echo "-------- COPY EVERYTHING BELOW THIS LINE --------"
echo "cd /tmp"
echo "# You need to manually upload frontend-chunks.tar.gz to the server"
echo "# Or use the manual file upload method below"
echo "-------- END OF COPY --------"
