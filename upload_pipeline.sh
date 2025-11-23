#!/bin/bash
#
# Quick Pipeline Upload Script
# Usage: ./upload_pipeline.sh nn /path/to/noon-namshi.csv
#        ./upload_pipeline.sh styli /path/to/styli_raw_data.csv
#

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/upload_to_s3.py"
API_URL="${API_URL:-https://your-domain.com/api}"
JWT_TOKEN="${JWT_TOKEN}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if Python script exists
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo -e "${RED}❌ Error: $PYTHON_SCRIPT not found${NC}"
    exit 1
fi

# Show usage
show_usage() {
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║    Quick Pipeline Upload                                   ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "Usage: $0 <pipeline> <csv-file>"
    echo ""
    echo "  pipeline: nn or styli"
    echo "  csv-file: path to CSV file"
    echo ""
    echo "Examples:"
    echo "  $0 nn /Users/yusuf/noon-namshi.csv"
    echo "  $0 styli /Users/yusuf/styli_raw_data.csv"
    echo ""
    echo "Environment Variables:"
    echo "  JWT_TOKEN    (required) - Your JWT authentication token"
    echo "  API_URL      (optional) - API endpoint (default: $API_URL)"
    echo ""
    exit 1
}

# Check arguments
if [ $# -lt 2 ]; then
    show_usage
fi

PIPELINE=$1
CSV_FILE=$2

# Validate pipeline argument
if [ "$PIPELINE" != "nn" ] && [ "$PIPELINE" != "styli" ]; then
    echo -e "${RED}❌ Invalid pipeline: $PIPELINE (must be 'nn' or 'styli')${NC}"
    exit 1
fi

# Check if file exists
if [ ! -f "$CSV_FILE" ]; then
    echo -e "${RED}❌ File not found: $CSV_FILE${NC}"
    exit 1
fi

# Check JWT token
if [ -z "$JWT_TOKEN" ]; then
    echo -e "${RED}❌ Error: JWT_TOKEN environment variable not set${NC}"
    echo -e "${YELLOW}Get token: curl -X POST $API_URL/login/ -d '{\"username\":\"admin\",\"password\":\"your-password\"}'${NC}"
    exit 1
fi

# Get current date for date range
START_DATE=$(date +%Y-%m-01)
END_DATE=$(date +%Y-%m-%d)

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║         UPLOADING TO S3 & TRIGGERING PIPELINE              ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}Pipeline:${NC}  $([ "$PIPELINE" = "nn" ] && echo "Noon-Namshi" || echo "Styli")"
echo -e "${GREEN}File:${NC}      $CSV_FILE"
echo -e "${GREEN}API URL:${NC}   $API_URL"
echo -e "${GREEN}Date Range:${NC} $START_DATE → $END_DATE"
echo ""

# Run Python script
python3 "$PYTHON_SCRIPT" \
    --file "$CSV_FILE" \
    --pipeline "$PIPELINE" \
    --start "$START_DATE" \
    --end "$END_DATE" \
    --token "$JWT_TOKEN" \
    --url "$API_URL"

RESULT=$?

if [ $RESULT -eq 0 ]; then
    echo -e "${GREEN}✅ Success! Check your dashboard for updated analytics.${NC}"
    exit 0
else
    echo -e "${RED}❌ Upload failed. Check errors above.${NC}"
    exit 1
fi
