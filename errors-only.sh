#!/bin/bash
# Show only errors and warnings

RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

clear
echo -e "${CYAN}⚠️  Growthnity Errors & Warnings${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

echo -e "${YELLOW}🔍 Checking last 100 log entries...${NC}"
echo ""

RESULT=$(ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248 'cd ~/growthnity && docker compose logs --tail=100 | grep -iE "error|exception|traceback|fail|warning"')

if [ -z "$RESULT" ]; then
    echo -e "${GREEN}✅ No errors or warnings found!${NC}"
else
    echo "$RESULT" | while IFS= read -r line; do
        if [[ $line == *"ERROR"* ]] || [[ $line == *"error"* ]]; then
            echo -e "${RED}$line${NC}"
        else
            echo -e "${YELLOW}$line${NC}"
        fi
    done
fi

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
