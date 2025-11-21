#!/bin/bash
# Quick live log viewer with colors

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'

clear
echo -e "${CYAN}🚀 Growthnity Live Logs - Press Ctrl+C to stop${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248 'cd ~/growthnity && docker compose logs -f --tail=30' | \
while IFS= read -r line; do
    # Color based on content
    if [[ $line == *"ERROR"* ]] || [[ $line == *"error"* ]] || [[ $line == *"500"* ]]; then
        echo -e "${RED}$line${NC}"
    elif [[ $line == *"WARNING"* ]] || [[ $line == *"warning"* ]]; then
        echo -e "${YELLOW}$line${NC}"
    elif [[ $line == *"backend"* ]]; then
        echo -e "${MAGENTA}$line${NC}"
    elif [[ $line == *"frontend"* ]]; then
        echo -e "${CYAN}$line${NC}"
    elif [[ $line == *"db-1"* ]]; then
        echo -e "${BLUE}$line${NC}"
    elif [[ $line == *"200"* ]]; then
        echo -e "${GREEN}$line${NC}"
    else
        echo "$line"
    fi
done
