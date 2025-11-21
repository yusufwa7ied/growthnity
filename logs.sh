#!/bin/bash
# Pretty log viewer for Growthnity

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

clear
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}           🚀 GROWTHNITY LOGS VIEWER 🚀${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "${GREEN}Choose what you want to see:${NC}"
echo ""
echo -e "  ${YELLOW}1${NC} - 📊 Live logs (all services)"
echo -e "  ${YELLOW}2${NC} - 🐍 Backend logs only"
echo -e "  ${YELLOW}3${NC} - 🌐 Frontend/Nginx logs only"
echo -e "  ${YELLOW}4${NC} - 🗄️  Database logs only"
echo -e "  ${YELLOW}5${NC} - ⚠️  Errors only (last 50 lines)"
echo -e "  ${YELLOW}6${NC} - 🔍 Search logs (custom keyword)"
echo -e "  ${YELLOW}7${NC} - 📈 Recent API requests (last 20)"
echo -e "  ${YELLOW}8${NC} - 💾 Save logs to file"
echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
read -p "Enter choice (1-8): " choice
echo ""

SSH_CMD="ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248"

case $choice in
    1)
        echo -e "${GREEN}📡 Streaming live logs from all services...${NC}"
        echo -e "${BLUE}Press Ctrl+C to stop${NC}"
        echo ""
        $SSH_CMD 'cd ~/growthnity && docker compose logs -f --tail=50' | \
        while IFS= read -r line; do
            if [[ $line == *"ERROR"* ]] || [[ $line == *"Error"* ]]; then
                echo -e "${RED}$line${NC}"
            elif [[ $line == *"WARNING"* ]] || [[ $line == *"Warning"* ]]; then
                echo -e "${YELLOW}$line${NC}"
            elif [[ $line == *"backend"* ]]; then
                echo -e "${MAGENTA}$line${NC}"
            elif [[ $line == *"frontend"* ]]; then
                echo -e "${CYAN}$line${NC}"
            elif [[ $line == *"db"* ]]; then
                echo -e "${BLUE}$line${NC}"
            else
                echo "$line"
            fi
        done
        ;;
    2)
        echo -e "${MAGENTA}🐍 Backend logs (last 50 lines, following new)...${NC}"
        echo -e "${BLUE}Press Ctrl+C to stop${NC}"
        echo ""
        $SSH_CMD 'cd ~/growthnity && docker compose logs -f --tail=50 backend' | \
        while IFS= read -r line; do
            if [[ $line == *"ERROR"* ]] || [[ $line == *"error"* ]]; then
                echo -e "${RED}$line${NC}"
            else
                echo -e "${MAGENTA}$line${NC}"
            fi
        done
        ;;
    3)
        echo -e "${CYAN}🌐 Frontend/Nginx logs (last 50 lines, following new)...${NC}"
        echo -e "${BLUE}Press Ctrl+C to stop${NC}"
        echo ""
        $SSH_CMD 'cd ~/growthnity && docker compose logs -f --tail=50 frontend' | \
        while IFS= read -r line; do
            if [[ $line == *"GET"* ]]; then
                echo -e "${GREEN}$line${NC}"
            elif [[ $line == *"POST"* ]]; then
                echo -e "${YELLOW}$line${NC}"
            elif [[ $line == *"error"* ]] || [[ $line == *"404"* ]] || [[ $line == *"500"* ]]; then
                echo -e "${RED}$line${NC}"
            else
                echo -e "${CYAN}$line${NC}"
            fi
        done
        ;;
    4)
        echo -e "${BLUE}🗄️  Database logs (last 50 lines, following new)...${NC}"
        echo -e "${BLUE}Press Ctrl+C to stop${NC}"
        echo ""
        $SSH_CMD 'cd ~/growthnity && docker compose logs -f --tail=50 db'
        ;;
    5)
        echo -e "${RED}⚠️  Recent errors from all services...${NC}"
        echo ""
        $SSH_CMD 'cd ~/growthnity && docker compose logs --tail=50 | grep -i "error\|exception\|traceback\|fail"' | \
        while IFS= read -r line; do
            echo -e "${RED}$line${NC}"
        done
        echo ""
        echo -e "${GREEN}✅ Done! (If empty, no errors found)${NC}"
        ;;
    6)
        read -p "Enter keyword to search: " keyword
        echo ""
        echo -e "${GREEN}🔍 Searching for '$keyword' in logs...${NC}"
        echo ""
        $SSH_CMD "cd ~/growthnity && docker compose logs --tail=100 | grep -i '$keyword'" | \
        while IFS= read -r line; do
            echo -e "${YELLOW}$line${NC}"
        done
        echo ""
        echo -e "${GREEN}✅ Search complete!${NC}"
        ;;
    7)
        echo -e "${GREEN}📈 Recent API requests (last 20)...${NC}"
        echo ""
        $SSH_CMD 'cd ~/growthnity && docker compose logs --tail=100 frontend | grep -E "GET|POST"' | tail -20 | \
        while IFS= read -r line; do
            if [[ $line == *"200"* ]]; then
                echo -e "${GREEN}$line${NC}"
            elif [[ $line == *"404"* ]] || [[ $line == *"500"* ]]; then
                echo -e "${RED}$line${NC}"
            else
                echo -e "${YELLOW}$line${NC}"
            fi
        done
        echo ""
        echo -e "${GREEN}✅ Done!${NC}"
        ;;
    8)
        TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
        FILENAME="growthnity_logs_${TIMESTAMP}.txt"
        echo -e "${GREEN}💾 Saving logs to $FILENAME...${NC}"
        echo ""
        $SSH_CMD 'cd ~/growthnity && docker compose logs --tail=500' > "$FILENAME"
        echo -e "${GREEN}✅ Saved $(wc -l < $FILENAME) lines to $FILENAME${NC}"
        ;;
    *)
        echo -e "${RED}❌ Invalid choice!${NC}"
        ;;
esac

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
