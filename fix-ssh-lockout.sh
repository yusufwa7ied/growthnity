# Fail2ban IP Whitelist Configuration
# Run these commands on your AWS server to prevent SSH lockouts

# 1. Connect via AWS Session Manager (browser-based, no SSH needed):
#    https://console.aws.amazon.com/systems-manager/session-manager

# 2. Add your IP to fail2ban whitelist:

sudo bash -c 'cat >> /etc/fail2ban/jail.local << EOF
[DEFAULT]
# Whitelist your home/office IP - change this to your actual IP
ignoreip = 127.0.0.1/8 ::1 210.79.155.104

# Reduce ban time from forever to 1 hour
bantime = 3600

# Allow more retry attempts before ban
maxretry = 10
EOF'

# 3. Restart fail2ban
sudo systemctl restart fail2ban

# 4. Verify configuration
sudo fail2ban-client status sshd

# ========================================
# To unban your IP if locked out:
# ========================================

# Get your current IP:
curl -s https://api.ipify.org

# Unban it:
sudo fail2ban-client set sshd unbanip YOUR_IP_HERE

# Check all banned IPs:
sudo fail2ban-client get sshd banned

# Clear all bans:
sudo fail2ban-client unban --all
