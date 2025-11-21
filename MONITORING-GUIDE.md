# Growthnity Monitoring & Maintenance Guide

## 🎯 What You Have Now

### ✅ Monitoring Setup (Complete)
1. **CloudWatch Metrics** - Server health monitoring
   - CPU utilization
   - Network traffic
   - Status checks
   - URL: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#metricsV2:

2. **CloudWatch Logs** - Application logs in real-time
   - `/growthnity/backend` - Django/Python logs
   - `/growthnity/frontend` - Nginx/access logs
   - `/growthnity/db` - PostgreSQL logs
   - URL: https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups

3. **Local Log Scripts** - Quick debugging from your Mac
   - `./logs.sh` - Interactive menu
   - `./watch-logs.sh` - Live colored logs
   - `./errors-only.sh` - Show only errors
   - `./check-health.sh` - Full health check

### ✅ Production Setup (Complete)
- ✅ Domain: growthnity-app.com
- ✅ SSL Certificate (valid until Feb 18, 2026)
- ✅ Auto-renewal configured
- ✅ PostgreSQL database with all data migrated
- ✅ Docker containers running
- ✅ Branding updated (Growthnity App + logo)

---

## 📊 Daily Monitoring Checklist

### Quick Health Check (2 minutes)
```bash
cd /Users/yusuf/Desktop/perf/my_project
./check-health.sh
```

**Look for:**
- ✅ All 3 containers running (db, backend, frontend)
- ✅ Disk space < 80%
- ✅ Memory < 1.5GB
- ✅ No recent errors
- ✅ Website responding HTTP 200

### Weekly AWS Console Check (5 minutes)

1. **Check CPU Usage:**
   - Go to CloudWatch → Metrics → EC2
   - Should be < 70% average
   - If consistently > 80%, upgrade instance

2. **Review Error Logs:**
   - CloudWatch → Logs → `/growthnity/backend`
   - Look for ERROR or exception messages
   - If errors found, investigate with `./logs.sh`

3. **Check SSL Certificate:**
   - Should auto-renew 30 days before expiry
   - Current expiry: Feb 18, 2026

---

## 🚨 Alerts to Set Up (Recommended)

### 1. High CPU Alert
**When:** CPU > 80% for 5 minutes
**Action:** Consider upgrading from t3.small to t3.medium

**Setup:**
- CloudWatch → Alarms → Create alarm
- Metric: EC2 → CPUUtilization
- Threshold: Greater than 80
- Email: [your-email]

### 2. Application Error Alert
**When:** More than 5 errors in 5 minutes
**Action:** Check logs and investigate

**Setup:**
- CloudWatch → Logs → `/growthnity/backend`
- Create metric filter: Pattern = "ERROR"
- Create alarm: Errors > 5

### 3. Disk Space Alert
**When:** Disk usage > 80%
**Action:** Clean up logs or upgrade disk

---

## 🛠️ Common Maintenance Tasks

### Update Application Code
```bash
cd /Users/yusuf/Desktop/perf/my_project

# Make your changes locally
# Test locally first

# Deploy to production
git add -A
git commit -m "Your changes"
git push origin main

# Deploy to AWS
ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248 'cd ~/growthnity && git pull && docker compose up -d --build'
```

### View Live Logs
```bash
# Interactive menu
./logs.sh

# Or SSH and watch live
ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248
cd ~/growthnity
docker compose logs -f
```

### Restart Services
```bash
ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248

# Restart all services
cd ~/growthnity && docker compose restart

# Restart specific service
cd ~/growthnity && docker compose restart backend
cd ~/growthnity && docker compose restart frontend
```

### Database Backup
```bash
ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248

# Backup database
cd ~/growthnity
docker compose exec -T db pg_dump -U growthnity_user growthnity_db > backup_$(date +%Y%m%d).sql

# Download backup to Mac
exit
scp -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248:~/growthnity/backup_*.sql ~/Desktop/
```

### Clean Up Docker
```bash
ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248

# Remove unused containers/images
docker system prune -a

# View disk usage
docker system df
```

---

## 🔧 Troubleshooting

### Site is Down
1. Check containers: `ssh` → `cd ~/growthnity && docker compose ps`
2. Check logs: `./logs.sh` → Option 5 (errors only)
3. Restart: `docker compose restart`

### Slow Performance
1. Check CPU: CloudWatch → Metrics
2. Check memory: `./check-health.sh`
3. Consider upgrading instance if consistently high

### SSL Certificate Issues
```bash
ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248

# Check certificate status
sudo certbot certificates

# Manual renewal (if needed)
sudo certbot renew
docker compose restart frontend
```

### Database Issues
```bash
ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248

# Check database logs
cd ~/growthnity && docker compose logs db

# Access database
docker compose exec db psql -U growthnity_user -d growthnity_db
```

---

## 💰 AWS Cost Optimization

**Current Monthly Estimate: ~$15-25**
- EC2 t3.small: ~$15/month
- Data transfer: ~$2-5/month
- CloudWatch: Free tier (5GB logs, 10 metrics, 10 alarms)

**Free Tier Includes:**
- 5GB CloudWatch log ingestion
- 5GB log storage
- 10 custom metrics
- 10 alarms

**Cost Reduction Tips:**
1. Stop instance when not in use (dev/testing)
2. Use Reserved Instance for 1-year commitment (40% savings)
3. Clean up old logs regularly
4. Monitor data transfer costs

---

## 📞 Quick Commands Reference

```bash
# Health check
./check-health.sh

# View logs interactively
./logs.sh

# Live logs with colors
./watch-logs.sh

# Check errors only
./errors-only.sh

# SSH to server
ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248

# Deploy changes
git push && ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248 'cd ~/growthnity && git pull && docker compose up -d --build'
```

---

## 🎓 Learning Resources

**AWS CloudWatch:**
- https://docs.aws.amazon.com/cloudwatch/

**Docker Compose:**
- https://docs.docker.com/compose/

**Django Production:**
- https://docs.djangoproject.com/en/stable/howto/deployment/

**EC2 Best Practices:**
- https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/best-practices.html
