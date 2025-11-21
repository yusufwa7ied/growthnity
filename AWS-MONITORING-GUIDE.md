# AWS CloudWatch Setup for Growthnity

## Step 1: Install CloudWatch Agent on EC2

```bash
# SSH to your EC2
ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248

# Install CloudWatch agent
wget https://s3.amazonaws.com/amazoncloudwatch-agent/ubuntu/amd64/latest/amazon-cloudwatch-agent.deb
sudo dpkg -i -E ./amazon-cloudwatch-agent.deb

# Install Docker log driver for CloudWatch
sudo apt-get update
sudo apt-get install -y awscli
```

## Step 2: Configure Docker to Send Logs to CloudWatch

Add this to your docker-compose.yml for each service:

```yaml
services:
  backend:
    logging:
      driver: "awslogs"
      options:
        awslogs-region: "us-east-1"  # Your AWS region
        awslogs-group: "growthnity/backend"
        awslogs-create-group: "true"
  
  frontend:
    logging:
      driver: "awslogs"
      options:
        awslogs-region: "us-east-1"
        awslogs-group: "growthnity/frontend"
        awslogs-create-group: "true"
```

## Step 3: Access Logs in AWS Console

1. Go to: https://console.aws.amazon.com/cloudwatch/
2. Click **Logs** → **Log groups** in left sidebar
3. You'll see:
   - `/growthnity/backend` - Django/Python logs
   - `/growthnity/frontend` - Nginx/Access logs
   - `/growthnity/db` - PostgreSQL logs

## Step 4: Create CloudWatch Alarms

### CPU Usage Alert
1. CloudWatch → Alarms → Create Alarm
2. Select Metric → EC2 → Per-Instance Metrics → CPUUtilization
3. Set threshold: > 80% for 5 minutes
4. Add email notification

### Disk Space Alert
1. CloudWatch → Alarms → Create Alarm
2. Select Metric → EC2 → Disk Space
3. Set threshold: > 80% used
4. Add email notification

### Application Error Alert
1. CloudWatch → Logs → Log groups
2. Select `/growthnity/backend`
3. Create metric filter: Pattern = "ERROR"
4. Create alarm: Errors > 5 in 5 minutes

---

## Alternative: Simple AWS Monitoring (No Setup Required)

### EC2 Dashboard Monitoring:
1. Go to: https://console.aws.amazon.com/ec2/
2. Click **Instances**
3. Select your instance (growthnity)
4. Click **Monitoring** tab

You can see:
- ✅ CPU utilization
- ✅ Network in/out
- ✅ Disk read/write
- ✅ Status checks

### CloudWatch Default Metrics:
Go to: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#metricsV2:

Filter by your instance ID to see:
- CPU usage graphs
- Network traffic
- Disk operations

---

## Quick Setup Option: Use Logs Insights

1. Go to CloudWatch → Logs Insights
2. Query your logs with SQL-like syntax:

```sql
# Find all errors in last hour
fields @timestamp, @message
| filter @message like /ERROR/
| sort @timestamp desc
| limit 50
```

```sql
# Count requests by status code
fields @timestamp, status
| filter @message like /HTTP/
| stats count() by status
```

---

## Cost-Effective Monitoring Setup

**Free Tier Includes:**
- 5GB of log data ingestion
- 5GB of log storage
- 1 million API requests
- 10 custom metrics
- 10 alarms

**Recommendation:** Start with CloudWatch default metrics (free), add logs only if needed.
