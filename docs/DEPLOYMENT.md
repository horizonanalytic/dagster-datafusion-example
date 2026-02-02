# Deployment Guide

Guide for deploying the Dagster + DataFusion example to production environments.

## Table of Contents

- [AWS ECS Deployment](#aws-ecs-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Environment Configuration](#environment-configuration)
- [Production Checklist](#production-checklist)

---

## AWS ECS Deployment

### Prerequisites

- AWS account with ECS, S3, RDS access
- AWS CLI configured
- Docker installed locally
- ECR repository created

### 1. Build and Push Docker Image

```bash
# Authenticate with ECR
aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-2.amazonaws.com

# Build image
docker build -t dagster-example:latest .

# Tag for ECR
docker tag dagster-example:latest <account-id>.dkr.ecr.us-east-2.amazonaws.com/dagster-example:latest

# Push to ECR
docker push <account-id>.dkr.ecr.us-east-2.amazonaws.com/dagster-example:latest
```

### 2. Create RDS PostgreSQL Instance

```bash
# Create RDS instance for Dagster metadata
aws rds create-db-instance \
  --db-instance-identifier dagster-metadata-prod \
  --db-instance-class db.t3.micro \
  --engine postgres \
  --master-username dagster \
  --master-user-password <secure-password> \
  --allocated-storage 20 \
  --vpc-security-group-ids sg-xxxxxxxx \
  --db-subnet-group-name dagster-subnet-group
```

### 3. Create ECS Task Definition

```json
{
  "family": "dagster-webserver",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "containerDefinitions": [
    {
      "name": "dagster-webserver",
      "image": "<account-id>.dkr.ecr.us-east-2.amazonaws.com/dagster-example:latest",
      "essential": true,
      "portMappings": [
        {
          "containerPort": 3000,
          "protocol": "tcp"
        }
      ],
      "command": [
        "dagster-webserver",
        "-h", "0.0.0.0",
        "-p", "3000",
        "-w", "workspace.yaml"
      ],
      "environment": [
        {
          "name": "ENVIRONMENT",
          "value": "prod"
        },
        {
          "name": "AWS_REGION",
          "value": "us-east-2"
        },
        {
          "name": "PROD_S3_BUCKET",
          "value": "your-prod-bucket"
        },
        {
          "name": "PROD_S3_PREFIX",
          "value": "dagster_prod"
        }
      ],
      "secrets": [
        {
          "name": "DAGSTER_POSTGRES_USER",
          "valueFrom": "arn:aws:secretsmanager:region:account:secret:dagster/db-user"
        },
        {
          "name": "DAGSTER_POSTGRES_PASSWORD",
          "valueFrom": "arn:aws:secretsmanager:region:account:secret:dagster/db-password"
        },
        {
          "name": "DAGSTER_POSTGRES_HOST",
          "valueFrom": "arn:aws:secretsmanager:region:account:secret:dagster/db-host"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/dagster-webserver",
          "awslogs-region": "us-east-2",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ],
  "taskRoleArn": "arn:aws:iam::account-id:role/dagster-task-role",
  "executionRoleArn": "arn:aws:iam::account-id:role/ecsTaskExecutionRole"
}
```

### 4. Create ECS Service

```bash
aws ecs create-service \
  --cluster dagster-cluster \
  --service-name dagster-webserver \
  --task-definition dagster-webserver \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-xxx],securityGroups=[sg-xxx],assignPublicIp=ENABLED}" \
  --load-balancers "targetGroupArn=arn:aws:elasticloadbalancing:...,containerName=dagster-webserver,containerPort=3000"
```

### 5. Create Daemon Service

Similar to webserver, but with command:
```json
"command": ["dagster-daemon", "run"]
```

---

## Kubernetes Deployment

### 1. Create Namespace

```bash
kubectl create namespace dagster
```

### 2. Deploy PostgreSQL (or use managed service)

```yaml
# postgres-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: dagster
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:15-alpine
        env:
        - name: POSTGRES_USER
          value: dagster
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: dagster-secrets
              key: postgres-password
        - name: POSTGRES_DB
          value: dagster
        ports:
        - containerPort: 5432
        volumeMounts:
        - name: postgres-storage
          mountPath: /var/lib/postgresql/data
      volumes:
      - name: postgres-storage
        persistentVolumeClaim:
          claimName: postgres-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: dagster
spec:
  selector:
    app: postgres
  ports:
  - port: 5432
    targetPort: 5432
```

### 3. Deploy Dagster Webserver

```yaml
# dagster-webserver-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dagster-webserver
  namespace: dagster
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dagster-webserver
  template:
    metadata:
      labels:
        app: dagster-webserver
    spec:
      containers:
      - name: dagster-webserver
        image: <account-id>.dkr.ecr.us-east-2.amazonaws.com/dagster-example:latest
        command: ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
        env:
        - name: ENVIRONMENT
          value: prod
        - name: AWS_REGION
          value: us-east-2
        - name: PROD_S3_BUCKET
          value: your-prod-bucket
        - name: DAGSTER_POSTGRES_HOST
          value: postgres
        - name: DAGSTER_POSTGRES_USER
          valueFrom:
            secretKeyRef:
              name: dagster-secrets
              key: postgres-user
        - name: DAGSTER_POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: dagster-secrets
              key: postgres-password
        ports:
        - containerPort: 3000
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
---
apiVersion: v1
kind: Service
metadata:
  name: dagster-webserver
  namespace: dagster
spec:
  type: LoadBalancer
  selector:
    app: dagster-webserver
  ports:
  - port: 80
    targetPort: 3000
```

### 4. Deploy Dagster Daemon

Similar to webserver, but with:
```yaml
command: ["dagster-daemon", "run"]
```

---

## Environment Configuration

### Production Environment Variables

```bash
# Environment
ENVIRONMENT=prod

# S3 Configuration (use real S3, not LocalStack)
PROD_S3_BUCKET=your-prod-bucket
PROD_S3_PREFIX=dagster_prod
AWS_REGION=us-east-2

# Database Configuration
DAGSTER_POSTGRES_HOST=prod-db.xxxxx.us-east-2.rds.amazonaws.com
DAGSTER_POSTGRES_PORT=5432
DAGSTER_POSTGRES_USER=dagster
DAGSTER_POSTGRES_PASSWORD=<from-secrets-manager>
DAGSTER_POSTGRES_DB=dagster

# IAM Role (preferred over static credentials)
# AWS_ACCESS_KEY_ID=<not-needed-with-iam-role>
# AWS_SECRET_ACCESS_KEY=<not-needed-with-iam-role>
```

### IAM Role Policies

**Task Role** (for Dagster containers):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-prod-bucket",
        "arn:aws:s3:::your-prod-bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:region:account:secret:dagster/*"
    }
  ]
}
```

---

## Production Checklist

### Security

- [ ] Use IAM roles instead of static credentials
- [ ] Store secrets in AWS Secrets Manager or Kubernetes Secrets
- [ ] Enable S3 encryption at rest (AES-256 or KMS)
- [ ] Enable S3 versioning for audit trail
- [ ] Restrict S3 bucket access (no public access)
- [ ] Use VPC for network isolation
- [ ] Enable CloudWatch logging
- [ ] Use HTTPS for Dagster UI (ALB with ACM certificate)

### High Availability

- [ ] Deploy webserver with multiple replicas (2+)
- [ ] Use RDS Multi-AZ for database
- [ ] Use Application Load Balancer with health checks
- [ ] Configure auto-scaling for ECS tasks
- [ ] Set up CloudWatch alarms for critical metrics

### Monitoring

- [ ] CloudWatch metrics for ECS (CPU, memory)
- [ ] CloudWatch logs for application logs
- [ ] RDS monitoring (connections, CPU, storage)
- [ ] S3 metrics (request count, 4xx/5xx errors)
- [ ] Dagster UI health check endpoint
- [ ] Set up PagerDuty/Opsgenie for alerting

### Cost Optimization

- [ ] Use S3 Lifecycle policies (move old data to Glacier)
- [ ] Right-size ECS tasks (don't over-provision)
- [ ] Use Spot instances for non-critical workloads
- [ ] Enable S3 Intelligent-Tiering for automatic cost optimization
- [ ] Monitor AWS Cost Explorer for unexpected charges

### Backup and Disaster Recovery

- [ ] Enable RDS automated backups (7-35 day retention)
- [ ] Enable S3 versioning
- [ ] Document recovery procedures
- [ ] Test backup restoration regularly

### Performance

- [ ] Use HIVE partitioning for large datasets
- [ ] Enable Parquet compression (Snappy or Zstd)
- [ ] Configure appropriate ECS task size
- [ ] Use DataFusion query caching
- [ ] Monitor query performance metrics

---

## Integration with DataFusion Service

Once Dagster is deployed, configure the DataFusion service ([dagster-datafusion](https://github.com/horizonanalytic/dagster-datafusion)) to discover assets:

```bash
# In dagster-datafusion .env
DAGSTER_PROD_DB_URL=postgresql://user:password@prod-db.rds.amazonaws.com:5432/dagster
DAGSTER_PROD_ENABLED=true
DAGSTER_SYNC_INTERVAL_SECS=300

# S3 Configuration (should match Dagster)
S3_BUCKET=your-prod-bucket
S3_PREFIX=dagster_prod
AWS_REGION=us-east-2
```

The DataFusion service will automatically discover and register Parquet tables from Dagster assets.

---

## Troubleshooting Production Issues

### Check ECS Task Logs

```bash
aws logs tail /ecs/dagster-webserver --follow
```

### Check RDS Connections

```bash
aws rds describe-db-instances --db-instance-identifier dagster-metadata-prod
```

### Verify S3 Access

```bash
aws s3 ls s3://your-prod-bucket/dagster_prod/ --recursive
```

### Check Dagster Health

```bash
curl https://dagster.yourdomain.com/health
```

---

## Useful Resources

- [Dagster Deployment Documentation](https://docs.dagster.io/deployment)
- [AWS ECS Best Practices](https://docs.aws.amazon.com/AmazonECS/latest/bestpracticesguide/)
- [Kubernetes Best Practices](https://kubernetes.io/docs/concepts/configuration/overview/)
