terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # S3 backend — state stored remotely, matches your existing AWS setup
  backend "s3" {
    bucket         = "dwp-kafka-terraform-state"
    key            = "kafka-pipeline/terraform.tfstate"
    region         = "eu-west-2"  # London region
    encrypt        = true
    dynamodb_table = "terraform-locks"
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "dwp-kafka-pipeline"
      ManagedBy   = "terraform"
      Environment = var.environment
    }
  }
}

# ── VARIABLES ──────────────────────────────────────────────────────────────

variable "aws_region"   { default = "eu-west-2" }
variable "environment"  { default = "production" }
variable "db_password"  { sensitive = true }

# ── VPC ────────────────────────────────────────────────────────────────────

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
}

resource "aws_subnet" "private" {
  count             = 3
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]
}

data "aws_availability_zones" "available" { state = "available" }

# ── SECURITY GROUPS ────────────────────────────────────────────────────────

resource "aws_security_group" "kafka" {
  name   = "dwp-kafka-sg"
  vpc_id = aws_vpc.main.id

  # Kafka broker port — only from within VPC
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Kafka plaintext — internal VPC only"
  }

  # TLS port for MSK
  ingress {
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Kafka TLS"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "app" {
  name   = "dwp-app-sg"
  vpc_id = aws_vpc.main.id

  ingress {
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Producer HTTP"
  }

  ingress {
    from_port   = 8081
    to_port     = 8081
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Consumer actuator"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ── AMAZON MSK (Managed Kafka) ─────────────────────────────────────────────

resource "aws_msk_cluster" "dwp" {
  cluster_name           = "dwp-kafka-${var.environment}"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = aws_subnet.private[*].id
    security_groups = [aws_security_group.kafka.id]

    storage_info {
      ebs_storage_info {
        volume_size = 100  # GB per broker
      }
    }
  }

  # Encryption at rest + in transit — mandatory for DWP data
  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  # IAM authentication — no long-lived credentials
  client_authentication {
    sasl {
      iam = true
    }
  }

  # Enable broker logs to S3 for compliance
  broker_logs {
    s3 {
      enabled = true
      bucket  = aws_s3_bucket.kafka_logs.id
      prefix  = "broker-logs/"
    }
  }
}

# ── S3 (Log archival) ──────────────────────────────────────────────────────

resource "aws_s3_bucket" "kafka_logs" {
  bucket = "dwp-kafka-logs-${var.environment}-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "kafka_logs" {
  bucket = aws_s3_bucket.kafka_logs.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

data "aws_caller_identity" "current" {}

# ── RDS MySQL (Multi-AZ) ───────────────────────────────────────────────────

resource "aws_db_subnet_group" "main" {
  name       = "dwp-db-subnet-group"
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_db_instance" "mysql" {
  identifier        = "dwp-mysql-${var.environment}"
  engine            = "mysql"
  engine_version    = "8.0"
  instance_class    = "db.t3.medium"
  allocated_storage = 100
  storage_type      = "gp3"
  storage_encrypted = true  # always encrypt DWP data at rest

  db_name  = "dwp"
  username = "dwp_user"
  password = var.db_password

  multi_az               = true   # automatic failover
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.kafka.id]

  backup_retention_period = 7
  deletion_protection     = true  # never accidentally drop a production DB

  performance_insights_enabled = true

  parameter_group_name = aws_db_parameter_group.mysql.name
}

resource "aws_db_parameter_group" "mysql" {
  family = "mysql8.0"
  name   = "dwp-mysql-params"

  parameter {
    name  = "slow_query_log"
    value = "1"
  }
  parameter {
    name  = "long_query_time"
    value = "1"  # log queries slower than 1 second
  }
  parameter {
    name  = "binlog_format"
    value = "ROW"  # required for Debezium CDC
  }
}

# ── OUTPUTS ────────────────────────────────────────────────────────────────

output "msk_bootstrap_brokers_tls" {
  value       = aws_msk_cluster.dwp.bootstrap_brokers_sasl_iam
  description = "MSK broker endpoints for producer/consumer config"
  sensitive   = true
}

output "rds_endpoint" {
  value       = aws_db_instance.mysql.endpoint
  description = "RDS MySQL endpoint"
  sensitive   = true
}
