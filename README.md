# kafka-dwp-pipeline

> A production-grade event streaming pipeline using Apache Kafka — built with Node.js producers, Spring Boot consumers, MySQL + MongoDB sinks, and deployed on AWS via GitLab CI/CD.

---

## Architecture Overview

```
┌─────────────────┐        ┌──────────────────────────────────────────┐        ┌──────────────────────┐
│   Producer      │        │         Kafka Broker Cluster (x3)        │        │   Consumer Group     │
│   (Node.js)     │──────► │  Topic: dwp.orders                       │──────► │  (Spring Boot)       │
│   REST API      │        │  Partitions: 3 | Replication Factor: 3   │        │  C1 → MySQL (RDS)    │
│   Spring Boot   │        │  Retention: 7d | Segment: 1GB            │        │  C2 → MongoDB        │
└─────────────────┘        └──────────────────────────────────────────┘        │  C3 → Audit log      │
                                                                                └──────────────────────┘
         │                              │                                                │
         └──────────────────────────────┴────────────────────────────────────────────────┘
                                    AWS EC2 · RDS · S3 · VPC
                                    GitLab CI/CD · Terraform
```

### Key Design Decisions

| Concern | Choice | Reason |
|---|---|---|
| Partitioning | `user_id` as key | Guarantees ordering per user |
| Replication | RF=3, min.insync=2 | Tolerates 1 broker failure without data loss |
| Consumer delivery | At-least-once + idempotency key | Safe for financial/DWP data |
| Schema safety | JSON Schema validation | Catches breaking changes before they reach brokers |
| DB sink | MySQL for relational, MongoDB for event log | Play to each database's strengths |

---

## Project Structure

```
kafka-dwp-pipeline/
├── producer-service/          # Node.js Kafka producer (REST API → Kafka)
│   ├── src/
│   │   ├── app.js             # Express entry point
│   │   ├── kafka/
│   │   │   ├── producer.js    # KafkaJS producer setup
│   │   │   └── schemas.js     # JSON schema validation
│   │   └── routes/
│   │       └── orders.js      # POST /orders → Kafka
│   ├── package.json
│   └── Dockerfile
│
├── consumer-service/          # Spring Boot Kafka consumer → MySQL + MongoDB
│   ├── src/main/java/com/dwp/kafka/
│   │   ├── config/            # Kafka consumer config
│   │   ├── consumer/          # @KafkaListener handlers
│   │   ├── model/             # Order entity / document
│   │   └── service/           # Business logic + DB writes
│   ├── pom.xml
│   └── Dockerfile
│
├── docker/
│   └── docker-compose.yml     # Local dev: Kafka, Zookeeper, MySQL, MongoDB
│
├── infrastructure/
│   └── terraform/             # AWS EC2, RDS, MSK, S3, VPC
│
├── scripts/
│   ├── create-topics.sh       # Idempotent topic creation
│   └── health-check.sh        # Cluster health assertions
│
└── .gitlab-ci.yml             # Full CI/CD pipeline
```

---

## Quick Start (Local)

### Prerequisites

- Docker + Docker Compose
- Node.js 18+
- Java 17 + Maven

### 1. Start the local stack

```bash
cd docker
docker-compose up -d
```

This starts:
- Kafka broker (port 9092)
- Zookeeper (port 2181)
- MySQL (port 3306)
- MongoDB (port 27017)
- Kafka UI (port 8080) → http://localhost:8080

### 2. Create topics

```bash
chmod +x scripts/create-topics.sh
./scripts/create-topics.sh
```

### 3. Start the producer

```bash
cd producer-service
npm install
npm run dev
```

Producer runs on http://localhost:3000

### 4. Start the consumer

```bash
cd consumer-service
mvn spring-boot:run
```

### 5. Publish a test event

```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "usr-001",
    "orderId": "ord-abc-123",
    "service": "universal-credit",
    "amount": 450.00,
    "currency": "GBP",
    "status": "SUBMITTED"
  }'
```

Watch the consumer logs — you'll see the event land in MySQL and MongoDB within milliseconds.

---

## Environment Variables

### Producer (Node.js)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `KAFKA_CLIENT_ID` | `dwp-producer` | Producer client identifier |
| `KAFKA_TOPIC` | `dwp.orders` | Target topic |
| `PORT` | `3000` | HTTP server port |

### Consumer (Spring Boot)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker list |
| `KAFKA_GROUP_ID` | `dwp-consumers` | Consumer group ID |
| `MYSQL_URL` | `jdbc:mysql://localhost:3306/dwp` | RDS connection |
| `MONGO_URI` | `mongodb://localhost:27017/dwp_events` | MongoDB connection |

---

## Kafka Topic Configuration

```properties
Topic:              dwp.orders
Partitions:         3
Replication Factor: 3
Retention:          604800000ms (7 days)
Segment Size:       1073741824 (1 GB)
Compression:        lz4
Cleanup Policy:     delete
```

---

## AWS Deployment

See [`infrastructure/terraform/`](./infrastructure/terraform/) for full Terraform definitions.

Resources provisioned:
- **Amazon MSK** — managed Kafka cluster (3 brokers, kafka.m5.large)
- **EC2** — producer and consumer services (t3.medium, auto-scaling group)
- **RDS MySQL** — Multi-AZ, encrypted at rest, automated backups
- **MongoDB Atlas** (or DocumentDB) — replica set, VPC peered
- **S3** — Kafka log archival + Terraform state
- **VPC** — private subnets, security groups, NAT gateway

---

## CI/CD Pipeline (GitLab)

The `.gitlab-ci.yml` defines a 4-stage pipeline:

```
build → test → security-scan → deploy
```

- **build**: Docker images for producer + consumer
- **test**: Unit tests + Kafka integration tests (Testcontainers)
- **security-scan**: Trivy image scan, SAST
- **deploy**: Rolling update to EC2, zero-downtime restart

---

## Monitoring

Once deployed, connect Prometheus + Grafana to track:

- Consumer lag per partition (`kafka_consumer_group_lag`)
- Producer throughput (`kafka_producer_record_send_rate`)
- Broker disk usage (`kafka_log_size`)
- Under-replicated partitions (`kafka_server_replicamanager_underreplicatedpartitions`)

---

## Contributing

1. Fork the repo
2. Create a feature branch (`git checkout -b feat/your-feature`)
3. Commit with conventional commits (`feat:`, `fix:`, `chore:`)
4. Push and open a merge request

---

## Author

**Kishan Gupta** — Full-Stack Engineer · Data & Streaming Systems  
5+ years building distributed systems on Node.js, Spring Boot, AWS, and Apache Kafka.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat-square&logo=linkedin)](https://linkedin.com/in/your-profile)
[![The DATA Lab](https://img.shields.io/badge/The_DATA_Lab-Follow-7C3AED?style=flat-square)](https://github.com/your-username)
