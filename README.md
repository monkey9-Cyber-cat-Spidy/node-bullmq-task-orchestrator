# Node BullMQ Task Orchestrator

Production-grade distributed task orchestrator and asynchronous queue processing engine engineered for high-throughput background workloads, automated exponential backoff retries, and Dead Letter Queue (DLQ) fault isolation.

## Architecture & Features

- **Priority Queue Scheduling**: High priority workloads process first with strict sub-millisecond priority ordering.
- **Exponential Backoff & Jitter**: Automated retries with configurable exponential delay to prevent hammering downstream databases during transient outages.
- **Dead Letter Queue (DLQ)**: Hard failures exceeding retry limits are automatically isolated for manual inspection, error analysis, and on-demand replay.
- **Fine-Grained Worker Concurrency**: Configurable active worker threads per queue partition with graceful shutdown drain hooks.
- **Real-Time Telemetry & Metrics API**: Live status tracking, queue depths (active, waiting, completed, failed, delayed, dlq), and progress percentage reporting.
- **Production Ready Deployment**: Fully containerized with Docker and Docker Compose configuration.

## API Documentation

### 1. Enqueue Task
```http
POST /api/jobs
Content-Type: application/json

{
  "name": "report-generation",
  "data": { "rows": 5000 },
  "priority": 1,
  "attempts": 3
}
```

### 2. Query Job Status & Progress
```http
GET /api/jobs/:id
```

### 3. Replay Dead Letter Job
```http
POST /api/jobs/:id/retry
```

### 4. Health & Queue Depth Telemetry
```http
GET /api/health
```

## Testing & Verification

Run the automated test suite verifying priority ordering, worker retries, DLQ isolation, and progress tracking:
```bash
npm test
```
