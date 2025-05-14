Objective
Develop a robust application that monitors a specified directory for MP4 video stream files, processes them in chunks, and uploads to S3-compatible object storage with checkpoint management.
Technical Requirements
Core Functionality
File Monitoring
Watch a specified directory for new .mp4 files
Detect file modifications to determine active video streams
Support continuous file updates during streaming
Chunk Processing
Read input files in 10MB chunks
Upload chunks sequentially to S3 object storage
Maintain chunk upload order and tracking
Stream Detection
Implement a mechanism to detect when a video stream is complete
Use file modification timeout (configurable, default 30 seconds)
Handle partial and complete video streams gracefully
Checkpoint and Reliability
Implement checkpoint mechanism to track:
Uploaded chunks
Current file processing state
Upload progress
Support resuming interrupted uploads
Handle network failures and application restarts
Metadata Management
Generate a metadata JSON file for each processed video stream
Include information:
Chunk list
Upload timestamps
Stream duration
Total file size
Technical Stack
Language: any of JavaScript/TypeScript, Go, Java/Kotlin, Rust, Python
Checkpoint Storage: any of your preference (database/persistent queue/key-value)
Containerization: Docker
Implementation Constraints
Implement robust error handling
Provide configurable parameters
Ensure idempotent chunk uploads
Support cancellation and graceful shutdown
Acceptance Criteria
Docker Compose setup with:
Application service
Checkpoints storage
MinIO (or any other S3-compatible storage)
Mountable local volume for input files
Successful processing of multiple MP4 files
Chunks and metadata visible in MinIO
Resilience to network interruptions
Checkpoint recovery mechanism
Detailed Workflow
Application starts and configures watchers
New MP4 file detected in watched directory
File chunks read and uploaded to S3
Metadata tracked in Redis
Stream completion detected
Final metadata file written
Ability to resume from last checkpoint on restart
Non-Functional Requirements
Logging of all significant events
Prometheus metrics for monitoring
Configurable timeout and chunk size
Minimal resource consumption
Deliverables
Complete source code in public Github repository
Dockerfile
Docker Compose configuration
README with setup and usage instructions
Example configuration files
Evaluation Criteria
Code quality
Error handling
Performance
Scalability
Documentation
Samples
Here are some code examples that can give you more context. You don’t have to reuse it.
Sample Implementation

import fs from 'fs';
import path from 'path';
import chokidar from 'chokidar';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';

interface StreamProcessorConfig {
watchDir: string;
s3Bucket: string;
chunkSize: number;
streamTimeout: number;
}

class VideoStreamProcessor {
private s3Client: S3Client;
private redisClient: Redis;
private config: StreamProcessorConfig;

constructor(config: StreamProcessorConfig) {
this.config = config;
this.s3Client = new S3Client({ /_ S3 configuration _/ });
this.redisClient = new Redis(/_ Redis connection _/);
}

async startWatching() {
const watcher = chokidar.watch(this.config.watchDir, {
ignored: /(^|[\/\\])\../, // ignore dotfiles
persistent: true,
awaitWriteFinish: {
stabilityThreshold: this.config.streamTimeout,
pollInterval: 100
}
});

    watcher
      .on('add', path => this.processNewFile(path))
      .on('change', path => this.processUpdatedFile(path));

}

private async processNewFile(filePath: string) {
if (!filePath.endsWith('.mp4')) return;

    const streamId = uuidv4();
    await this.initializeStreamMetadata(streamId, filePath);

}

private async processUpdatedFile(filePath: string) {
// Implement chunk reading and S3 upload logic
// Track progress in Redis
}

private async initializeStreamMetadata(streamId: string, filePath: string) {
// Store initial stream metadata
}

private async uploadChunkToS3(streamId: string, chunk: Buffer) {
// Implement idempotent chunk upload
}

private async finalizeStream(streamId: string) {
// Write metadata file to S3
// Clean up Redis entries
}
}

// Configuration and startup
const processor = new VideoStreamProcessor({
watchDir: '/input-videos',
s3Bucket: 'video-streams',
chunkSize: 10 _ 1024 _ 1024, // 10MB
streamTimeout: 30000 // 30 seconds
});

processor.startWatching();
Dockerfile

FROM node:18-alpine
WORKDIR /usr/src/app
RUN apk add --no-cache dumb-init
COPY package\*.json ./
RUN npm ci --only=production
COPY . .
RUN npm run build
ENTRYPOINT ["dumb-init", "--"]
CMD ["node", "dist/index.js"]

Docker Compose

version: '3.8'

services:
app:
build: .
volumes: - ./input-videos:/input-videos
depends_on: - minio - redis
environment: - MINIO_ENDPOINT=minio - REDIS_URL=redis://redis:6379

minio:
image: minio/minio
ports: - "9000:9000" - "9001:9001"
environment:
MINIO_ROOT_USER: minioadmin
MINIO_ROOT_PASSWORD: minioadmin
command: server /data --console-address ":9001"
volumes: - minio-data:/data

redis:
image: redis:alpine
volumes: - redis-data:/data

volumes:
minio-data:
redis-data:

Gap Analysis & Coverage Matrix
Requirement (from original brief) Covered? Where / How it’s handled (after revision)
Watch dir for new .mp4 ✅ watchfiles.awatch() on _.mp4 in WATCH_DIR
Detect file modifications & support continuous writes ✅ Watcher re‑fires on modified; reads deltas until idle timeout
10 MB sequential chunks, preserve order ✅ Fixed CHUNK_SIZE (default 10 MiB, env override) + incremental part_number
Upload tracking (list + order) ✅ Redis hash parts:{stream} stores {part_no:etag}
Stream‑completed detection by mtime‑Δ > timeout (30 s default) ✅ Periodic stale‑scan task; STREAM_TIMEOUT env
Graceful handling of partial & complete streams ✅ Multipart uploads can stay “incomplete”; resume picks them up
Checkpoint mech. (chunks, state, progress) ✅ Redis keys: upload_id, next_part, bytes_sent, hash of ETags
Resume after crash / network fail ✅ On startup iterate streams:active; retry with exponential back‑off
Idempotent uploads ✅ Same part_number overwrites in S3 multipart; verify ETag vs Redis
Metadata JSON w/ chunk list + timestamps + duration + size 🚧 Added New metadata.py—after complete_multipart_upload():
  • ffprobe (static build) to grab duration
  • Collect {part_no, size, etag, uploaded_at} list
  • JSON schema → put to s3://<bucket>/<key>.metadata.json
Logging all significant events 🚧 Added structlog + JSON logs to stdout; levels configurable (LOG_LEVEL)
Prometheus metrics ✅ chunks_uploaded_total, failed_upload_total, active_streams, bytes_uploaded_total
Configurable parameters ✅ pydantic_settings.Settings maps env → runtime
Cancellation / graceful shutdown ✅ SIGINT/SIGTERM set event → coroutines flush & exit
Minimal resource consumption ✅ Single asyncio loop, bounded worker pool; memory mainly buffer _ 2 chunks
Docker Compose: app + MinIO + checkpoint store + bind mount ✅ docker-compose.yml (rev v3.9) shown below
Multiple MP4 processed successfully ✅ Concurrent tasks per file (async), order preserved within file
Visible chunks & metadata in MinIO ✅ Multipart parts + \*.metadata.json object
Resilience to network interruptions ✅ aioboto3/botocore auto‑retry + resume logic
Checkpoint recovery mechanism ✅ Redis persists to volume; resume logic on boot
Prom metrics port ✅ PROM_PORT env (default 8000) exposed in Compose
Deliverables (Dockerfile, README, etc.) ✅ Outlined; README includes quick‑start & sample .env
Example configuration files ✅ example.env, prometheus.yml snippet in repo

No uncovered items remain.

Revised High‑Level Plan (v2)
Configuration

config.py (env‑driven) – now also FFPROBE_PATH (optional; default =ffprobe in PATH).

Watcher Layer

watcher.py

Async generator from watchfiles.

Emits StreamEvent(type=CREATE|WRITE|IDLE, path).

Uses STREAM_TIMEOUT to fire IDLE → marks stream ready to finish.

Processing Pipeline per Stream

pgsql
Copy
Edit
┌── on CREATE ────────────────────────────────────────────┐
│ generate stream_id (UUID v4) │
│ Redis HSET stream:{id}:meta {path, started_at, …} │
│ S3: CreateMultipartUpload → upload_id │
└──────────────────────────────────────────────────────────┘
│
┌── on WRITE ─────────────────────────────────────────────┐
│ while new_bytes >= CHUNK_SIZE: │
│ read chunk, part_no := INCR stream:{id}:next_part │
│ aioboto3.upload_part │
│ HSET parts:{id} part_no etag; INCRBY bytes_sent │
└──────────────────────────────────────────────────────────┘
│
┌── on IDLE (> timeout) ──────────────────────────────────┐
│ ffprobe duration │
│ CompleteMultipartUpload │
│ Build metadata JSON │
│ PutObject metadata.json │
│ DEL stream:{id}:\* │
└──────────────────────────────────────────────────────────┘
Checkpoint & Resume

On start‑up:

Scan streams:active set.

For each, load upload_id, last part_no, continue upload loop.

Observability

Logging – structured JSON (log level via LOG_LEVEL).

Metrics – exported at /metrics:

video_chunks_uploaded_total{stream_id}

video_bytes_uploaded_total

video_stream_duration_seconds

video_failed_upload_total

Optional /healthz HTTP endpoint (fastapi) for liveness/readiness.

Docker images

app: Python 3.12‑slim + ffprobe static (≈ 32 MB layer)

minio & redis as before.

Small Alpine sidecar for Prom → not required—Prometheus can scrape app directly.

Repository layout

bash
Copy
Edit
.
├── src/
├── tests/
├── Dockerfile
├── docker-compose.yml
├── example.env
├── README.md
└── .github/workflows/ci.yml
CI / Quality

ruff, pytest, mypy.

GitHub Actions workflow:

lint → unit tests → docker compose up --build -d → integration test copies sample MP4, awaits metadata appearance → teardown.

Updated docker-compose.yml (excerpt)
yaml
Copy
Edit
version: '3.9'
services:
app:
build: .
environment:
WATCH_DIR: /input-videos
S3_ENDPOINT: http://minio:9000
S3_BUCKET: video-streams
REDIS_URL: redis://redis:6379/0
PROM_PORT: 8000
STREAM_TIMEOUT: 30
volumes: - ./input-videos:/input-videos
depends_on: [minio, redis]
ports: - "8000:8000" # Prometheus scrape

minio:
image: minio/minio:latest
environment:
MINIO_ROOT_USER: minioadmin
MINIO_ROOT_PASSWORD: minioadmin
command: server /data --console-address ":9001"
ports: - "9000:9000" - "9001:9001"
volumes: - minio-data:/data

redis:
image: redis:7-alpine
volumes: - redis-data:/data

volumes:
minio-data:
redis-data:
What changed since the previous draft?
Area Revision
Metadata JSON Now mandatory, includes chunk list (+ ETags), upload timestamps, stream duration via ffprobe, total size, start/end times.
Logging Switched to structlog with JSON output; added LOG_LEVEL env.
Metrics Added per‑stream duration gauge + bytes counter.
Docs / README Will describe installing static ffprobe, running Prometheus example, and environment variable matrix.

All original requirements are now explicitly satisfied. Let me know if you want deeper detail (full code snippet, unit‑test example, Helm chart, etc.).
