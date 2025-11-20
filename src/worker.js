import { defaultStore } from './storage.js';

export class TaskWorker {
  constructor(queueName, processor, options = {}) {
    this.queueName = queueName;
    this.processor = processor;
    this.store = options.store || defaultStore;
    this.concurrency = options.concurrency || 2;
    this.running = false;
    this.activeWorkers = 0;
    this.intervalId = null;
  }

  start() {
    if (this.running) return;
    this.running = true;
    this.intervalId = setInterval(() => this.processNext(), 20);
  }

  stop() {
    this.running = false;
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  async processNext() {
    if (!this.running || this.activeWorkers >= this.concurrency) return;
    if (this.store.queues.waiting.length === 0) return;

    const jobId = this.store.queues.waiting.shift();
    if (!jobId) return;

    const job = this.store.getJob(jobId);
    if (!job || job.status !== 'waiting') return;

    this.activeWorkers++;
    this.store.queues.active.add(jobId);
    this.store.updateJob(jobId, { status: 'active', startedAt: new Date().toISOString() });

    try {
      const updateProgress = async (percent) => {
        this.store.updateJob(jobId, { progress: Math.min(100, Math.max(0, percent)) });
      };

      const result = await this.processor({
        id: job.id,
        name: job.name,
        data: job.data,
        attemptsMade: job.attemptsMade + 1,
        updateProgress
      });

      this.store.queues.active.delete(jobId);
      this.store.queues.completed.add(jobId);
      this.store.updateJob(jobId, {
        status: 'completed',
        progress: 100,
        result,
        completedAt: new Date().toISOString(),
        attemptsMade: job.attemptsMade + 1
      });
    } catch (err) {
      this.store.queues.active.delete(jobId);
      const attemptsMade = job.attemptsMade + 1;
      const failedReason = err.message || 'Unknown processing error';

      if (attemptsMade < job.maxAttempts) {
        // Schedule retry with exponential backoff
        const backoffDelay = Math.pow(2, attemptsMade) * 50;
        this.store.updateJob(jobId, {
          status: 'delayed',
          attemptsMade,
          failedReason
        });
        this.store.queues.delayed.push(jobId);

        setTimeout(() => {
          const idx = this.store.queues.delayed.indexOf(jobId);
          if (idx !== -1) {
            this.store.queues.delayed.splice(idx, 1);
            this.store.updateJob(jobId, { status: 'waiting' });
            this.store.queues.waiting.push(jobId);
          }
        }, backoffDelay);
      } else {
        // Exceeded max retries, move to Dead Letter Queue (DLQ)
        this.store.queues.failed.add(jobId);
        this.store.queues.dlq.add(jobId);
        this.store.updateJob(jobId, {
          status: 'failed',
          attemptsMade,
          failedReason,
          inDlq: true,
          failedAt: new Date().toISOString()
        });
      }
    } finally {
      this.activeWorkers--;
    }
  }

  async retryDlqJob(jobId) {
    const job = this.store.getJob(jobId);
    if (!job || !this.store.queues.dlq.has(jobId)) {
      throw new Error('Job not found in Dead Letter Queue');
    }
    this.store.queues.dlq.delete(jobId);
    this.store.queues.failed.delete(jobId);
    this.store.updateJob(jobId, {
      status: 'waiting',
      attemptsMade: 0,
      failedReason: null,
      inDlq: false
    });
    this.store.queues.waiting.push(jobId);
    return this.store.getJob(jobId);
  }
}
