import { test, describe, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import { TaskStore } from '../src/storage.js';
import { TaskQueue } from '../src/queue.js';
import { TaskWorker } from '../src/worker.js';

describe('Distributed Task Orchestrator Tests', () => {
  let store;
  let queue;
  let worker;

  beforeEach(() => {
    store = new TaskStore();
    queue = new TaskQueue('test-queue', store);
  });

  afterEach(() => {
    if (worker) worker.stop();
  });

  test('should enqueue a job with priority and retrieve metrics', async () => {
    const job = await queue.add('test-task', { foo: 'bar' }, { priority: 1 });
    assert.strictEqual(job.name, 'test-task');
    assert.strictEqual(job.status, 'waiting');
    assert.strictEqual(job.priority, 1);

    const metrics = await queue.getMetrics();
    assert.strictEqual(metrics.totalJobs, 1);
    assert.strictEqual(metrics.waiting, 1);
  });

  test('should process a job successfully with progress updates', async () => {
    let progressSeen = 0;
    worker = new TaskWorker('test-queue', async (job) => {
      await job.updateProgress(50);
      return { success: true };
    }, { store, concurrency: 1 });

    worker.start();
    const job = await queue.add('calc', { n: 42 });

    // Wait for worker execution
    await new Promise(resolve => setTimeout(resolve, 100));

    const updatedJob = await queue.getJob(job.id);
    assert.strictEqual(updatedJob.status, 'completed');
    assert.strictEqual(updatedJob.progress, 100);
    assert.deepStrictEqual(updatedJob.result, { success: true });
  });

  test('should retry failed jobs and move to DLQ when maxAttempts exceeded', async () => {
    let attempts = 0;
    worker = new TaskWorker('test-queue', async () => {
      attempts++;
      throw new Error('Fatal database connection error');
    }, { store, concurrency: 1 });

    worker.start();
    const job = await queue.add('retry-task', {}, { attempts: 2 });

    // Wait for retries to exhaust
    await new Promise(resolve => setTimeout(resolve, 350));

    const failedJob = await queue.getJob(job.id);
    assert.strictEqual(failedJob.status, 'failed');
    assert.strictEqual(failedJob.inDlq, true);
    assert.strictEqual(failedJob.attemptsMade, 2);

    const metrics = await queue.getMetrics();
    assert.strictEqual(metrics.deadLetterQueue, 1);
  });

  test('should recover and re-queue a job from DLQ', async () => {
    worker = new TaskWorker('test-queue', async () => {
      throw new Error('Critical error');
    }, { store, concurrency: 1 });

    worker.start();
    const job = await queue.add('dlq-job', {}, { attempts: 1 });
    await new Promise(resolve => setTimeout(resolve, 80));

    assert.strictEqual((await queue.getJob(job.id)).inDlq, true);

    // Manual retry
    const retriedJob = await worker.retryDlqJob(job.id);
    assert.strictEqual(retriedJob.status, 'waiting');
    assert.strictEqual(retriedJob.inDlq, false);
  });
});
