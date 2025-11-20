import crypto from 'crypto';
import { defaultStore } from './storage.js';

export class TaskQueue {
  constructor(name = 'default-orchestrator', store = defaultStore, options = {}) {
    this.name = name;
    this.store = store;
    this.defaultMaxAttempts = options.maxAttempts || 3;
    this.defaultBackoffMs = options.backoffMs || 100;
  }

  async add(name, data = {}, options = {}) {
    const id = options.jobId || 'job_' + crypto.randomBytes(8).toString('hex');
    const priority = options.priority || 10; // lower number = higher priority
    const maxAttempts = options.attempts || this.defaultMaxAttempts;
    const delay = options.delay || 0;

    const job = {
      id,
      name,
      data,
      priority,
      attemptsMade: 0,
      maxAttempts,
      status: delay > 0 ? 'delayed' : 'waiting',
      progress: 0,
      result: null,
      failedReason: null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      executeAt: Date.now() + delay
    };

    this.store.saveJob(job);

    if (delay > 0) {
      this.store.queues.delayed.push(id);
      setTimeout(() => {
        const idx = this.store.queues.delayed.indexOf(id);
        if (idx !== -1) {
          this.store.queues.delayed.splice(idx, 1);
          this.store.updateJob(id, { status: 'waiting' });
          this.insertByPriority(id, priority);
        }
      }, delay);
    } else {
      this.insertByPriority(id, priority);
    }

    return job;
  }

  insertByPriority(id, priority) {
    const list = this.store.queues.waiting;
    let inserted = false;
    for (let i = 0; i < list.length; i++) {
      const existingJob = this.store.getJob(list[i]);
      if (existingJob && priority < existingJob.priority) {
        list.splice(i, 0, id);
        inserted = true;
        break;
      }
    }
    if (!inserted) {
      list.push(id);
    }
  }

  async getJob(id) {
    return this.store.getJob(id);
  }

  async getMetrics() {
    return this.store.getMetrics();
  }
}
