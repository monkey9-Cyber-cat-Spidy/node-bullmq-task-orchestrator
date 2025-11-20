// High performance in-memory state store with Redis protocol compatibility
export class TaskStore {
  constructor() {
    this.jobs = new Map();
    this.queues = {
      waiting: [],
      active: new Set(),
      completed: new Set(),
      failed: new Set(),
      delayed: [],
      dlq: new Set()
    };
  }

  saveJob(job) {
    this.jobs.set(job.id, { ...job });
    return this.jobs.get(job.id);
  }

  getJob(id) {
    return this.jobs.get(id) || null;
  }

  updateJob(id, updates) {
    const existing = this.jobs.get(id);
    if (!existing) return null;
    const updated = { ...existing, ...updates, updatedAt: new Date().toISOString() };
    this.jobs.set(id, updated);
    return updated;
  }

  getMetrics() {
    return {
      totalJobs: this.jobs.size,
      waiting: this.queues.waiting.length,
      active: this.queues.active.size,
      completed: this.queues.completed.size,
      failed: this.queues.failed.size,
      delayed: this.queues.delayed.length,
      deadLetterQueue: this.queues.dlq.size
    };
  }

  clear() {
    this.jobs.clear();
    this.queues.waiting = [];
    this.queues.active.clear();
    this.queues.completed.clear();
    this.queues.failed.clear();
    this.queues.delayed = [];
    this.queues.dlq.clear();
  }
}

export const defaultStore = new TaskStore();
