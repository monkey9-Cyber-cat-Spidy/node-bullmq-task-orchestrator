import express from 'express';
import { TaskQueue } from './queue.js';
import { TaskWorker } from './worker.js';
import { defaultStore } from './storage.js';

const app = express();
app.use(express.json());

export const queue = new TaskQueue('orchestrator-queue', defaultStore);

// Register worker with simulated task processors
export const worker = new TaskWorker('orchestrator-queue', async (job) => {
  if (job.name === 'email-dispatch') {
    await job.updateProgress(50);
    return { dispatched: true, recipient: job.data.recipient };
  } else if (job.name === 'report-generation') {
    await job.updateProgress(25);
    await job.updateProgress(75);
    return { reportId: 'REP_' + Date.now(), rows: job.data.rows || 100 };
  } else if (job.name === 'faulty-task') {
    if (job.attemptsMade < 3) {
      throw new Error('Simulated transient worker failure');
    }
    return { recovered: true };
  } else if (job.name === 'permanent-fail') {
    throw new Error('Unrecoverable critical processing error');
  }
  return { status: 'processed', name: job.name };
}, { concurrency: 4, store: defaultStore });

worker.start();

// REST API Endpoints
app.get('/api/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    metrics: queue.getMetrics()
  });
});

app.post('/api/jobs', async (req, res) => {
  const { name, data, priority, attempts, delay } = req.body;
  if (!name) {
    return res.status(400).json({ error: 'Job name is required' });
  }

  const job = await queue.add(name, data, { priority, attempts, delay });
  res.status(202).json({
    message: 'Job enqueued successfully',
    job
  });
});

app.get('/api/jobs/:id', async (req, res) => {
  const job = await queue.getJob(req.params.id);
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  res.json({ job });
});

app.post('/api/jobs/:id/retry', async (req, res) => {
  try {
    const job = await worker.retryDlqJob(req.params.id);
    res.json({ message: 'Job rescheduled from DLQ', job });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
if (process.env.NODE_ENV !== 'test') {
  app.listen(PORT, () => {
    console.log(`Task Orchestrator listening on port ${PORT}`);
  });
}

export default app;
