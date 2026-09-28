require('dotenv').config();

const connectDB = require('./config/db');
const { connectRabbitMQ, QUEUE, DLQ } = require('./config/rabbitmq');
const Order = require('./models/Order');

// Set FAIL_MODE=true in .env to make every attempt fail, so you can watch
// the retry counter grow and finally see the message land in orders.dlq.
const FAIL_MODE = process.env.FAIL_MODE === 'true';

// Retry limit. The message is attempted 3 times in total
// (retryCount 0, 1, 2). The 3rd failure is sent to the DLQ instead of
// being requeued, so a broken message can never loop forever.
const MAX_RETRIES = 3;

// Every `node worker.js` process has its own pid, so the logs show which
// worker handled a message when several workers run side by side.
const WORKER_ID = String(process.pid);

const log = (...args) => console.log(`[Worker ${WORKER_ID}]`, ...args);
const logError = (...args) => console.error(`[Worker ${WORKER_ID}]`, ...args);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Publish helper. Returns true only if RabbitMQ accepted the message,
// so the caller knows when it is safe to ACK the original.
function tryPublish(channel, queueName, body) {
  try {
    channel.sendToQueue(queueName, Buffer.from(JSON.stringify(body)), { persistent: true });
    return true;
  } catch (err) {
    logError(`Could not publish to "${queueName}": ${err.message}`);
    return false;
  }
}

// Called when processing failed. Decides between retrying and dead-lettering.
async function handleFailure(channel, msg, payload, err) {
  logError(`Processing failed for order ${payload.orderId}: ${err.message}`);

  // retryCount is stored INSIDE the message, so it survives the trip
  // through the queue and any worker that picks the message up next.
  const retryCount = (payload.retryCount || 0) + 1;

  // Attempts 1 and 2 -> put the message back on the orders queue.
  if (retryCount < MAX_RETRIES) {
    log(`Retry attempt ${retryCount}/${MAX_RETRIES} - requeueing message on "${QUEUE}"`);

    // We cannot change a message that is already in the queue, so we publish
    // a NEW copy carrying the increased retryCount, then ACK the old copy.
    // (nack(requeue=true) would send back the UNCHANGED message - infinite loop.)
    if (!tryPublish(channel, QUEUE, { ...payload, retryCount })) {
      logError('Message NOT acknowledged - RabbitMQ will redeliver it');
      return;
    }

    channel.ack(msg);
    log('Message requeued for another attempt');
    return;
  }

  // Attempt 3 failed -> dead-letter the message instead of retrying again.
  log(`Max retries reached for order ${payload.orderId}`);
  log('Sending message to DLQ');

  const dlqMessage = JSON.stringify({
    orderId: payload.orderId,
    customerName: payload.customerName,
    items: payload.items,
    total: payload.total,
    retryCount,
    error: err.message,
    failedAt: new Date().toISOString()
  });

  try {
    channel.sendToQueue(DLQ, Buffer.from(dlqMessage), { persistent: true });
  } catch (publishErr) {
    // The DLQ publish failed, so we MUST NOT ack: the message stays with
    // RabbitMQ and will be redelivered instead of being lost.
    logError(`Could not publish to "${DLQ}": ${publishErr.message}`);
    logError('Message NOT acknowledged - RabbitMQ will redeliver it');
    return;
  }

  // Only now, after the copy is safe in the DLQ, do we remove the original.
  channel.ack(msg);
  log(`Message moved to ${DLQ}`);
}

async function start() {
  await connectDB();

  const { channel } = await connectRabbitMQ();

  // Competing consumers: this worker shares the ONE "orders" queue with
  // every other running worker. RabbitMQ gives each message to only one of them.
  // prefetch(1) => handle one unacknowledged message at a time.
  await channel.prefetch(1);

  log(`Worker started. Waiting for messages on queue "${QUEUE}"...`);

  // noAck: false => manual acknowledgements. RabbitMQ keeps the message
  // "in flight" until we explicitly call channel.ack(msg).
  await channel.consume(
    QUEUE,
    async (msg) => {
      if (!msg) return;

      let payload;
      try {
        payload = JSON.parse(msg.content.toString());
      } catch (err) {
        // A message we cannot even parse can never be processed.
        // Park it in the DLQ instead of retrying it 3 times.
        logError(`Invalid JSON message: ${err.message}`);
        if (tryPublish(channel, DLQ, { orderId: null, retryCount: 0, error: `Invalid JSON: ${err.message}` })) {
          channel.ack(msg);
          log(`Message moved to ${DLQ}`);
        }
        return;
      }

      try {
        const attempt = (payload.retryCount || 0) + 1;
        log(`Received order.created -> orderId=${payload.orderId}, customer=${payload.customerName}, attempt=${attempt}/${MAX_RETRIES}`);
        log('Processing order (simulated slow work, 3 seconds)...');

        await sleep(3000);

        log('Order confirmation is being processed...');

        // Simulated failure point. The message has NOT been acked yet.
        if (FAIL_MODE) {
          throw new Error('Simulated failure (FAIL_MODE=true)');
        }

        await Order.findByIdAndUpdate(payload.orderId, { status: 'processed' });

        log(`Order processed: ${payload.orderId} status "pending" -> "processed"`);

        // ACK: tell RabbitMQ "I handled this, you can forget it".
        channel.ack(msg);
        log('Message acknowledged');
      } catch (err) {
        await handleFailure(channel, msg, payload, err);
      }
    },
    { noAck: false }
  );
}

start().catch((err) => {
  // Node hides connection errors in an AggregateError with an empty message
  const details = err.errors ? err.errors.map((e) => e.message).join('; ') : err.message;
  logError(`Failed to start worker: ${details || err}`);
  process.exit(1);
});
