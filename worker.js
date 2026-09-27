require('dotenv').config();

const connectDB = require('./config/db');
const { connectRabbitMQ, QUEUE } = require('./config/rabbitmq');
const Order = require('./models/Order');

// Set FAIL_MODE=true in .env to simulate a processing failure and see
// that the message is NOT acknowledged and gets redelivered.
const FAIL_MODE = process.env.FAIL_MODE === 'true';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function start() {
  await connectDB();

  const { channel } = await connectRabbitMQ();

  // Process one message at a time so a slow order never races the next one.
  await channel.prefetch(1);

  console.log(`Worker started. Waiting for messages on queue "${QUEUE}"...`);

  // noAck: false => manual acknowledgements. RabbitMQ keeps the message
  // "in flight" until we explicitly call channel.ack(msg).
  await channel.consume(
    QUEUE,
    async (msg) => {
      if (!msg) return;

      try {
        const order = JSON.parse(msg.content.toString());

        console.log(`Received order.created -> orderId=${order.orderId}, customer=${order.customerName}`);
        console.log('Processing order (simulated slow work, 3 seconds)...');

        await sleep(3000);

        console.log('Order confirmation is being processed...');

        // Simulated failure point. The message has NOT been acked yet.
        if (FAIL_MODE) {
          throw new Error('Simulated failure (FAIL_MODE=true)');
        }

        await Order.findByIdAndUpdate(order.orderId, { status: 'processed' });

        console.log(`Order processed: ${order.orderId} status "pending" -> "processed"`);

        // ACK: tell RabbitMQ "I handled this, you can forget it".
        channel.ack(msg);
        console.log('Message acknowledged');
      } catch (err) {
        console.error(`Processing failed: ${err.message}`);

        // FAILURE: we never called ack, so RabbitMQ still owns the message.
        // nack(msg, false, true) = requeue=true, so RabbitMQ puts the message
        // back on the "orders" queue and delivers it again (redelivery).
        // (In real systems you would limit retries or use a dead-letter queue.)
        console.log('Message NOT acknowledged - RabbitMQ will redeliver it');
        channel.nack(msg, false, true);
      }
    },
    { noAck: false }
  );
}

start().catch((err) => {
  console.error('Failed to start worker:', err.message);
  process.exit(1);
});
