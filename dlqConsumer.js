require('dotenv').config();

const { connectRabbitMQ, DLQ } = require('./config/rabbitmq');

// Reads dead-lettered messages from "orders.dlq" and only logs them.
// It never touches MongoDB: its job is to prove that failed messages were
// preserved instead of disappearing. Nothing here is automatic - you start
// this script yourself with: npm run dlq
async function start() {
  const { channel } = await connectRabbitMQ();

  await channel.prefetch(1);

  console.log(`DLQ consumer started. Waiting for messages on "${DLQ}"...`);

  await channel.consume(
    DLQ,
    (msg) => {
      if (!msg) return;

      const failed = JSON.parse(msg.content.toString());

      console.log('DLQ message received');
      console.log(`orderId: ${failed.orderId}`);
      console.log(`retryCount: ${failed.retryCount}`);
      console.log(`reason: ${failed.error}`);
      console.log(`customerName: ${failed.customerName}`);

      // The message was read successfully, so now we can remove it.
      channel.ack(msg);
      console.log(`Message acknowledged (removed from "${DLQ}")`);
    },
    { noAck: false }
  );
}

start().catch((err) => {
  console.error('Failed to start DLQ consumer:', err.message);
  process.exit(1);
});
