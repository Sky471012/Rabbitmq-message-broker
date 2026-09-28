const amqp = require('amqplib');

// The main queue. API publishes to it, all workers consume from it.
// Multiple workers share this ONE queue (competing consumers).
const QUEUE = 'orders';

// Dead Letter Queue. A message lands here after it fails MAX_RETRIES times.
const DLQ = 'orders.dlq';

let connection = null;
let channel = null;

async function connectRabbitMQ() {
  const url = process.env.RABBITMQ_URL;
  if (!url) throw new Error('RABBITMQ_URL is missing from .env');

  connection = await amqp.connect(url);
  channel = await connection.createChannel();

  // durable: the queue survives a RabbitMQ restart
  await channel.assertQueue(QUEUE, { durable: true });
  await channel.assertQueue(DLQ, { durable: true });

  console.log(`RabbitMQ connected. Queues "${QUEUE}" and "${DLQ}" are ready`);
  return { connection, channel };
}

function getChannel() {
  if (!channel) throw new Error('RabbitMQ channel is not connected yet');
  return channel;
}

module.exports = { connectRabbitMQ, getChannel, QUEUE, DLQ };
