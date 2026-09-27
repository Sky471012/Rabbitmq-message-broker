const amqp = require('amqplib');

// The single queue this project uses. API publishes to it, worker consumes from it.
const QUEUE = 'orders';

let connection = null;
let channel = null;

async function connectRabbitMQ() {
  const url = process.env.RABBITMQ_URL;
  if (!url) throw new Error('RABBITMQ_URL is missing from .env');

  connection = await amqp.connect(url);
  channel = await connection.createChannel();

  // durable: the queue survives a RabbitMQ restart
  await channel.assertQueue(QUEUE, { durable: true });

  console.log('RabbitMQ connected. Queue "orders" is ready');
  return { connection, channel };
}

function getChannel() {
  if (!channel) throw new Error('RabbitMQ channel is not connected yet');
  return channel;
}

module.exports = { connectRabbitMQ, getChannel, QUEUE };
