const { getChannel, QUEUE } = require('../config/rabbitmq');

// Producer: publishes an order.created message to the "orders" queue.
// This function returns immediately - it never waits for the worker.
function publishOrderCreated(order) {
  const channel = getChannel();

  const message = JSON.stringify({
    type: 'order.created',
    orderId: order._id.toString(),
    customerName: order.customerName,
    items: order.items,
    total: order.total,
    // Every message starts at 0. Each failed attempt increments this,
    // until it reaches MAX_RETRIES and the message goes to the DLQ.
    retryCount: 0
  });

  console.log('Publishing order.created');

  // persistent: the message is written to disk by RabbitMQ, so it is not
  // lost if RabbitMQ restarts while the worker is offline.
  channel.sendToQueue(QUEUE, Buffer.from(message), { persistent: true });

  console.log('Message published');
}

module.exports = { publishOrderCreated };
