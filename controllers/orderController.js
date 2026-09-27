const mongoose = require('mongoose');
const orderService = require('../services/orderService');
const { publishOrderCreated } = require('../services/producer');

// Simple manual validation so the API never writes bad data to MongoDB.
function validateOrder(body) {
  const errors = [];

  if (!body.customerName || typeof body.customerName !== 'string' || !body.customerName.trim()) {
    errors.push('customerName is required');
  }

  if (!Array.isArray(body.items) || body.items.length === 0) {
    errors.push('items must be a non-empty array');
  } else {
    body.items.forEach((item, i) => {
      if (!item.name) errors.push(`items[${i}].name is required`);
      if (typeof item.quantity !== 'number' || item.quantity < 1) errors.push(`items[${i}].quantity must be a number >= 1`);
      if (typeof item.price !== 'number' || item.price < 0) errors.push(`items[${i}].price must be a number >= 0`);
    });
  }

  if (typeof body.total !== 'number' || body.total < 0) {
    errors.push('total must be a number >= 0');
  }

  return errors;
}

// POST /orders
async function createOrder(req, res) {
  try {
    const errors = validateOrder(req.body);
    if (errors.length > 0) {
      return res.status(400).json({ errors });
    }

    const order = await orderService.createOrder({
      customerName: req.body.customerName.trim(),
      items: req.body.items,
      total: req.body.total,
      status: 'pending'
    });

    console.log(`Order created: ${order._id}`);

    // Publish to RabbitMQ. The worker will pick it up whenever it is ready.
    publishOrderCreated(order);

    // We return immediately - we do NOT wait for the worker to process it.
    console.log('Response returned');
    return res.status(201).json(order);
  } catch (err) {
    console.error('Error creating order:', err.message);
    return res.status(500).json({ error: 'Failed to create order' });
  }
}

// GET /orders/:id
async function getOrder(req, res) {
  try {
    const { id } = req.params;

    if (!mongoose.isValidObjectId(id)) {
      return res.status(400).json({ error: 'Invalid order id' });
    }

    const order = await orderService.getOrderById(id);
    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }

    return res.json(order);
  } catch (err) {
    console.error('Error fetching order:', err.message);
    return res.status(500).json({ error: 'Failed to fetch order' });
  }
}

module.exports = { createOrder, getOrder };
