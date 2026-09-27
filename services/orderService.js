const Order = require('../models/Order');

async function createOrder(data) {
  return Order.create(data);
}

async function getOrderById(id) {
  return Order.findById(id);
}

module.exports = { createOrder, getOrderById };
