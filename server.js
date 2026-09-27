require('dotenv').config();

const express = require('express');
const connectDB = require('./config/db');
const { connectRabbitMQ } = require('./config/rabbitmq');
const orderRoutes = require('./routes/orderRoutes');

const PORT = process.env.PORT || 3000;

async function start() {
  await connectDB();
  await connectRabbitMQ();

  const app = express();
  app.use(express.json());
  app.use(orderRoutes);

  app.listen(PORT, () => {
    console.log(`API server listening on http://localhost:${PORT}`);
  });
}

start().catch((err) => {
  console.error('Failed to start API server:', err.message);
  process.exit(1);
});
