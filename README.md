# RabbitMQ Order Processing — Learning Project

A small Node.js project that demonstrates the fundamental **producer → queue → consumer** flow using RabbitMQ, with an e-commerce order-processing example.

**Stack:** Node.js, Express, MongoDB Atlas, Mongoose, RabbitMQ, JavaScript (no TypeScript).

## What this teaches

- Producer / consumer architecture
- Asynchronous processing (API returns before work finishes)
- Queues and message persistence
- Manual acknowledgements (`ack` / `nack`)
- Decoupling the API from the worker

**Not included (on purpose):** exchanges, routing keys, dead-letter queues, retries, competing consumers, auth, frontend, Docker, Redis, Kafka.

---

## Project structure

```
messageBroker-rabbitmq/
├── server.js                 # Express API (producer side)
├── worker.js                 # RabbitMQ consumer (separate process)
├── config/
│   ├── db.js                 # Mongoose connection
│   └── rabbitmq.js           # RabbitMQ connection + "orders" queue
├── models/
│   └── Order.js              # Order schema
├── routes/
│   └── orderRoutes.js        # POST /orders, GET /orders/:id
├── controllers/
│   └── orderController.js    # validation + response handling
├── services/
│   ├── orderService.js       # MongoDB reads/writes
│   └── producer.js           # publishes order.created
├── package.json
├── .env.example
└── .gitignore
```

---

## Setup

### 1. Install packages

**Linux / macOS (bash):**
```bash
npm install express mongoose amqplib dotenv
```

**PowerShell:**
```powershell
cd "D:\projects\System Design\messageBroker-rabbitmq"
npm install express mongoose amqplib dotenv
```

### 2. Create your `.env`

**Linux / macOS:**
```bash
cp .env.example .env
```

**PowerShell:**
```powershell
Copy-Item .env.example .env
```

Then edit `.env` and set your real MongoDB Atlas URI:

```env
PORT=3000
MONGODB_URI=mongodb+srv://USER:PASSWORD@cluster0.xxxxx.mongodb.net/orders-db?retryWrites=true&w=majority
RABBITMQ_URL=amqp://localhost:5672
FAIL_MODE=false
```

### 3. Start RabbitMQ

**Linux (apt):**
```bash
sudo systemctl start rabbitmq-server
sudo systemctl status rabbitmq-server
# management UI (optional): http://localhost:15672 (guest/guest)
```

**PowerShell (if installed as a Windows service):**
```powershell
Start-Service RabbitMQ
Get-Service RabbitMQ
```

Or run it another way (Docker on your machine, etc.) — just make sure `RABBITMQ_URL` matches.

### 4. Start the processes

You need **three terminals**.

**Terminal A — API:**
```bash
npm start
```
```powershell
npm start
```

**Terminal B — Worker:**
```bash
npm run worker
```
```powershell
npm run worker
```

Expected logs:
```
API:    MongoDB connected
        RabbitMQ connected. Queue "orders" is ready
        API server listening on http://localhost:3000

Worker: MongoDB connected
        RabbitMQ connected. Queue "orders" is ready
        Worker started. Waiting for messages on queue "orders"...
```

---

## API endpoints

### `POST /orders`

Creates an order with status `pending`, publishes an `order.created` message, and returns immediately (does **not** wait for the worker).

Request body:

```json
{
  "customerName": "Alice",
  "items": [
    { "name": "Keyboard", "quantity": 1, "price": 49.99 },
    { "name": "Mouse", "quantity": 2, "price": 15.00 }
  ],
  "total": 79.99
}
```

#### Linux / macOS (curl)

```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName":"Alice","items":[{"name":"Keyboard","quantity":1,"price":49.99},{"name":"Mouse","quantity":2,"price":15}],"total":79.99}'
```

One-liner that also extracts the new order id:

```bash
curl -s -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName":"Alice","items":[{"name":"Keyboard","quantity":1,"price":49.99}],"total":49.99}' \
  | tee /tmp/order.json
ORDER_ID=$(node -e "console.log(require('/tmp/order.json')._id)")
echo "Order id: $ORDER_ID"
```

#### PowerShell

```powershell
$body = @{
  customerName = "Alice"
  items = @(
    @{ name = "Keyboard"; quantity = 1; price = 49.99 },
    @{ name = "Mouse";    quantity = 2; price = 15.00 }
  )
  total = 79.99
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Uri "http://localhost:3000/orders" `
  -Method Post `
  -ContentType "application/json" `
  -Body $body
```

Capture the id (PowerShell):

```powershell
$order = Invoke-RestMethod -Uri "http://localhost:3000/orders" `
  -Method Post -ContentType "application/json" -Body $body

$order._id
$order.status   # "pending" immediately - worker has NOT processed it yet
$orderId = $order._id
```

> PowerShell 5.1 (Windows PowerShell) sometimes has TLS/encoding quirks with local APIs. If you get an error, run `curl.exe` instead — it ships with Windows 10/11:
> ```powershell
> curl.exe -X POST http://localhost:3000/orders -H "Content-Type: application/json" -d '{\"customerName\":\"Alice\",\"items\":[{\"name\":\"Keyboard\",\"quantity\":1,\"price\":49.99}],\"total\":49.99}'
> ```

**Expected API logs:**
```
Order created: 66f1a2b3c4d5e6f7a8b9c0d1
Publishing order.created
Message published
Response returned
```

**Expected worker logs:**
```
Received order.created -> orderId=66f1a2b3c4d5e6f7a8b9c0d1, customer=Alice
Processing order (simulated slow work, 3 seconds)...
Order confirmation is being processed...
Order processed: 66f1a2b3c4d5e6f7a8b9c0d1 status "pending" -> "processed"
Message acknowledged
```

---

### `GET /orders/:id`

Returns a single order. Use the `_id` from the POST response.

#### Linux / macOS (curl)

```bash
curl http://localhost:3000/orders/66f1a2b3c4d5e6f7a8b9c0d1
```

Pretty-printed:
```bash
curl -s http://localhost:3000/orders/66f1a2b3c4d5e6f7a8b9c0d1 | node -e "let d='';process.stdin.on('data',c=>d+=c).on('end',()=>console.log(JSON.stringify(JSON.parse(d),null,2)))"
```

#### PowerShell

```powershell
Invoke-RestMethod -Uri "http://localhost:3000/orders/$orderId"
```

Or with `curl.exe`:
```powershell
curl.exe http://localhost:3000/orders/$orderId
```

**Responses:**
- `200` — order found
- `400` — invalid order id
- `404` — `{"error":"Order not found"}`

---

### Validation errors (400)

Missing/invalid fields return:

```json
{
  "errors": [
    "customerName is required",
    "items must be a non-empty array",
    "total must be a number >= 0"
  ]
}
```

**curl:**
```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{"items":[],"total":-5}'
```

**PowerShell:**
```powershell
Invoke-RestMethod -Uri "http://localhost:3000/orders" -Method Post `
  -ContentType "application/json" -Body '{"items":[],"total":-5}'
```

---

## Experiments

### Experiment 1 — Basic flow (API → queue → worker → MongoDB)

1. Start API (`npm start`) and worker (`npm run worker`).
2. `POST /orders`.
3. Watch the API print `Order created` → `Publishing order.created` → `Message published` → `Response returned`.
4. Watch the worker print `Received order.created` → 3s delay → `Order processed` → `Message acknowledged`.
5. `GET /orders/:id` → `"status": "processed"`.

### Experiment 2 — Decoupling: worker offline

1. Stop the worker with `Ctrl+C`.
2. POST another order — **the API still returns `201` successfully.**

**Linux:**
```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName":"Bob","items":[{"name":"Mouse","quantity":2,"price":15}],"total":30}'
```

**PowerShell:**
```powershell
$body2 = @{
  customerName = "Bob"
  items = @(@{ name = "Mouse"; quantity = 2; price = 15 })
  total = 30
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Uri "http://localhost:3000/orders" -Method Post `
  -ContentType "application/json" -Body $body2
```

3. Check the order → still `"status": "pending"`.
4. RabbitMQ **retains the message** because no consumer is processing it.
5. Start the worker again (`npm run worker`).
6. It immediately receives the pending message and processes it → `"processed"`.

### Experiment 3 — Acknowledgement behaviour (success vs failure)

**Success → ACK:**
Follow Experiment 1. The worker logs `Message acknowledged` only *after* MongoDB is updated.

**Failure → no ACK → redelivery:**

1. In `.env` set `FAIL_MODE=true`.
2. Restart the worker (`Ctrl+C`, then `npm run worker`).
3. POST an order.

**Linux:**
```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName":"Carol","items":[{"name":"Desk","quantity":1,"price":120}],"total":120}'
```

**PowerShell:**
```powershell
$body3 = @{
  customerName = "Carol"
  items = @(@{ name = "Desk"; quantity = 1; price = 120 })
  total = 120
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Uri "http://localhost:3000/orders" -Method Post `
  -ContentType "application/json" -Body $body3
```

4. Worker output:
```
Processing failed: Simulated failure (FAIL_MODE=true)
Message NOT acknowledged - RabbitMQ will redeliver it
```
5. The message was **never acked**, so RabbitMQ puts it back on the queue and delivers it again. The order stays `"pending"`.
6. Set `FAIL_MODE=false`, restart the worker → the redelivered message succeeds → `Message acknowledged` → `"processed"`.

> This infinite redelivery loop is intentional for learning. Real systems later add retry limits and dead-letter queues — deliberately out of scope here.

---

## Architecture

```
Client
  -> Node.js API
  -> MongoDB          (order saved as "pending")
  -> RabbitMQ         (message queued)
  -> Worker           (3s processing)
  -> MongoDB          (order updated to "processed")
```

### 1. What is a producer?

The code that **sends** messages. Here it is `services/producer.js` inside the API — it calls `channel.sendToQueue('orders', ...)` and moves on. It doesn't know or care who receives the message.

### 2. What is a consumer?

The code that **receives** messages. Here it is `worker.js` — `channel.consume('orders', callback)` registers a function that runs every time a message arrives. The API and worker never talk to each other directly; they both only know about RabbitMQ.

### 3. What is a queue?

A named buffer of messages held by RabbitMQ — here `"orders"`. Messages go in one end and wait until someone consumes them. It's a mailbox: the sender drops a letter, the receiver picks it up whenever they check.

### 4. Why is RabbitMQ between the API and the worker?

It separates the two so neither depends on the other. The API's only job is "accept order, publish event". The worker's only job is "process event". Each can crash, restart, or deploy independently — the queue absorbs the gap. Without it, the API would have to call the worker over HTTP and would break whenever the worker was down.

### 5. Why doesn't the API wait for the worker?

Because `sendToQueue` returns immediately — writing to a queue takes milliseconds, while the worker takes 3+ seconds. Waiting would tie up the API, block the client, and couple response time to worker speed. The API's contract is *"order received"*, not *"order processed"*. Clients check `GET /orders/:id` later to see `processed` — that is asynchronous processing.

### 6. What does ACK mean?

Acknowledgement. With manual acks (`noAck: false`), RabbitMQ hands a message to the worker but keeps it marked as unacknowledged. Only when the worker calls `channel.ack(msg)` does RabbitMQ delete it. If the worker never acks — because it crashed or called `nack` — RabbitMQ still owns the message and will deliver it again. It's like signing for a package: until you sign, the courier is still responsible for it.

### 7. What happens when the consumer is offline?

RabbitMQ keeps the message in the queue (that's Experiment 2). The API still returns `201` — publishing doesn't require a live consumer. When the worker starts again, it drains the backlog. This is the core benefit: producer and consumer never need to be running at the same time.

### 8. What happens when processing fails?

The worker throws **before** calling `ack`. It calls `channel.nack(msg, false, true)` — `requeue: true` — so RabbitMQ puts the message back and redelivers it. Nothing is lost. If we had acked first and then failed, the message would be gone forever. That's the rule: **ack only after successful processing.**

### 9. How is this different from Redis caching?

| | RabbitMQ (this project) | Redis cache |
|---|---|---|
| Goal | Move work between processes | Store data for fast reads |
| Message lives | Until consumed + acked | Until TTL / eviction |
| Loss impact | Work never happens | Just a cache miss, DB rebuilds it |
| Direction | Producer → queue → consumer | App ↔ cache ↔ DB |
| Semantics | Delivery guarantee | Best-effort speed layer |

A cache says *"read this value quickly"*; a queue says *"do this work later, reliably"*. Caching reduces database reads; queuing moves slow work **out** of the request path.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `ECONNREFUSED` on 5672 | RabbitMQ isn't running — start the service |
| `MONGODB_URI is missing` | Create `.env` from `.env.example`, add your Atlas URI |
| `Authentication failed` | Wrong Atlas user/password, or IP not allowlisted in Atlas |
| Worker never receives messages | Worker must be running; check it printed `Waiting for messages` |
| Order stuck at `pending` | Worker crashed or `FAIL_MODE=true` — check worker logs |
| `EADDRINUSE` on 3000 | Another process uses port 3000 — change `PORT` in `.env` |
