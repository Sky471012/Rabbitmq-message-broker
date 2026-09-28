# RabbitMQ Order Processing — Learning Project

A small Node.js project that demonstrates the fundamental **producer → queue → consumer** flow using RabbitMQ, with an e-commerce order-processing example.

**Stack:** Node.js, Express, MongoDB Atlas, Mongoose, RabbitMQ, JavaScript (no TypeScript).

## What this teaches

- Producer / consumer architecture
- Asynchronous processing (API returns before work finishes)
- Queues and message persistence
- Manual acknowledgements (`ack` / `nack`)
- Decoupling the API from the worker
- Competing consumers (multiple workers on one queue)
- Retry limits and a Dead Letter Queue (`orders.dlq`)

**Not included (on purpose):** exchanges, routing keys, retry libraries, exponential backoff, auth, frontend, Docker, Redis, Kafka.

---

## Project structure

```
messageBroker-rabbitmq/
├── server.js                 # Express API (producer side)
├── worker.js                 # RabbitMQ consumer (run it 1, 2 or 3 times)
├── dlqConsumer.js            # Reads and logs dead-lettered messages
├── config/
│   ├── db.js                 # Mongoose connection
│   └── rabbitmq.js           # RabbitMQ connection + "orders" + "orders.dlq"
├── models/
│   └── Order.js              # Order schema
├── routes/
│   └── orderRoutes.js        # POST /orders, GET /orders/:id
├── controllers/
│   └── orderController.js    # validation + response handling
├── services/
│   ├── orderService.js       # MongoDB reads/writes
│   └── producer.js           # publishes order.created (retryCount: 0)
├── package.json
├── .env.example
└── .gitignore
```

---

## Setup

### 1. Install packages

```bash
npm install express mongoose amqplib dotenv
```

### 2. Create your `.env`

```bash
cp .env.example .env
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

You need **several terminals**.

**Terminal A — API:**
```bash
npm start
```

**Terminals B, C, D — Workers (optional but recommended):**
```bash
npm run worker      # terminal B
npm run worker      # terminal C
npm run worker      # terminal D
```

Every worker connects to the **same** `orders` queue — that is the whole point of competing consumers.

Expected logs:
```
API:       MongoDB connected
           RabbitMQ connected. Queues "orders" and "orders.dlq" are ready
           API server listening on http://localhost:3000

Worker 1:  MongoDB connected
           RabbitMQ connected. Queues "orders" and "orders.dlq" are ready
           [Worker 21344] Worker started. Waiting for messages on queue "orders"...

Worker 2:  [Worker 21402] Worker started. Waiting for messages on queue "orders"...
```

Each worker logs its own process id (`[Worker 21344]`, `[Worker 21402]`, ...) so you can see which one got each message.

**Optional — DLQ consumer (start only when you want to inspect failures):**
```bash
npm run dlq
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
[Worker 21344] Received order.created -> orderId=66f1a2b3c4d5e6f7a8b9c0d1, customer=Alice, attempt=1/3
[Worker 21344] Processing order (simulated slow work, 3 seconds)...
[Worker 21344] Order confirmation is being processed...
[Worker 21344] Order processed: 66f1a2b3c4d5e6f7a8b9c0d1 status "pending" -> "processed"
[Worker 21344] Message acknowledged
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

### Experiment 3 — Competing consumers (multiple workers)

1. Start **3 workers** in three terminals: `npm run worker` ×3.
2. POST 3 orders quickly:

**Linux:**
```bash
for i in 1 2 3; do
  curl -X POST http://localhost:3000/orders \
    -H "Content-Type: application/json" \
    -d "{\"customerName\":\"Customer $i\",\"items\":[{\"name\":\"Item $i\",\"quantity\":1,\"price\":10}],\"total\":10}"
done
```

**PowerShell:**
```powershell
1..3 | ForEach-Object {
  $b = @{
    customerName = "Customer $_"
    items = @(@{ name = "Item $_"; quantity = 1; price = 10 })
    total = 10
  } | ConvertTo-Json -Depth 5
  Invoke-RestMethod -Uri "http://localhost:3000/orders" -Method Post `
    -ContentType "application/json" -Body $b | Out-Null
}
```

3. Watch the three terminals: the messages are spread across the workers — RabbitMQ delivers each message to **only one** consumer.

### Experiment 4 — Retry limit + Dead Letter Queue

**Success → ACK:**
Follow Experiment 1. The worker logs `Message acknowledged` only *after* MongoDB is updated.

**Failure → 3 attempts → `orders.dlq`:**

1. In `.env` set `FAIL_MODE=true`.
2. Restart **all** workers (`Ctrl+C`, then `npm run worker`).
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

4. Worker output (note: **any** worker may pick up each retry — that is expected):

```
[Worker 21344] Received order.created -> orderId=... attempt=1/3
[Worker 21344] Processing order (simulated slow work, 3 seconds)...
[Worker 21344] Order confirmation is being processed...
[Worker 21344] Processing failed for order ...: Simulated failure (FAIL_MODE=true)
[Worker 21344] Retry attempt 1/3 - requeueing message on "orders"
[Worker 21344] Message requeued for another attempt

[Worker 21402] Received order.created -> orderId=... attempt=2/3
[Worker 21402] Processing failed for order ...: Simulated failure (FAIL_MODE=true)
[Worker 21402] Retry attempt 2/3 - requeueing message on "orders"
[Worker 21402] Message requeued for another attempt

[Worker 21480] Received order.created -> orderId=... attempt=3/3
[Worker 21480] Processing failed for order ...: Simulated failure (FAIL_MODE=true)
[Worker 21480] Max retries reached for order ...
[Worker 21480] Sending message to DLQ
[Worker 21480] Message moved to orders.dlq
```

5. The order in MongoDB stays `"status": "pending"` — nothing was processed, but the message did not vanish.

**Inspect the DLQ:**

Start the DLQ consumer in another terminal:
```bash
npm run dlq
```

```
DLQ consumer started. Waiting for messages on "orders.dlq"...
DLQ message received
orderId: 66f1...
retryCount: 3
reason: Simulated failure (FAIL_MODE=true)
customerName: Carol
```

You can also check the queue without consuming it:
```bash
rabbitmqctl list_queues name messages        # Linux
rabbitmqctl list_queues name messages        # PowerShell
```

`orders.dlq  1` means one dead-lettered message is waiting.

6. Set `FAIL_MODE=false` and restart the workers — new orders succeed again.

> Note: `npm run dlq` **removes** messages from the DLQ as it logs them (it acks them). Start it only when you want to drain the DLQ.

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

The worker throws **before** calling `ack`, so RabbitMQ still owns the message. Then `handleFailure()` in `worker.js` decides:

- **Attempt 1 or 2 failed** → the worker publishes a *new copy* of the message with `retryCount + 1` back onto `orders`, then acks the old copy. Another worker (maybe a different one) picks it up next.
- **Attempt 3 failed** → the worker publishes the message to `orders.dlq`, and only **after that publish succeeds** does it ack the original. If the DLQ publish fails, nothing is acked and RabbitMQ redelivers.

The rule stays the same: **never ack a message before its outcome is safely stored** — either the work is done in MongoDB, or the message is parked in the DLQ.

### 9. Why not just `nack(msg, false, true)` like before?

Because a requeued message comes back **unchanged**. `retryCount` would stay `0` forever and the message would bounce between workers infinitely. To count attempts, the worker must publish a *modified* copy (that carries the new `retryCount`) and then remove the original with an ack. The `retryCount` lives in the message body, so it travels with the message to whichever worker gets it next.

### 10. What is a Dead Letter Queue?

A normal queue used as a "parking lot" for messages that can't be processed. Nothing special is configured — the worker just publishes to `orders.dlq` by name when the retry limit is reached. The value is that the failed message keeps all its data (`orderId`, `items`, `total`, `retryCount`, `error`) instead of being dropped, so you can inspect it later or reprocess it manually.

---

## Message flow at a glance

```
POST /orders
     |
     v
  orders  (retryCount: 0)
     |
     +--> W1 / W2 / W3   (whichever is free - competing consumers)
     |
     +-- success --> ack --> MongoDB: status = processed   [DONE]

     +-- fail 1 --> retryCount 1 --> back on orders
     +-- fail 2 --> retryCount 2 --> back on orders
     +-- fail 3 --> publish to orders.dlq --> ack   [DEAD LETTERED]
                                                          |
                                                          v
                                                    dlqConsumer.js (logs it)
```
