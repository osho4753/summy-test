# Symmy Task — Highload ERP to E-shop Integration

A robust, production-ready Django application for synchronizing large-scale ERP data to an e-shop via Celery. Designed with high-load principles, memory efficiency, and strict data validation in mind.

## 🚀 Key Architectural Features

- **Atomic Task Processing (Anti-Poison Pill)** — Adopts a strict "1 task = 1 product" pattern. Long-running blocking batches are eliminated. If a network drop or 429 error occurs, only the affected product is retried, ensuring zero state loss and preventing endless retry loops.
- **Proactive Rate Limiting** — Instead of reactively spamming the API until a `429 Too Many Requests` is hit, the system utilizes Celery's native Token Bucket algorithm (`rate_limit='5/s'`). Requests are smoothly dripped to the external API, naturally preventing bans and database write spikes (max 5 TPS).
- **Memory-Efficient Stream Parsing** — Uses `ijson` to read massive JSON dumps iteratively. Eliminates Out-Of-Memory (OOM) risks regardless of the ERP file size.
- **Declarative Data Validation** — Powered by `Pydantic`. Raw inputs are never mutated (no dirty hacks). Business logic (VAT calculation, stock aggregation) is cleanly encapsulated using `@computed_field` and `@field_validator`.
- **Optimized Delta Sync (Bulk Read)** — The orchestrator buffers hashes and queries the database in chunks (Bulk Read) to prevent N+1 queries, dispatching Celery tasks _only_ for genuinely new or modified products.
- **Safe Prefork Networking** — HTTP Sessions are securely initialized per-worker-process via Celery signals (`worker_process_init`) to prevent socket corruption.

## 🛠 Tech Stack

- Python 3.11+
- Django 5.2
- Celery + Redis
- PostgreSQL
- **Pydantic** (Validation & Transformation)
- **ijson** (Stream Parsing)
- Docker & Docker Compose
- pytest & responses

## 🚦 Quick Start

### 1. Clone and Run

```bash
git clone <repo-url>
cd symmy-task
docker-compose up -d --build
```

````

### 2. Apply Migrations

```bash
docker-compose exec web python manage.py migrate

```

### 3. Run Sync Task

The background worker starts automatically. Trigger the orchestrator task from the Django shell:

```bash
docker-compose exec web python manage.py shell

>>> from integrator.tasks import sync_erp_to_eshop
>>> sync_erp_to_eshop.delay()
# The orchestrator will instantly dispatch atomic tasks (sync_single_product) to the broker

```

## 📁 Project Structure

```text
symmy-task/
├── core/                    # Django project configuration
├── integrator/              # Integration app
│   ├── models.py            # ProductSyncState (Delta Sync)
│   ├── schemas.py           # Pydantic models (Declarative @computed_fields)
│   ├── services.py          # ijson stream parsing generator
│   ├── tasks.py             # Celery tasks (Orchestrator & Atomic `sync_single_product`)
│   ├── tests.py             # Pytest suite with mock responses
│   └── admin.py             # Admin panel
├── erp_data.json            # ERP test data
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── pytest.ini

```

## 🔄 Transformation Logic (Pydantic Schema)

- **Price:** `price_vat_incl = round(price_vat_excl * 1.21, 2)`. Null, missing, or negative values automatically default to `0.00`.
- **Stock:** `stock_total = sum(valid_numeric_stocks)`. Invalid values (e.g., `"N/A"`, booleans) are safely ignored.
- **Color:** Safely extracted from nested `attributes`. Defaults to `"N/A"` if missing.

## 🌐 E-shop API Configuration

| Scenario       | Method | URL                                            |
| -------------- | ------ | ---------------------------------------------- |
| New product    | POST   | `https://api.fake-eshop.cz/v1/products/`       |
| Update product | PATCH  | `https://api.fake-eshop.cz/v1/products/{sku}/` |

Authentication: `{"X-Api-Key": "symma-secret-token"}`

## 🧪 Testing

The project includes a robust testing suite focusing on schema validation, chunking, and Celery retries.

```bash
# Run tests with verbosity
docker-compose exec web pytest integrator/tests.py -v

```

**Test Coverage Highlights:**

- `Pydantic` schema transformation and edge-case handling.
- Batched stream parsing deduplication.
- API mocking via `responses` for successful POST/PATCH batch syncs.
- Retry mechanisms and Rate Limit handling verification.

## 🔧 Environment Variables

| Variable             | Default Value                           |
| -------------------- | --------------------------------------- |
| `CELERY_BROKER_URL`  | `redis://redis:6379/0`                  |
| `ESHOP_API_BASE_URL` | `https://api.fake-eshop.cz/v1/products` |
| `ESHOP_API_KEY`      | _Empty string_                          |
````
