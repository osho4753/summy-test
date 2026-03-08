# Symmy Task — Highload ERP to E-shop Integration

A robust, production-ready Django application for synchronizing large-scale ERP data to an e-shop via Celery. Designed with high-load principles, memory efficiency, and strict data validation in mind.

## 🚀 Key Architectural Features

- **Memory-Efficient Processing (Stream Parsing)** — Uses `ijson` to read massive JSON files in chunks. Eliminates Out-Of-Memory (OOM) risks regardless of the ERP dump size.
- **Strict Data Validation** — Powered by `Pydantic`. Ensures bulletproof type coercion, handles edge cases (e.g., negative prices, missing attributes), and encapsulates business logic.
- **Optimized Database I/O** — Drastically reduces database load by using batched `bulk_create` with conflict resolution (`update_conflicts=True`) instead of thousands of isolated transactions.
- **Smart Rate Limiting** — Advanced 429 Error handling. Respects external API limits by dynamically reading `Retry-After` headers and utilizing **Exponential Backoff with Jitter** to prevent "Thundering Herd" DDoS scenarios.
- **Delta Sync** — Sends only changed data based on SHA-256 hashing to minimize network overhead.
- **Safe Prefork Networking** — HTTP Sessions are securely initialized per-worker-process via Celery signals to prevent socket corruption.

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

The background worker starts automatically via Docker Compose. Trigger the orchestrator task from the Django shell:

```bash
docker-compose exec web python manage.py shell

>>> from integrator.tasks import sync_erp_to_eshop
>>> sync_erp_to_eshop.delay()

```

## 📁 Project Structure

```text
symmy-task/
├── core/                    # Django project configuration
├── integrator/              # Integration app
│   ├── models.py            # ProductSyncState (Delta Sync)
│   ├── schemas.py           # Pydantic schemas (Validation & Transformation)
│   ├── services.py          # Stream parsing logic (ijson chunks)
│   ├── tasks.py             # Celery tasks (Orchestrator & Batch Processing)
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
