# Symmy Task

A robust Django application for synchronizing large-scale ERP data to an e-shop via Celery.

## Key Architectural Features

- **Non-blocking Rate Limiting** — API rate limits (5 req/s) and `429 Too Many Requests` are handled natively via Celery's `rate_limit` and non-blocking `self.retry()`.
- **Zero N+1 Database Writes (Celery Chords)** — Writing synchronization states to the database one by one would crush the DB under high load. This system uses `celery.chord` to dispatch a batch of atomic HTTP tasks, gather their successful results, and write them to PostgreSQL in a single `bulk_create`/`bulk_update` query.
- **Smart API Fallback (POST -> PATCH)** — Local database state is treated as a cache, not the absolute source of truth. If a `POST` request fails with a `409 Conflict` (or `400` duplicate error), the API Client automatically falls back to a `PATCH` request, making the synchronization highly resilient to state mismatches.
- **Class-Based Task Connection Pooling** — HTTP Sessions are preserved across task executions to reduce socket overhead. Instead of using anti-patterns like global variables and signal hooks, this is cleanly achieved using Custom Class-Based Celery Tasks (`celery.Task` inheritance).
- **Memory-Efficient Stream Parsing** — Uses `ijson` to read massive JSON dumps iteratively. Eliminates Out-Of-Memory (OOM) risks regardless of the ERP file size.
- **Declarative Data Validation** — Powered by `Pydantic`. Raw inputs are never mutated. Business logic is cleanly encapsulated, and magic numbers (like VAT) are moved to environment variables.

## 🛠 Tech Stack

- Python 3.11+
- Django 5.2
- Celery + Redis (Message Broker & Result Backend for Chords)
- PostgreSQL
- **Pydantic** (Validation & Transformation)
- **ijson** (Stream Parsing)
- Docker & Docker Compose
- pytest & responses

## Quick Start

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

```

## Project Structure

```text
symmy-task/
├── core/                    # Django project configuration
├── integrator/              # Integration app
│   ├── models.py            # ProductSyncState (Delta Sync)
│   ├── schemas.py           # Pydantic models (Declarative validation)
│   ├── services.py          # ijson stream parsing generator
│   ├── tasks.py             # Celery tasks (Orchestrator, Atomic Sync, Bulk Save)
│   ├── api_client.py        # Smart HTTP Client with Fallback logic
│   ├── tests.py             # Pytest suite with mock responses
│   └── admin.py             # Admin panel
├── erp_data.json            # ERP test data
├── docker-compose.yml
├── Dockerfile
└── requirements.txt

```

## Transformation Logic

- **Price:** `price_vat_incl = price_vat_excl * VAT_MULTIPLIER`. Null, missing, or negative values automatically default to `0.00`. The VAT multiplier is configurable via `.env`.
- **Stock:** `stock_total = sum(valid_numeric_stocks)`. Invalid values (e.g., `"N/A"`, booleans) are safely ignored.
- **Color:** Safely extracted from nested `attributes`. Defaults to `"N/A"` if missing.

## E-shop API Configuration

| Scenario       | Method | URL                                            |
| -------------- | ------ | ---------------------------------------------- |
| New product    | POST   | `https://api.fake-eshop.cz/v1/products/`       |
| Update product | PATCH  | `https://api.fake-eshop.cz/v1/products/{sku}/` |

Authentication: `{"X-Api-Key": "symma-secret-token"}`

## Testing

The project includes a robust testing suite focusing on schema validation, high-load architecture (bulk database writes), and API resilience (Fallback & Rate Limiting).

```bash
# Run tests with verbosity
docker-compose exec web pytest integrator/tests.py -v

```

## Environment Variables

| Variable             | Default Value                           | Description                           |
| -------------------- | --------------------------------------- | ------------------------------------- |
| `CELERY_BROKER_URL`  | `redis://redis:6379/0`                  | Redis connection string               |
| `ESHOP_API_BASE_URL` | `https://api.fake-eshop.cz/v1/products` | Target e-shop API endpoint            |
| `ESHOP_API_KEY`      | _Empty string_                          | Secret token for API auth             |
| `VAT_MULTIPLIER`     | `1.21`                                  | Configurable VAT rate (e.g., 21% tax) |

```

```
````
