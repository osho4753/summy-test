# Symmy Task — ERP to E-shop Integration

Django application for synchronizing ERP data to e-shop via Celery.

## Features

- **Delta Sync** — sends only changed data (based on SHA-256 hash)
- **Data Transformation** — automatic VAT calculation (21%), stock aggregation
- **Edge Case Handling** — null values, duplicates, invalid data
- **Rate Limiting** — automatic retry on 429 response

## Tech Stack

- Python 3.11+
- Django 5.2
- Celery + Redis
- PostgreSQL
- Docker & Docker Compose
- pytest

## Quick Start

### 1. Clone and Run

```bash
git clone <repo-url>
cd symmy-task
docker-compose up -d --build
```

### 2. Apply Migrations

```bash
docker-compose exec web python manage.py migrate
```

### 3. Start Celery Worker

```bash
docker-compose exec web celery -A core worker --loglevel=info
```

### 4. Run Sync Task

```python
# Django shell
docker-compose exec web python manage.py shell

>>> from integrator.tasks import sync_erp_to_eshop
>>> sync_erp_to_eshop.delay()  # Async
# or
>>> sync_erp_to_eshop()  # Sync
```

## Project Structure

```
symmy-task/
├── core/                    # Django project
│   ├── celery.py            # Celery configuration
│   ├── settings.py          # Django settings
│   └── ...
├── integrator/              # Integration app
│   ├── models.py            # ProductSyncState (Delta Sync)
│   ├── services.py          # Transformation business logic
│   ├── tasks.py             # Celery task sync_erp_to_eshop
│   ├── tests.py             # Tests (pytest + responses)
│   └── admin.py             # Admin panel
├── erp_data.json            # ERP test data
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── pytest.ini
```

## ERP Data (erp_data.json)

File contains test data with edge cases:

| SKU     | Edge Case                      |
| ------- | ------------------------------ |
| SKU-001 | Valid reference record         |
| SKU-002 | Negative price → 0             |
| SKU-003 | `attributes: null` → color N/A |
| SKU-004 | `price_vat_excl: null` → 0     |
| SKU-006 | Duplicate (deduplication)      |
| SKU-008 | `stocks.praha: "N/A"` → 0      |

## Transformation Logic

- **Price:** `price_vat_incl = round(price_vat_excl * 1.21, 2)` (null/negative → 0)
- **Stock:** `stock_total = sum(stocks.values())` (invalid values → 0)
- **Color:** `color = attributes.color` or `"N/A"`

## E-shop API

| Scenario       | Method | URL                                            |
| -------------- | ------ | ---------------------------------------------- |
| New product    | POST   | `https://api.fake-eshop.cz/v1/products/`       |
| Update product | PATCH  | `https://api.fake-eshop.cz/v1/products/{sku}/` |

Headers: `{"X-Api-Key": "symma-secret-token"}`

## Testing

```bash
docker-compose exec web pytest integrator/tests.py -v
```

### Test Coverage

- Unit tests for transformation (VAT, stocks, color)
- Integration tests for Celery task
- API mocking via `responses`
- Rate limiting test (429 → retry)

## Environment Variables

| Variable                 | Default Value          |
| ------------------------ | ---------------------- |
| `CELERY_BROKER_URL`      | `redis://redis:6379/0` |
| `DJANGO_SETTINGS_MODULE` | `core.settings`        |

## License

MIT
