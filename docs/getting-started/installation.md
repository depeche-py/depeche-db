# Installation

Install from PyPI using your favorite package manager

```bash
pip install depeche-db psycopg2-binary
# OR
poetry add depeche-db psycopg2-binary
```


## Optional: Run a PostgreSQL database


```yaml
# docker-compose.yml
version: '3'

services:
  db_dev:
    image: 'postgres:14.5'
    environment:
      POSTGRES_USER: demo
      POSTGRES_PASSWORD: demo
      POSTGRES_DB: demo
    ports:
      - 4888:5432
    restart: unless-stopped
```

```bash
docker compose up -d
```
