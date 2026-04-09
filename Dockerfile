FROM python:3.11-slim

WORKDIR /app

# Install Node.js 20 and supervisord
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl supervisor openssl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# ── Python (FastAPI Dashboard) ──
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/ ./api/
COPY scraper/ ./scraper/
COPY static/ ./static/

RUN mkdir -p data

# ── Node.js (Shopify Remix App) ──
COPY shopify-app/ ./shopify-app/
WORKDIR /app/shopify-app
RUN npm ci --omit=dev 2>/dev/null || npm install --omit=dev
RUN DATABASE_URL="file:/app/shopify-app/prisma/dev.sqlite" npx prisma generate
RUN DATABASE_URL="file:/app/shopify-app/prisma/dev.sqlite" npx prisma db push --accept-data-loss
RUN npm run build

# ── Supervisord Config ──
WORKDIR /app
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

EXPOSE 8000

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
