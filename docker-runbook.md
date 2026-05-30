# Docker runbook

This image keeps all non-secret settings in `btchft/config.py`. The `.env` file must contain only:

```bash
DELTA_API_KEY=...
DELTA_SECRET_KEY=...
```

## Build

```bash
docker build -t btc-live-hft:v5.1 .
```

## Run with persistent live artefacts

```bash
mkdir -p artifacts/live

docker run --rm \
  --name btc-live-hft \
  --env-file .env \
  -v "$(pwd)/artifacts/live:/app/artifacts/live" \
  btc-live-hft:v5.1
```

Because the Dockerfile sets `ENTRYPOINT ["btc-live-hft"]` and `CMD ["run"]`, the command above runs the bot.

## Run an explicit command

```bash
docker run --rm --env-file .env \
  -v "$(pwd)/artifacts/live:/app/artifacts/live" \
  btc-live-hft:v5.1 run
```

## Inspect a trade-flow file inside the container

```bash
docker run --rm \
  -v "$(pwd)/data:/data:ro" \
  -v "$(pwd)/artifacts/live:/app/artifacts/live" \
  btc-live-hft:v5.1 inspect-tradeflow /data/futures-trades-monthly-BTCUSD-2026-05.csv.zip
```

## Train bootstrap model inside the container

```bash
docker run --rm \
  -v "$(pwd)/data:/data:ro" \
  -v "$(pwd)/artifacts:/app/artifacts" \
  btc-live-hft:v5.1 train-bootstrap-tradeflow /data/futures-trades-monthly-BTCUSD-2026-05.csv.zip
```
