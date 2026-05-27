# Institutional Multi-Asset Bot — v2.11 Config-Controlled Runtime

## One control plane

All trading/runtime policy is in `config.py`. `.env` is secrets only.

### Shadow mode, all data feeds analysed, no orders

```python
LIVE_TRADING_ENABLED = False
ANALYSIS_DATA_VENUES = ("delta", "coinswitch", "groww")
LIVE_EXECUTION_VENUES = ("groww",)
```

### Enable first live run for Groww/NIFTY only

Edit `config.py`:

```python
LIVE_TRADING_ENABLED = True
LIVE_EXECUTION_VENUES = ("groww",)
GROWW_APPROVED_STATIC_IPS = ("<YOUR_GROWW-WHITELISTED_EC2_ELASTIC_IP>",)
GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED = True  # only after broker confirmation
```

Delta still runs for analysis but cannot place an order while it is absent from `LIVE_EXECUTION_VENUES`.

### Later: explicitly allow Delta execution

```python
LIVE_EXECUTION_VENUES = ("groww", "delta")
```

Do this only after accepting Delta shadow telemetry/forward-markout evidence.

## Secrets file

Create `.env` from `.env.example` and fill credentials only. Do not add flags, venues, IP policy, risk values or model thresholds.

## Deploy

```bash
cd ~/quant/Quant
rm -f .env
podman build --no-cache -t localhost/quant:latest .
systemctl --user restart quant.service
podman logs -f quant
```

The systemd unit should load your external credentials file through `--env-file`.

## Telegram

`/thinking` and `/status` remain read-only observability commands. `/set` and `/setexchange` no longer mutate runtime policy; edit `config.py` and restart instead.
