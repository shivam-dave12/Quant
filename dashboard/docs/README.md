# Local Portfolio Command Center

This package gives you a local, mobile-friendly dashboard so you can monitor the bot visually instead of watching logs.

## What it does

- Runs on your laptop as a local web server.
- Opens in any browser on your laptop or phone.
- Works on your phone when the laptop and phone are on the same Wi-Fi.
- Shows system status, live positions, scanner desks, alerts, trade tape, and event tape.
- Can be fed either by direct bot events or by tailing your existing JSON log.

## Quick start

### 1) Install backend requirements

```bash
cd backend
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2) Start the dashboard

```bash
cd backend
python run.py
```

Open this on your laptop:

```text
http://127.0.0.1:8000
```

To open it on your phone, use your laptop's local IP, for example:

```text
http://192.168.1.23:8000
```

Use the same Wi-Fi network on both devices.

## Feed the dashboard from your current bot logs

In a second terminal:

```bash
python adapters/log_tail.py --log /path/to/your-bot-json.log --dashboard http://127.0.0.1:8000
```

That will tail the JSON log and keep the dashboard updated.

## Better integration: direct event emission

Use `adapters/emitter.py` inside your bot and publish structured events directly. Example:

```python
from adapters.emitter import DashboardEmitter

emitter = DashboardEmitter("http://127.0.0.1:8000")
emitter.heartbeat(mode="live", environment="local", max_positions=4)
emitter.scan_update(asset="BTC", venue="DELTA", symbol="BTCUSD", phase="SCANNING", price=118500)
emitter.position_opened(asset="BTC", venue="DELTA", symbol="BTCUSD", side="LONG", entry=118500, sl=117900, tp=119800, qty=0.01)
emitter.alert(asset="BTC", venue="DELTA", symbol="BTCUSD", severity="critical", title="Protection failure", message="Bracket child missing")
```

## Recommended next step

For production-quality monitoring, wire the emitter into these moments in your bot:

- heartbeat / portfolio summary loop
- scanner update loop
- candidate approved / deferred
- position opened / updated / closed
- protection failure / execution failure / data warning

That will be much more accurate than parsing text logs.
