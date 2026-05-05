# Dashboard V2 Sidecar — no strategy/core changes

This dashboard is intentionally a sidecar. It does not import or mutate strategy, execution, risk, or market aggregator code.

## Run locally inside the bot folder

```bash
cd dashboard_v2/backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run.py
```

Open `http://127.0.0.1:8000`.

## Feed from Docker log

```bash
python dashboard_v2/agents/log_tail_agent.py --log /var/lib/docker/containers/<id>/<id>-json.log --dashboard http://127.0.0.1:8000 --from-start
```

## AWS install

```bash
cd /path/to/bot/root
BOT_CONTAINER=<your_bot_container_name_or_id> SERVICE_USER=$USER BOT_ROOT=$(pwd) ./scripts/install_dashboard_v2_sidecar_aws.sh
```

## Laptop access

```bash
ssh -i /path/to/key.pem -N -L 8000:127.0.0.1:8000 ec2-user@YOUR_EC2_IP
```

Open `http://127.0.0.1:8000`.

## Mobile access

Use Tailscale/VPN on EC2 and mobile, then open `http://EC2_TAILSCALE_IP:8000`.

Do not expose port 8000 publicly without auth.
