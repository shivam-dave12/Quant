from pathlib import Path


def test_install_script_creates_tail_service():
    script = Path('scripts/install_dashboard_aws.sh').read_text()
    assert 'trading-dashboard-tail.service' in script
    assert 'log_tail_agent.py' in script
    assert '--from-start' in script
    assert '{{.LogPath}}' in script


def test_log_parser_extracts_catalog_and_scan():
    import importlib.util
    agent_path = Path('dashboard/agents/log_tail_agent.py')
    spec = importlib.util.spec_from_file_location('log_tail_agent', agent_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)

    catalog = '19:47:45.989 | INFO | • SYSTEM |      ✅ BTC      primary=delta      delta:BTCUSD'
    ev = mod.parse(catalog)
    assert ev and ev['type'] == 'catalog_asset'
    assert ev['asset'] == 'BTC'
    assert ev['symbol'] == 'BTCUSD'

    scan = '[BTC|DELTA:BTCUSD] BTC DELTA BTCUSD | price 100000.00 | SCANNING | open=0/4'
    ev = mod.parse(scan)
    assert ev and ev['type'] == 'scan'
    assert ev['asset'] == 'BTC'
    assert ev['price'] == 100000.0
