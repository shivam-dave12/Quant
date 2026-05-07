from execution.instrument_registry import InstrumentRegistry


class FakeDelta:
    def get_products(self, contract_types=None):
        return {"success": True, "result": [{"symbol": "BTCUSD", "state": "live", "contract_type": "perpetual_futures", "tick_size": 0.5, "contract_value": 0.001, "max_leverage": 40}]}


def test_delta_only_registry_matches_btc():
    reg = InstrumentRegistry()
    report = reg.discover(delta_api=FakeDelta(), requested=[{"asset_id":"BTC","display_name":"Bitcoin","asset_class":"crypto","aliases":["BTCUSD"],"priority":0}])
    assert len(report.matched) == 1
    btc = report.matched[0]
    assert btc.primary_exchange.value == "delta"
    assert btc.primary.symbol == "BTCUSD"
