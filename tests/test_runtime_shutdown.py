import importlib
import logging
import signal


def test_sigterm_requests_graceful_shutdown_without_ignore_warning():
    import runtime_shutdown_guard

    mod = importlib.reload(runtime_shutdown_guard)
    logger = logging.getLogger("test.runtime_shutdown")
    received = []
    previous = signal.getsignal(signal.SIGTERM)
    try:
        mod.install_graceful_shutdown_handler(
            logger,
            "test-runtime",
            lambda signal_name: received.append(signal_name),
            signals_to_handle=[signal.SIGTERM],
        )
        signal.raise_signal(signal.SIGTERM)
        assert mod.shutdown_requested() is True
        assert received == ["SIGTERM"]
    finally:
        signal.signal(signal.SIGTERM, previous)


def test_multi_asset_external_shutdown_is_non_blocking_and_wakes_loop():
    from orchestration.multi_asset_bot import MultiAssetInstitutionalBot

    bot = MultiAssetInstitutionalBot()
    bot.running = True
    bot.request_external_shutdown("SIGTERM")

    assert bot.running is False
    assert bot._external_shutdown_requested.is_set()
    assert bot._market_wakeup.is_set()
