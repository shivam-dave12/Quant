# -*- coding: utf-8 -*-
"""Backward-compatible import shim.

Live code imports strategy.display_engine. This module stays only so any
external command/plugin that still imports strategy.v9_display does not break.
"""
from strategy.display_engine import *  # noqa: F401,F403
