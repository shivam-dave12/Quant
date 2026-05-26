"""Runtime configuration facade. Policy is defined in code; .env is secrets only."""
from core.config import CONFIG, PlatformConfig, PlatformPolicy, Secrets
__all__ = ["CONFIG", "PlatformConfig", "PlatformPolicy", "Secrets"]
