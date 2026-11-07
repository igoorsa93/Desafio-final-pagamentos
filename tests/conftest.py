"""Pytest Configuration"""
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Pytest asyncio
pytest_plugins = ["pytest_asyncio"]

# Test constants
TEST_REDIS_URL = "redis://localhost:6379/1"
TEST_KAFKA_BOOTSTRAP = "localhost:9093"
TEST_SCHEMA_REGISTRY = "http://localhost:8082"
