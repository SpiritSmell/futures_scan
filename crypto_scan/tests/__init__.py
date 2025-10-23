"""
Test suite for Crypto Futures Price Collector v5
Comprehensive testing framework with unit, integration, and performance tests.
"""

import sys
import os
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "packages"))
