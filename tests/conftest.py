# Make the repository root importable during tests
import sys
from pathlib import Path

# tests/ is one level below the repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
