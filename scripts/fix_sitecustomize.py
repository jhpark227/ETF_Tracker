"""Write sitecustomize.py into the venv to fix macOS hidden-flag bug."""
from pathlib import Path

root = Path(__file__).parent.parent.resolve()
sc = root / ".venv/lib/python3.14/site-packages/sitecustomize.py"

sc.write_text(
    f"import sys\n"
    f"if {str(root)!r} not in sys.path:\n"
    f"    sys.path.insert(0, {str(root)!r})\n"
)
print(f"sitecustomize.py → {sc}")
