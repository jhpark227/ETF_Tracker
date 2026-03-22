SITE = .venv/lib/python3.14/site-packages

.PHONY: sync

sync:
	uv sync
	@python3 scripts/fix_sitecustomize.py
