gendocs: docs/generated/output README.md

gendocs-auto:
	while true; do \
		make gendocs; \
		inotifywait -e modify **/*.py; \
	done

docs/generated/output: docs/generated/getting_started.py docs/generated/_docgen.py
	cd docs/generated && uv run python _docgen.py getting_started.py

README.md: docs/generated/README.md examples/readme.py docs/generated/_genreadme.py
	cd docs/generated && uv run python _genreadme.py

.PHONY: gendocs gendocs-auto
