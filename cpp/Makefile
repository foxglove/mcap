default: build

.PHONY: build
build:
	docker compose build

.PHONY: format-check
format-check:
	docker compose run --rm -v .:/src base python3 scripts/format.py /src

.PHONY: format-fix
format-fix:
	docker compose run --rm -v .:/src base python3 scripts/format.py --fix /src

.PHONY: bag2mcap
bag2mcap:
	docker compose run bag2mcap
