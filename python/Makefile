.PHONY: pipenv
pipenv:
	pipenv install --dev --deploy

.PHONY: build
build: pipenv
	pipenv run python -m build mcap
	pipenv run python -m build mcap-protobuf-support
	pipenv run python -m build mcap-ros1-support

.PHONY: test
test: pipenv
	pipenv run python -m pytest mcap
	pipenv run python -m pytest mcap-protobuf-support
	pipenv run python -m pytest mcap-ros1-support

.PHONY: lint
lint: pipenv
	pipenv run python -m flake8
	pipenv run black --check --diff --color mcap
	pipenv run black --check --diff --color mcap-protobuf-support
	pipenv run black --check --diff --color mcap-ros1-support
	cd mcap && pipenv run pyright
	cd mcap-protobuf-support && pipenv run pyright mcap_protobuf
	cd mcap-ros1-support && pipenv run pyright mcap_ros1

.PHONY: docs
docs: pipenv
	pipenv run sphinx-apidoc --separate --tocfile index --output-dir docs/mcap-apidoc mcap/mcap
	pipenv run sphinx-apidoc --separate --tocfile index --output-dir docs/mcap-protobuf-apidoc mcap-protobuf-support/mcap_protobuf
	pipenv run sphinx-apidoc --separate --tocfile index --output-dir docs/mcap-ros1-apidoc mcap-ros1-support/mcap_ros1
	pipenv run sphinx-build -W --keep-going docs ../__docs__/python
	echo "Preview: file://$(abspath ../__docs__/python/index.html)"

.PHONY: clean
clean:
	rm -rf dist
	find . -name "build" -type d -exec rm -rf {} +
	find . -name "*.egg-info" -type d -exec rm -rf {} +
	find docs -name "*-apidoc" -type d -exec rm -rf {} +
