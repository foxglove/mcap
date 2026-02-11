.PHONY: pipenv
pipenv:
	SETUPTOOLS_ENABLE_FEATURES="legacy-editable" pipenv install --dev --deploy

.PHONY: build
build: pipenv
	pipenv run python -m build mcap
	pipenv run python -m build mcap-protobuf-support
	pipenv run python -m build mcap-ros1-support
	pipenv run python -m build mcap-ros2-support

.PHONY: test
test: pipenv
	pipenv run python -m pytest mcap
	pipenv run python -m pytest mcap-protobuf-support
	pipenv run python -m pytest mcap-ros1-support
	pipenv run python -m pytest mcap-ros2-support

.PHONY: examples
examples: pipenv
	mkdir -p example_mcaps
	pipenv run python examples/jsonschema/pointcloud_csv_to_mcap.py examples/jsonschema/bus.csv -o example_mcaps/bus.mcap
	pipenv run python examples/raw/reader.py example_mcaps/bus.mcap
	pipenv run python examples/raw/writer.py example_mcaps/raw.mcap
	pipenv run python examples/raw/reader.py example_mcaps/raw.mcap
	cd examples/protobuf && pipenv run python writer.py $(CURDIR)/example_mcaps/protobuf.mcap
	cd examples/protobuf && pipenv run python image_example.py $(CURDIR)/example_mcaps/image.mcap
	cd examples/protobuf && pipenv run python point_cloud_example.py $(CURDIR)/example_mcaps/point_cloud.mcap
	cd examples/protobuf && pipenv run python reader.py $(CURDIR)/example_mcaps/protobuf.mcap
	cd examples/protobuf && pipenv run python reader.py $(CURDIR)/example_mcaps/point_cloud.mcap
	pipenv run python examples/ros1/writer.py example_mcaps/ros1.mcap
	pipenv run python examples/ros1/reader.py example_mcaps/ros1.mcap
	pipenv run python examples/ros2-noenv/writer.py example_mcaps/ros2.mcap
	pipenv run python examples/ros2-noenv/reader.py example_mcaps/ros2.mcap
	make -C examples/ros2 clean
	make -C examples/ros2 write
	make -C examples/ros2 read
	pipenv install --categories ulog2mcap-example
	pipenv run pytest examples/ulog2mcap/test_convert.py -v

.PHONY: lint
lint: pipenv
	pipenv run python -m flake8
	pipenv run black --check --diff --color mcap
	pipenv run black --check --diff --color mcap-protobuf-support
	pipenv run black --check --diff --color --extend-exclude="_vendor" mcap-ros1-support
	pipenv run black --check --diff --color --extend-exclude="_vendor" mcap-ros2-support
	pipenv run isort .
	cd mcap && pipenv run pyright
	cd mcap-protobuf-support && pipenv run pyright mcap_protobuf
	cd mcap-ros1-support && pipenv run pyright mcap_ros1
	cd mcap-ros2-support && pipenv run pyright mcap_ros2

.PHONY: docs
docs: pipenv
	pipenv run sphinx-apidoc --separate --tocfile index --output-dir docs/mcap-apidoc mcap/mcap
	pipenv run sphinx-apidoc --separate --tocfile index --output-dir docs/mcap-protobuf-apidoc mcap-protobuf-support/mcap_protobuf
	pipenv run sphinx-apidoc --separate --tocfile index --output-dir docs/mcap-ros1-apidoc mcap-ros1-support/mcap_ros1 mcap-ros1-support/mcap_ros1/_vendor
	pipenv run sphinx-apidoc --separate --tocfile index --output-dir docs/mcap-ros2-apidoc mcap-ros2-support/mcap_ros2 mcap-ros2-support/mcap_ros2/_vendor
	pipenv run sphinx-build -W --keep-going docs ../__docs__/python
	echo "Preview: file://$(abspath ../__docs__/python/index.html)"

.PHONY: clean
clean:
	rm -rf dist
	find . -name "build" -type d -exec rm -rf {} +
	find . -name "*.egg-info" -type d -exec rm -rf {} +
	find docs -name "*-apidoc" -type d -exec rm -rf {} +
