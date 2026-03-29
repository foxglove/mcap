.PHONY: sync
sync:
	uv sync --frozen

.PHONY: build
build: sync
	uv run python -m build mcap
	uv run python -m build mcap-protobuf-support
	uv run python -m build mcap-ros1-support
	uv run python -m build mcap-ros2-support

.PHONY: test
test: sync
	uv run python -m pytest mcap
	uv run python -m pytest mcap-protobuf-support
	uv run python -m pytest mcap-ros1-support
	uv run python -m pytest mcap-ros2-support

.PHONY: examples
examples: sync
	mkdir -p example_mcaps
	uv run python examples/jsonschema/pointcloud_csv_to_mcap.py examples/jsonschema/bus.csv -o example_mcaps/bus.mcap
	uv run python examples/raw/reader.py example_mcaps/bus.mcap
	uv run python examples/raw/writer.py example_mcaps/raw.mcap
	uv run python examples/raw/reader.py example_mcaps/raw.mcap
	cd examples/protobuf && uv run python writer.py $(CURDIR)/example_mcaps/protobuf.mcap
	cd examples/protobuf && uv run python image_example.py $(CURDIR)/example_mcaps/image.mcap
	cd examples/protobuf && uv run python point_cloud_example.py $(CURDIR)/example_mcaps/point_cloud.mcap
	cd examples/protobuf && uv run python reader.py $(CURDIR)/example_mcaps/protobuf.mcap
	cd examples/protobuf && uv run python reader.py $(CURDIR)/example_mcaps/point_cloud.mcap
	uv run python examples/ros1/writer.py example_mcaps/ros1.mcap
	uv run python examples/ros1/reader.py example_mcaps/ros1.mcap
	uv run python examples/ros2-noenv/writer.py example_mcaps/ros2.mcap
	uv run python examples/ros2-noenv/reader.py example_mcaps/ros2.mcap
	make -C examples/ros2 clean
	make -C examples/ros2 write
	make -C examples/ros2 read
	uv sync --frozen --group ulog2mcap-example
	uv run pytest examples/ulog2mcap/test_convert.py -v

.PHONY: lint
lint: sync
	uv run python -m flake8
	uv run black --check --diff --color mcap
	uv run black --check --diff --color mcap-protobuf-support
	uv run black --check --diff --color --extend-exclude="_vendor" mcap-ros1-support
	uv run black --check --diff --color --extend-exclude="_vendor" mcap-ros2-support
	uv run isort .
	uv run pyright --project mcap
	uv run pyright --project mcap-protobuf-support mcap-protobuf-support/mcap_protobuf
	uv run pyright --project mcap-ros1-support mcap-ros1-support/mcap_ros1
	uv run pyright --project mcap-ros2-support mcap-ros2-support/mcap_ros2

.PHONY: docs
docs: sync
	uv run sphinx-apidoc --separate --tocfile index --output-dir docs/mcap-apidoc mcap/mcap
	uv run sphinx-apidoc --separate --tocfile index --output-dir docs/mcap-protobuf-apidoc mcap-protobuf-support/mcap_protobuf
	uv run sphinx-apidoc --separate --tocfile index --output-dir docs/mcap-ros1-apidoc mcap-ros1-support/mcap_ros1 mcap-ros1-support/mcap_ros1/_vendor
	uv run sphinx-apidoc --separate --tocfile index --output-dir docs/mcap-ros2-apidoc mcap-ros2-support/mcap_ros2 mcap-ros2-support/mcap_ros2/_vendor
	uv run sphinx-build -W --keep-going docs ../__docs__/python
	echo "Preview: file://$(abspath ../__docs__/python/index.html)"

.PHONY: clean
clean:
	rm -rf dist
	find . -name "build" -type d -exec rm -rf {} +
	find . -name "*.egg-info" -type d -exec rm -rf {} +
	find docs -name "*-apidoc" -type d -exec rm -rf {} +
