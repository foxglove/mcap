.PHONY: image
image:
	docker build -t mcap_ros2 .

.PHONY: test
test: image
	docker run -t --rm --name mcap_ros2 mcap_ros2 colcon test

.PHONY: write
write: image
	docker run -t --rm --name mcap_ros2 -v $(CURDIR):/mcap/ros2 mcap_ros2 ros2 run py_mcap_demo writer bag

.PHONY: read
read: image
	mkdir -p output
	docker run -t --rm --name mcap_ros2 -v $(CURDIR):/mcap/ros2 mcap_ros2 ros2 run py_mcap_demo reader bag

.PHONY: shell
shell: image
	docker run -it --rm --name mcap_ros2 mcap_ros2 /bin/bash
