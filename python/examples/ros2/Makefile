.PHONY: image
image:
	docker build -t mcap_ros2 .

.PHONY: test
test: image
	docker run -t --rm --name mcap_ros2 mcap_ros2 colcon test

.PHONY: write
write: image
	mkdir -p bags
	docker run -t --rm --name mcap_ros2 -v $(CURDIR)/bags:/mcap/ros2/bags mcap_ros2 ros2 run py_mcap_demo writer bags/demo

.PHONY: read
read: image
	mkdir -p bags
	docker run -t --rm --name mcap_ros2 -v $(CURDIR)/bags:/mcap/ros2/bags mcap_ros2 ros2 run py_mcap_demo reader bags/demo

.PHONY: shell
shell: image
	docker run -it --rm --name mcap_ros2 mcap_ros2 /bin/bash

.PHONY: clean
clean:
	rm -rf bags install log build
