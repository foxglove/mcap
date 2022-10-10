***************************
Working with ROS 2 Messages
***************************

These examples demonstrate writing ROS2 messages to an MCAP recording and
reading messages without a ROS2 installation. However, you will need to supply
the correct datatype and concatenated message definition when writing. When
importing message definitions in Python from ROS2, you can use `message._type`
and `message.__class__._full_text`.

Alternatively, you can use the `rosbag2_py` library along with the
`MCAP Storage Plugin <https://github.com/ros-tooling/rosbag2_storage_mcap/>`_ to
interact with MCAP files in ROS2 packages.

Writing Messages
----------------
.. literalinclude:: ../examples/ros2-noenv/writer.py

Reading Messages
----------------
.. literalinclude:: ../examples/ros2-noenv/reader.py
