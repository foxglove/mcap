*******************************************
Using ``rosbag2_py`` to use MCAP with ROS 2
*******************************************

You can use the `rosbag2_py` library along with the
`MCAP Storage Plugin <https://github.com/ros-tooling/rosbag2_storage_mcap/>`_ to interact with
MCAP files in ROS2 packages.

Workspace Setup
---------------

To get started, you'll need to install the following ROS packages for your distribution:

.. code-block:: bash

  ros-$ROS_DISTRO-ros-base
  ros-$ROS_DISTRO-ros2bag
  ros-$ROS_DISTRO-rosbag2-transport
  ros-$ROS_DISTRO-rosbag2-storage-mcap

Writing Messages
----------------
.. literalinclude:: ../examples/ros2/py_mcap_demo/py_mcap_demo/writer.py

Reading Messages
----------------
.. literalinclude:: ../examples/ros2/py_mcap_demo/py_mcap_demo/reader.py
