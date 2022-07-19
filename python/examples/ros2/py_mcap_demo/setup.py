from setuptools import setup

package_name = "py_mcap_demo"

setup(
    name=package_name,
    version="0.0.0",
    packages=[package_name],
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + package_name]),
        ("share/" + package_name, ["package.xml"]),
    ],
    install_requires=["setuptools"],
    zip_safe=True,
    maintainer="James Smith",
    maintainer_email="james@foxglove.dev",
    description="Example demonstrating rosbag2 python API with mcap",
    license="Apache-2.0",
    tests_require=["pytest", "mcap"],
    entry_points={
        "console_scripts": [
            "reader = py_mcap_demo.reader:main",
            "writer = py_mcap_demo.writer:main",
        ],
    },
)
