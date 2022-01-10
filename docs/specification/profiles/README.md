## Supported profiles

[ros1]: ./ros1.md
[ros2]: ./ros2.md

This directory contains supported "profiles" for MCAP channel info user data.
Usage of these profiles is not mandatory, but may be helpful to third party
tooling in better understanding and displaying your data. For instance, an
application that reads a "latching" key from a channel info record will not
necessarily know what to do with the value - however if the reader knows the
MCAP file is recorded with the "ros1" profile, it can make an inference that
this is indicating a "latching topic" and behave accordingly.

To make use of a profile, simply include the name of the profile in the
"profile" field in the file header, and include the required keys in the user
data section of all channel info records in the file. Additional keys can be
added beyond those required by the profile as desired.

Supported profiles are listed below:

- [ros1][ros1]
- [ros2][ros2]
