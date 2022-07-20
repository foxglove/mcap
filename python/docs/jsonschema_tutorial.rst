*************************************************
Tutorial: Converting CSV to MCAP with JSON Schema
*************************************************

Introduction
------------

In this tutorial we'll take a publicly available dataset and demonstrate how to convert it from a
CSV format to an MCAP file. We'll use Python and `JSON Schema <https://json-schema.org/>`_ to
quickly create messages without requiring extra serialization libraries or generated code.

You can run all the code in this tutorial or reference it
`in the MCAP repo <https://github.com/foxglove/mcap/tree/main/python/examples/jsonschema/pointcloud_csv_to_mcap.py>`_.

Decoding the data source
------------------------

This tutorial uses the public CSV dataset
`"Sydney Urban Objects Dataset" <https://www.acfr.usyd.edu.au/papers/SydneyUrbanObjectsDataset.shtml>`_,
released by the **Australian Centre for Field Robotics** at the University of Sydney.
This is a collection of CSV files containing point clouds collected from objects on the street.

Decoding the data is pretty simple, thanks to Python's built-in ``csv`` and ``datetime`` libraries:

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-csv-decode-start
    :end-before: # tutorial-csv-decode-end

Creating a ``foxglove.PointCloud``
----------------------------------

To view this point cloud, we must encode these points in a way that Foxglove Studio
understands. To facilitate this, Foxglove Studio provides a collection of message schemas that
it knows how to display. These are available for a variety of serializations
`on GitHub <https://github.com/foxglove/schemas>`_. For this tutorial, we'll focus on building a
JSON ``PointCloud`` instance, using the provided 
`schema <https://github.com/foxglove/schemas/blob/main/schemas/jsonschema/PointCloud.json>`_.

Let's start with encoding the point data. The schema expects a single ``base64``-encoded buffer
containing all point data, and some metadata describing how to decode it.

The CSV data contains one timestamp and four floating-point data fields per point.
``foxglove.PointCloud`` uses one timestamp for the whole point cloud, so we'll use the first
point's timestamp. We'll pack each field as a four byte single-precision little-endian float.

We start by describing our data layout in the ``foxglove.PointCloud`` message:

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-point-layout-start
    :end-before: # tutorial-point-layout-end
    :dedent:

And pack the points using Python's built-in ``struct`` and ``base64`` libraries.

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-pack-points-start
    :end-before: # tutorial-pack-points-end
    :dedent:

In Foxglove Studio, each 3D object exists in its own coordinate frame. A point cloud's ``frame_id``
identifies which coordinate frame it belongs in, and its ``pose`` determines its relative position
from that coordinate frame's center.

Since we will only have one coordinate frame in our MCAP file, we can choose any string as our
``frame_id``, and use the identity pose to place our point cloud in its center.

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-pose-frame-id-start
    :end-before: # tutorial-pose-frame-id-end
    :dedent:

We'll leave the ``timestamp`` field for later, when we write the messages into the MCAP file.

Writing the MCAP file
---------------------

Now that the point cloud is built, we can write it into an MCAP file. We'll start with some imports
from the `Python MCAP library <https://github.com/foxglove/mcap/tree/main/python>`_:

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-mcap-imports-start
    :end-before: # tutorial-mcap-imports-end
    :dedent:

Let's open a file where we'll write our output MCAP data. First, we'll need to write our header.

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-write-header-start
    :end-before: # tutorial-write-header-end
    :dedent:

.. note::
  We can choose whatever name we want for ``library``. We're not using one of the
  `MCAP well-known profiles <https://github.com/foxglove/mcap/blob/main/docs/specification/appendix.md#well-known-profiles>`_,
  so we use our own custom profile name.

Next, create a "channel" of messages to contain our point cloud. The schema
name and content informs Foxglove Studio that it can parse and display this message as a point
cloud.

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-write-channel-start
    :end-before: # tutorial-write-channel-end
    :dedent:

Next, we write our messages. If we only wrote one message, our MCAP file would be zero duration.
To address that, let's write our point cloud message a few times with successive
timestamps.

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-write-message-start
    :end-before: # tutorial-write-message-end
    :dedent:

Finally, we invoke ``finish()`` on the MCAP writer to include the summary and footer
in the output file.

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-finish-start
    :end-before: # tutorial-finish-end
    :dedent:

That's it! We now have a valid MCAP file with 10 point cloud messages. We can
check this with the `MCAP CLI tool <https://github.com/foxglove/mcap/tree/main/go/cli/mcap>`_:

.. code-block:: bash

    $ brew install mcap
    ...
    $ mcap info output.mcap
    library: my-excellent-library
    profile: x-jsonschema
    messages: 10
    duration: 900ms
    start: 2011-11-04T01:32:10.881030912+11:00 (1320330730.881030912)
    end: 2011-11-04T01:32:11.781030912+11:00 (1320330731.781030912)
    compression:
            zstd: [1/1 chunks] (93.43%)
    channels:
            (1) /pointcloud  10 msgs (11.11 Hz)   : foxglove.PointCloud [jsonschema]
    attachments: 0
    $ mcap doctor output.mcap
    Examining output.mcap

View your point cloud in `Foxglove Studio <https://studio.foxglove.dev>`_ by
dragging the MCAP file into the window. Add a `3D Panel <https://foxglove.dev/docs/studio/panels/3d>`_
and enable the **/pointcloud** topic to see the result!
