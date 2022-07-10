********************************
Tutorial: Converting CSV to MCAP
********************************

Introduction
------------

In this tutorial we'll take a publically-available dataset and demonstrate a simple case
of converting CSV data to MCAP for viewing in Foxglove Studio. All of the code from this tutorial
is runnable, and can be found
`in the MCAP repo <https://github.com/foxglove/mcap/tree/main/python/examples/jsonschema/pointcloud_csv_to_mcap.py>`_.

Decoding the Data Source
------------------------

This tutorial uses the public CSV dataset "Sydney Urban Objects Dataset",
released by the Australian Centre for Field Robotics at the University of Sydney, NSW, Australia.
The original dataset can be downloaded from
`their website <https://www.acfr.usyd.edu.au/papers/SydneyUrbanObjectsDataset.shtml>`_. This is a
collection of CSV files containing pointclouds collected from objects on the street.
Decoding the data is pretty simple, thanks to Python's built-in ``csv`` and ``datetime`` libraries:

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-csv-decode-start
    :end-before: # tutorial-csv-decode-end

Creating a ``foxglove.Pointcloud``
----------------------------------

In order to view this pointcloud, we need encode these points in a way that Foxglove Studio
understands. To facilitate this, Foxglove Studio provides a collection of schemas for messages that
it knows how to display. These are available for a variety of serializations 
`on GitHub <https://github.com/foxglove/schemas>`_. For this tutorial, we'll focus on building a
``PointCloud`` instance with JSON encoding. 

To get started, take a look at the schema
`here <https://github.com/foxglove/schemas/blob/main/schemas/jsonschema/PointCloud.json>`_.

Lets start with encoding the point data. The schema expects a single ``base64``-encoded buffer
containing all point data, and some metadata describing how to decode it:

.. code-block:: json

    "point_stride": { "type": "integer", "minimum": 0, "description": "Number of bytes between points in the `data`" },
    "fields": {
      "type": "array",
      "items": {
        "$comment": "Generated from PackedElementField by @foxglove/schemas",
        "title": "PackedElementField",
        "description": "A field present within each element in a byte array of packed elements.",
        "type": "object",
        "properties": {
          "name": { "type": "string", "description": "Name of the field" },
          "offset": { "type": "integer", "minimum": 0, "description": "Byte offset from start of data buffer" },
          "type": {
            "title": "NumericType: Numeric type",
            "description": "Type of data in the field. Integers are stored using little-endian byte order.",
            "oneOf": [
              { "title": "UNKNOWN", "const": 0 },
              { "title": "UINT8", "const": 1 },
              { "title": "INT8", "const": 2 },
              { "title": "UINT16", "const": 3 },
              { "title": "INT16", "const": 4 },
              { "title": "UINT32", "const": 5 },
              { "title": "INT32", "const": 6 },
              { "title": "FLOAT32", "const": 7 },
              { "title": "FLOAT64", "const": 8 }
            ]
          }
        }
      },
      "description": "Fields in the `data`"
    },
    "data": {
      "type": "string",
      "contentEncoding": "base64",
      "description": "Point data, interpreted using `fields`"
    }

From the CSV data we have one timestamp and four floating-point data fields per point.
``foxglove.PointCloud`` uses one timestamp for the whole pointcloud, so we'll just use the first
point for that. We'll pack each field as a four byte single-precision little-endian float.

We start by describing our data layout in the ``foxglove.PointCloud`` message:

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-point-layout-start
    :end-before: # tutorial-point-layout-end

And pack the points using the Python built-in ``struct`` and ``base64`` libraries.

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-pack-points-start
    :end-before: # tutorial-pack-points-end

To set the pointcloud timestamp, the ``foxglove.PointCloud`` schema expects a pair of seconds
and nanoseconds:

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-timestamp-start
    :end-before: # tutorial-timestamp-end

And finally we set an arbitrary pose and ``frame_id``.

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-pose-frame-id-start
    :end-before: # tutorial-pose-frame-id-end

Writing the MCAP
----------------

Now that the pointcloud is built, we can write it into an MCAP. We'll start with some imports from
the python MCAP library:

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-mcap-imports-start
    :end-before: # tutorial-mcap-imports-end

Then we'll open a file for writing and write a header. Note that we can chose whatever name we
want for both ``profile`` and ``library``, but the profile must start with ``x-``.

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-write-header-start
    :end-before: # tutorial-write-header-end

After writing a header, we can create a "channel" of messages to contain our pointcloud. The schema
name and content informs Foxglove Studio that it can parse and display this message as a pointcloud.

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-write-channel-start
    :end-before: # tutorial-write-channel-end

Finally, we can write our message and ``finish()`` the MCAP writer. ``finish()`` writes the summary
and footer to the MCAP file.

.. literalinclude:: ../examples/jsonschema/pointcloud_csv_to_mcap.py
    :start-after: # tutorial-write-message-start
    :end-before: # tutorial-write-message-end

That's it! Once this is done, we have a valid MCAP file containing a pointcloud message. We can
check this with the MCAP CLI tool:

.. code-block:: bash

    $ mcap info output.mcap 
    library: my-excellent-library
    profile: x-jsonschema
    messages: 1
    duration: 0s
    start: 2011-11-04T01:32:10.881030912+11:00 (1320330730.881030912)
    end: 2011-11-04T01:32:10.881030912+11:00 (1320330730.881030912)
    compression:
            zstd: [1/1 chunks] (40.38%) 
    channels:
            (1) /pointcloud  1 msgs (+Inf Hz)   : foxglove.PointCloud [jsonschema]  
    attachments: 0
    $ mcap doctor output.mcap
    Examining output.mcap

We can view the result in Foxglove Studio by navigating to `<https://studio.foxglove.dev>`_
and dragging the MCAP file into the window.