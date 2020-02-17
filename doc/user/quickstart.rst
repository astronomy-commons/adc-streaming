==========
Quickstart
==========

.. contents::
   :local:

Reading messages:

.. code:: bash

    import genesis.streaming as gs

    with gs.open("kafka://bootstrap.server/topic", "r", format="json") as stream:
        for idx, msg in stream:
            print(f"id: {idx}, contents: {msg}")

Writing messages:

.. code:: bash

    import genesis.streaming as gs

    with gs.open("kafka://bootstrap.server/topic", "w", format="json") as stream:
        stream.write({"contents": "hello"})
