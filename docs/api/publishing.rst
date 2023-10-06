.. -*- mode: rst -*-

Publishing
==========

A `Publisher` is responsible for emitting events to a topic stream.

In Ensign, you can create a `Publisher` once you have established a client. On publish, the client checks to see if it has an open publish stream created and if it doesn’t, it opens a stream to the correct Ensign node.

API Reference
-------------

.. automodule:: pyensign.publisher
    :members: Publisher
    :undoc-members:
    :show-inheritance:
