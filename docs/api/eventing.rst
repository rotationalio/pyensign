.. -*- mode: rst -*-

Events
======

In an event-driven or microservice architecture, an event is the atomic element of data.

An events can have many different datatypes, which are then wrapped in an object or struct that provides some schema information to help Ensign know how to serialize and deserialize your data.

API Reference
-------------

.. automodule:: pyensign.api.v1beta1.event
    :members: wrap, unwrap
    :undoc-members:
    :show-inheritance:
