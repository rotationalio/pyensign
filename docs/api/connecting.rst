.. -*- mode: rst -*-

Client Connection
=================

In order to write or read data from an underlying data system (like a database or event stream), you need a client to connect to the data system and interact with it as needed (such as reading and writing data). This connection often looks something like `conn = DBConnection(credentials)`, and after creating the conn variable, subsequent lines of code can leverage it to perform the kinds of data interactions you wish to make.

*Note: To establish a client in Ensign you need an API key . By default Ensign will read credentials from the ENSIGN_CLIENT_ID and ENSIGN_CLIENT_SECRET environment variables. If you include these in your bash profile, you can connect to Ensign with the following without having to specify your credentials in code.*

API Reference
-------------

.. automodule:: pyensign.ensign
    :members: Ensign
    :undoc-members:
    :show-inheritance:
