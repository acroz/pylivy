pylivy
======

.. image:: https://travis-ci.org/acroz/pylivy.svg?branch=master
    :target: https://travis-ci.org/acroz/pylivy

.. image:: https://badge.fury.io/py/livy.svg
    :target: https://pypi.org/project/livy/

`Livy <https://livy.incubator.apache.org/>`_ is an open source REST interface
for interacting with `Spark <http://spark.apache.org/>`_. ``pylivy`` is a
Python client for Livy, enabling easy remote code execution on a Spark cluster.

Installation
------------

.. code:: bash

    $ pip install -U livy

Note that ``pylivy`` requires Python 3.6 or later.

Usage
-----

The :class:`LivySession <livy.session.LivySession>` class is the main interface
provided by ``pylivy``::

    from livy import LivySession

    LIVY_URL = 'http://spark.example.com:8998'

    with LivySession(LIVY_URL) as session:
        # Run some code on the remote cluster
        session.run("filtered = df.filter(df.name == 'Bob')")
        # Retrieve the result
        local_df = session.read('filtered')

Authenticate requests sent to Livy by passing `any requests Auth object
<http://docs.python-requests.org/en/master/user/authentication/>`_ to the
``LivySession``. For example, to perform HTTP basic auth do::

    from requests.auth import HTTPBasicAuth

    auth = HTTPBasicAuth('username', 'password')

    with LivySession(LIVY_URL, auth) as session:
        session.run("filtered = df.filter(df.name == 'Bob')")
        local_df = session.read('filtered')

API Documentation
-----------------

.. toctree::
    :maxdepth: 2

    api/session
    api/client
