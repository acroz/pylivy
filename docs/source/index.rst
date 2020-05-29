pylivy
======

.. image:: https://github.com/acroz/pylivy/workflows/Tests/badge.svg?branch=master
    :target: https://github.com/acroz/pylivy/actions

.. image:: https://badge.fury.io/py/livy.svg
    :target: https://pypi.org/project/livy/

.. image:: https://img.shields.io/pypi/pyversions/livy.svg
    :target: https://pypi.org/project/livy/

`Livy <https://livy.incubator.apache.org/>`_ is an open source REST interface
for interacting with `Spark <http://spark.apache.org/>`_. ``pylivy`` is a
Python client for Livy, enabling easy remote code execution on a Spark cluster.

Installation
------------

.. code:: bash

    $ pip install -U livy

Note that ``pylivy`` requires Python 3.6 or later.

Basic Usage
-----------

The :class:`~livy.session.LivySession` class is the main interface provided by
``pylivy``::

    from livy import LivySession

    LIVY_URL = "http://spark.example.com:8998"

    with LivySession.create(LIVY_URL) as session:
        # Run some code on the remote cluster
        session.run("filtered = df.filter(df.name == 'Bob')")
        # Retrieve the result
        local_df = session.read("filtered")

Similarly, batch sessions in Livy can be created and managed with the
:class:`~livy.batch.LivyBatch` class::

    from livy import LivyBatch

    LIVY_URL = "http://spark.example.com:8998"

    batch = LivyBatch.create(
        LIVY_URL,
        file=(
            "https://repo.typesafe.com/typesafe/maven-releases/org/"
            "apache/spark/spark-examples_2.11/1.6.0-typesafe-001/"
            "spark-examples_2.11-1.6.0-typesafe-001.jar"
        ),
        class_name="org.apache.spark.examples.SparkPi",
    )
    batch.wait()

See :class:`LivySession.create <livy.session.LivySession.create>` or
:class:`LivyBatch.create <livy.batch.LivyBatch.create>` for the full range of
options that can be specified when creating sessions or batches.

Authentication
--------------

Authenticate requests sent to Livy by passing `any requests Auth object
<http://docs.python-requests.org/en/master/user/authentication/>`_ to the
:class:`~livy.session.LivySession`. For example, to perform HTTP basic auth
do::

    from livy import LivySession
    from requests.auth import HTTPBasicAuth

    auth = HTTPBasicAuth("username", "password")

    with LivySession.create(LIVY_URL, auth) as session:
        session.run("filtered = df.filter(df.name == 'Bob')")
        local_df = session.read("filtered")

Custom requests session
-----------------------

``pylivy`` uses `requests <http://docs.python-requests.org/>`_ to make HTTP
requests to your Livy server. You can specify your own requests session in
order to customise how requests are made to the server.

For example, to add a custom header to all requests make to Livy::

    from livy import LivySession
    import requests

    LIVY_URL = "http://spark.example.com:8998"

    requests_session = requests.Session()
    requests_session.headers.update(
        {"X-Auth-Token": "MY-SECURITY-TOKEN"}
    )

    with LivySession.create(
        LIVY_URL,
        requests_session=requests_session
    ) as session:
        session.run("filtered = df.filter(df.name == 'Bob')")

API Documentation
-----------------

.. toctree::
    :maxdepth: 2

    api/session
    api/batch
    api/client

Contributing
------------

.. toctree::
    :maxdepth: 2

    contributing
