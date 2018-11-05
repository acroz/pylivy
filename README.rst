pylivy
======

.. image:: https://travis-ci.org/acroz/pylivy.svg?branch=master
    :target: https://travis-ci.org/acroz/pylivy

.. image:: https://badge.fury.io/py/livy.svg
    :target: https://pypi.org/project/livy/

`Livy <https://livy.incubator.apache.org/>`_ is an open source REST interface
for interacting with `Spark <http://spark.apache.org/>`_. ``pylivy`` is a
Python client for Livy, enabling easy remote code execution on a Spark cluster.

Usage
-----

.. code:: python

    from livy import LivySession

    LIVY_URL = 'http://spark.example.com:8998'

    with LivySession(LIVY_URL) as session:
        # Run some code on the remote cluster
        session.run("filtered = df.filter(df.name == 'Bob')")
        # Retrieve the result
        local_df = session.read('filtered')

.. code:: python

    # Authenticated against a knox/livy/spark server

    LIVY_URL = os.environ.get('LIVY_TEST_URL_KNOX', 'https://knox.livy.spark.example.com/gateway/kerbtest2/livy/v1/')
    username = os.environ.get('USERNAME', 'user')
    passwd = os.environ.get('PASSWORD', 'password')
    auth = (username, passwd)
    from livy import LivySession

    with LivySession(LIVY_URL,auth=auth) as session:
        # Run some code on the remote cluster
        session.run("filtered = df.filter(df.name == 'Bob')")
        # Retrieve the result
        local_df = session.read('filtered')