pylivy
======

.. image:: https://travis-ci.org/acroz/pylivy.svg?branch=master
    :target: https://travis-ci.org/acroz/pylivy

.. image:: https://badge.fury.io/py/livy.svg
    :target: https://pypi.python.org/pypi/livy/

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

You can also use ``pylivy`` asynchronously with ``asyncio``:

.. code:: python

    import asyncio
    from livy import AsyncLivySession

    LIVY_URL = 'http://spark.example.com:8998'

    await def do_task():

        async with AsyncLivySession(LIVY_URL) as session:
            # Run some code on the remote cluster
            await session.run("filtered = df.filter(df.name == 'Bob')")
            # Retrieve the result
            local_df = await session.read('filtered')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(do_task())
