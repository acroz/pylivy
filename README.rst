pylivy
======

`Livy <https://livy.incubator.apache.org/>`_ is an open source REST interface
for interacting with `Spark <http://spark.apache.org/>`_. ``pylivy`` is a
Python client for Livy, enabling easy remote code execution on a Spark cluster:

.. code:: python

    from livy import LivySession

    with LivySession('http://spark.example.com:8998') as session:
        # Run some code on the remote cluster
        session.run("filtered = df.filter(df.name == 'Bob')")
        # Retrieve the result
        local_df = session.read('filtered')

You can also use ``pylivy`` asynchronously:

.. code:: python

    from livy import AsyncLivySession

    async with AsyncLivySession('http://spark.example.com:8998') as session:
        # Run some code on the remote cluster
        await session.run("filtered = df.filter(df.name == 'Bob')")
        # Retrieve the result
        local_df = await session.read('filtered')
