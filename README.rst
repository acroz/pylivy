pylivy
======

`Livy <https://livy.incubator.apache.org/>`_ is an open source REST interface
for interacting with `Spark <http://spark.apache.org/>`_. `pylivy` is a Python
client for Livy, enabling easy remote code execution on a Spark cluster:

.. code:: python

    from livy import Livy

    with Livy('http://spark.example.com:8998') as client:
        # Run some code on the remote cluster
        client.run("filtered = df.filter(df.name == 'Bob')")
        # Retrieve the result
        local_df = client.read('filtered')
