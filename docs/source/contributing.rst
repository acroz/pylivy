Contributing to pylivy
======================

Thanks for considering contributing to ``pylivy``!

.. _opening-issues:

Asking questions and reporting issues
-------------------------------------

If you have any questions on using ``pylivy`` or would like to make a
suggestion on improving ``pylivy``, please open an issue on GitHub:

https://github.com/acroz/pylivy/issues

Submitting code changes
-----------------------

Before `opening a PR <https://github.com/acroz/pylivy/pulls>`_, have a look at
the information below on code formatting and tests. Tests will be run
automatically `on Travis <https://travis-ci.org/acroz/pylivy>`_ and must pass
before a PR can be merged.

Code formatting
+++++++++++++++

Code must be formatted with `Black <https://black.readthedocs.io/>`_ (with a
line length of 79, as configured in ``pyproject.toml``), plus pass
`Flake8 <http://flake8.pycqa.org/>`_ linting and `mypy
<http://mypy-lang.org/>`_ static type checks.

It's recommend that you configure your editor to `autoformat your code with
Black <https://black.readthedocs.io/en/stable/editor_integration.html>`_ and
to highlight any Flake8 or mypy errors. This will help you catch them early
and avoid disappointment when the tests are run later!

Running tests
+++++++++++++

``pylivy`` includes two types of code tests; unit tests and integration tests.
The unit tests test individual classes of the code base, while the integration
tests verify the behaviour of the library against an actual running Livy
server.

To run the unit tests, which run quickly and do not require a Livy server to be
running, first install ``tox`` (a Python testing tool) if you do not already
have it:

.. code-block:: bash

    pip install tox

then run:

.. code-block:: bash

    tox -e py

``tox`` will build the project into a package, prepare a Python virtual
environment with additional test dependencies, and execute the tests. You can
also specify the version of Python to test against by specifying ``py36``,
``py37`` etc. instead of ``py`` in the above command.

To run integration tests, you need to first start a Livy server to test
against. For this purpose, I've prepared a Docker image that runs a basic Livy
setup. To run it:

.. code-block:: bash

    docker run --publish 8998:8998 acroz/livy

Then, in a separate shell, run the integration tests:

.. code-block:: bash

    tox -e py-integration

Again, specify the version of Python to test against by specifying ``py36``,
``py37`` etc. instead of ``py``.

Adding tests
++++++++++++

Any new contributions to the library should include appropriate tests, possibly
including unit tests, integration tests, or both. Please get in touch by
:ref:`opening an issue <opening-issues>` if you'd like to discuss what makes
sense.

Both unit tests and integration tests are written with the `pytest
<https://docs.pytest.org/>`_ testing framework. If you're not familiar with it,
I suggest having a look at their extensive documentation and examples first.
