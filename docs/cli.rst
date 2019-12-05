CLI
===

Bioluigi provides a command-line interface tool to interact more conveniently
with Luigi scheduler.


List and filter tasks
---------------------

List all the tasks that match the given filters.

.. code-block:: bash

   bioluigi list [--status] [--detailed] [TASK_GLOB]

- ``--status`` is used to filter tasks by status (i.e. ``PENDING``, ``RUNNING``,
  ``DONE``, ...) and can be specified multiple times.
- ``--detailed`` triggers the detailed view produced by ``bioluigi view``
  subcommand.
- ``TASK_GLOB`` is a UNIX glob pattern that match tasks names and identifiers
  according to :py:func:`fnmatch.fnmatch` rules.

View a task
-----------

Given a task identifier, this subcommand will display the details of a task.

.. code-block:: bash

   bioluigi show TASK_ID

- ``TASK_ID`` is a task identifier.
