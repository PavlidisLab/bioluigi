CLI
===

Bioluigi provides a command-line interface tool to interact more conveniently
with Luigi scheduler.


List and filter tasks
---------------------

List all the tasks that match the given filters.

.. code-block:: bash

   bioluigi list [--status] [--detailed] [--no-limit] [TASK_GLOB]

- ``--status`` is used to filter tasks by status (i.e. ``PENDING``, ``RUNNING``,
  ``DONE``, ...) and can be specified multiple times.
- ``--detailed`` triggers the detailed view produced by ``bioluigi view``
  subcommand.
- ``--no-limit`` disable the limit to the number of returned tasks by the
  scheduler. Note that this might freeze the scheduler thread for a couple of
  seconds if a large number of tasks is queried.
- ``TASK_GLOB`` is a UNIX glob pattern that match tasks names and identifiers
  according to :py:func:`fnmatch.fnmatch` rules.

View a task
-----------

Given a task identifier, this subcommand will display the details of a task.

.. code-block:: bash

   bioluigi show TASK_ID

- ``TASK_ID`` is a task identifier.

Forgive a failed task
---------------------

Forgive a failed task which will update its status to ``PENDING``.

.. code-block:: bash

   bioluigi forgive [--recursive] TASK_ID
   
- ``TASK_ID`` is a task identifier.
- ``--recursive`` recursively forgive the task dependencies

Re-enable a disabled task
-------------------------

Re-enable a disabled task which will update its status to ``FAILED``.

.. code-block:: bash

   bioluigi reenable [--recursive] TASK_ID

- ``TASK_ID`` is a task identifier.
- ``--recursive`` recursively re-enable the task dependencies
