Schedulers
==========

Bioluigi extend the scope of Luigi by interfacing external schedulers such as
`Slurm <https://slurm.schedmd.com/>`_ to dispatch the execution of external
programs.

The simplest way of specifying a scheduler is via the configuration so that it
become the default for any scheduled external programs.

.. code-block:: ini

   [bioluigi]
   scheduler=slurm
   scheduler_extra_args=

The second option is to set it explicitly when invoking a task.

.. code-block:: python

   bcftools.Annotate(scheduler='slurm')

Local
-----

The local scheduler performs the execution within the context of a Luigi
worker and consumes two kind of resources: ``cpus`` and ``memory``. These
resources must be specified in ``luigi.cfg``.

.. code-block:: ini

   [bioluigi]
   cpus=16
   memory=32

This scheduler is used by default.

Slurm
-----

Unlike the local scheduler, no resource allocation is performed via Luigi
resource mechanism. Instead, two resources are consumed: ``slurm_jobs`` and
``slurm_cpus`` to respectively control how many jobs and CPUs can be allocated.

.. code-block:: ini

   [bioluigi]
   slurm_jobs=32
   slurm_cpus=256

.. note::

   Each scheduled external program consume a mostly idle Luigi worker and for
   concurrency to be achieved, many of them have to be specified with the
   ``--workers`` flag.

   .. code-block:: bash

      luigi --module tasks --workers 32 <task> <task_args>
