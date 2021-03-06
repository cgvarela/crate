.. highlight:: sh

.. _conf-environment-variables:

=====================
Environment Variables
=====================

.. rubric:: Table of Contents

.. contents::
   :local:

.. _env-crate-home:

``CRATE_HOME``
==============

Specifies the home directory of the installation, it is used to find default
file paths like e.g. ``config/crate.yml`` or the default data directory
location. This variable is usally defined at the by-distribution shipped
start-up script. In most cases it is the parent directory of the directory
containing the ``bin/crate`` executable.

:CRATE_HOME:
  Home directory of CrateDB installation.

  Used to refer to default config files, data locations, log files, etc.
  All configured relative paths will use this directory as a parent.

``CRATE_JAVA_OPTS``
===================

This variable allows you to set `Java options`_ for CrateDB, such as as the
thread stack size.

For example, to change the stack size in order to avoid stack overflow
exceptions::

    CRATE_JAVA_OPTS=-Xss500k

.. _`Java options`: http://docs.oracle.com/javase/7/docs/technotes/tools/windows/java.html#CBBIJCHG

.. _crate-heap-size:

``CRATE_HEAP_SIZE``
===================

This variable specifies the amount of memory that can be used by the JVM.

The value of the environment variable can be suffixed with ``g`` or ``m``. For
example::

    CRATE_HEAP_SIZE=4g

Certain operations in CrateDB require a lot of records to be hold in memory at
a time. If the amount of heap that can be allocated by the JVM is too low these
operations would fail with an OutOfMemory exception.

So it's important to choose a value high enough for the intended use-case. But
there are two limitations:

Use max. 50% of available RAM
-----------------------------

Be aware that there is also another user of memory besides CrateDB's HEAP: our
underlying storage engine `Lucene`_. It leverages the underlying OS for caching
in-memory data structures by design. `Lucene`_ indexes are split in several
segment files, every file is immutable and will never change. This makes them
super cache-friendly and the underlying OS will keep hot segments resident in
memory for faster access. So if all system memory is assigned to CrateDB's
HEAP, there won't be any left-over for `Lucene`_ which can cause serious
performance impacts.

.. NOTE::

   A good recommendation is to assign 50% of the available memory to CrateDB's
   HEAP while leaving the other 50% free. It will not get unused, `Lucene`_
   will use whatever is left-over.

.. _Lucene: https://lucene.apache.org/

Never use more than 30.5 Gigabyte
---------------------------------

In order to save on precious memory on x64 systems the Hotspot Java Virtual
Machine uses a technique called `Compressed Ordinary object pointers (oops)
<Compressed Oops>`_.

These are pointers to java objects in the heap that only consume 32 Bit, which
saves you lots of space. The actual native 64 bit pointers are computed by
scaling the 32 bit value by a factor of 8 and add it to a base heap address.
This allows the JVM to address about 32 GB of heap.

If you configure your heap to more than 32 GB `Compressed Oops`_ cannot be used
anymore. In effect, there will be much less space available in the heap as
object pointers now consume twice as much.

This boundary should be considered an upper bound for the heap size of any JVM
application.

.. NOTE::

   In order to ensure that `Compressed Oops`_ are used no matter what JVM
   CrateDB runs on, configuring the heap to a value less than or equal to *30.5
   GB* (``30500m``) is suggested, as some JVMs only support `Compressed Oops`_
   up to that value.

.. _`Compressed Oops`: https://wiki.openjdk.java.net/display/HotSpot/CompressedOops

Running CrateDB on machines with huge RAM
-----------------------------------------

If hardware with much more RAM is available, it is suggested to run more than
one CrateDB instance on that machine with each one having a heap size of around
30.5 GB (``30500m``). But still leave half of the available RAM to `Lucene`_.

In this case consider adding: ``cluster.routing.allocation.same_shard.host:
true`` to your config. This will prevent allocating primary and replica of the
same shard on the same machine even if more than one instances running on it.

``CRATE_GC_*``
==============

There are various environment variables to control garbage collection logging
for CrateDB.

See :ref:`conf-logging-gc`.

``CRATE_HEAP_DUMP_PATH``
========================

CrateDB will create a heap dump in case of a crash caused by an out of memory
error. It is necessary to make sure that there is enough disk space available
so that this heap dump could be created. The location of this heap dump can be
set via the ``CRATE_HEAP_DUMP_PATH`` environment variable.

:CRATE_HEAP_DUMP_PATH:
  | Path to a directory or file where the heap dump will be created. If a
    directory is specified, each time a heap dump is generated a new file will be
    created. If a path to a file is specified it will overwrite that each time.
  | *Default for .tar.gz:* The working directory
  | *Default for .rpm:*  /var/lib/crate
  | *Default for .deb:* /var/lib/crate
  | *Default for Docker:* /data/data
