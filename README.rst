########################
ts_logging_and_reporting
########################

Hello, ``ts_logging_and_reporting`` is a package providing summarizing and other functionality to query logging sources from the summit and usdf.

===================
About this software
===================

This is the backend repository for the `Nightly Digest`_

.. _Nightly Digest: https://usdf-rsp-dev.slac.stanford.edu/nightlydigest

This project is Rubin Observatory Software's *Project-wide Logging and Reporting* system
aiming to present and summarize content to give insight into nightly operations at the summit.
This includes fetching data from the `consolidated database`_, `exposure log`_, `narrative log`_,
`night report`_, and leverages other repositories like `rubin-nights`_ and `rubin-scheduler`_

.. _consolidated database: https://consdb.lsst.io/index.html
.. _exposure log: https://github.com/lsst-ts/exposurelog
.. _narrative log: https://github.com/lsst-ts/narrativelog
.. _night report: https://github.com/lsst-ts/ts_nightreport
.. _rubin-nights: https://github.com/lsst-sims/rubin_nights
.. _rubin-scheduler: https://rubin-scheduler.lsst.io

======
Set up
======

This is a conda package following the standards at `TSSW Developer Guide`_

.. _TSSW Developer Guide: https://tssw-developer.lsst.io/index.html
