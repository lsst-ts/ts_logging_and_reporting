########################
ts_logging_and_reporting
########################

``ts_logging_and_reporting`` is a package providing summarizing and other functionality to query logging sources from the summit and usdf.

===================
About this software
===================

The notebooks contained are a **Prototype** application.  It is intended to help figure
out what is needed for a *Project-wide Logging and Reporting* system
for Rubin. It helps with this by presenting content in the `Nightly Digest`_
.. _Nightly Digest: https://usdf-rsp-dev.slac.stanford.edu/times-square/github/lsst-ts/ts_logging_and_reporting/NightLog
jupyter notebook under Times Square. The intent is to eventually put
the content into a solid User Interface environment (such as **LOVE:**
*LSST Operator's Visualization Environment*). The Times Square
environment is used because it allows rapid turn-around from code
change to an easily accessible report on USDF.

While current *rendering* is in the Notebook, most of the rest of the code
(back-end) is in the python package included in this repository.  The
intent is for that package to be modified and used with the final UI.

======
Set up
======

For local development, create a virtual environment and install the required packages:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

This repository uses pre-commit hooks to ensure code is formatted correctly and 
that notebooks are cleared of outputs before committing. 
To set up the pre-commit hooks, run the following command in the root directory of the repository:

```bash

pre-commit install
```
