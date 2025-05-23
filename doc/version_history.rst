v0.2.0 (2025-05-23)
===================

New Features
------------

- ### Nightly Digest FastAPI Application Added to Logging and Reporting Backend

  - Introduces a FastAPI application with endpoints to retrieve data from data source adapters.
  - Updates data source adapters to accept an authentication token, either forwarded by the API endpoint or obtained from environment variables or authentication headers.
  - Modifies the `NarrativelogAdapter` to set the instrument in narrative log data using the `components_json` field instead of `components`. (`DM-50894 <https://rubinobs.atlassian.net//browse/DM-50894>`_)


v0.1.0 (2025-05-13)
===================

New Features
------------

- Add conda packaging files. (`DM-50732 <https://rubinobs.atlassian.net//browse/DM-50732>`_)
