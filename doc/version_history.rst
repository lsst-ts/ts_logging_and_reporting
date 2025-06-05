v0.3.0 (2025-06-05)
===================

New Features
------------

- Add Jira Adapter to query OBS project tickets. (`DM-50834 <https://rubinobs.atlassian.net//browse/DM-50834>`_)


- Add changelog workflow to check towncrier fragments are created. (`DM-50952 <https://rubinobs.atlassian.net//browse/DM-50952>`_)
- Return exposure data in the `/exposures` endpoint response (`DM-50966 <https://rubinobs.atlassian.net//browse/DM-50966>`_)
- Change route and service function parameters from datetime.date to int, to accept dayobs rather than dates from frontend (`DM-50966 <https://rubinobs.atlassian.net//browse/DM-50966>`_)
- Add `img_type` to exposure data (`DM-50966 <https://rubinobs.atlassian.net//browse/DM-50966>`_)
- Add Dockerfile for local development integration. (`OSW-490 <https://rubinobs.atlassian.net//browse/OSW-490>`_)


v0.2.0 (2025-05-23)
===================

New Features
------------

- Nightly Digest FastAPI Application Added to Logging and Reporting Backend

- Introduces a FastAPI application with endpoints to retrieve data from data source adapters.
- Updates data source adapters to accept an authentication token, either forwarded by the API endpoint or obtained from environment variables or authentication headers.
- Modifies the `NarrativelogAdapter` to set the instrument in narrative log data using the `components_json` field instead of `components`. (`DM-50894 <https://rubinobs.atlassian.net//browse/DM-50894>`_)


v0.1.0 (2025-05-13)
===================

New Features
------------

- Add conda packaging files. (`DM-50732 <https://rubinobs.atlassian.net//browse/DM-50732>`_)
