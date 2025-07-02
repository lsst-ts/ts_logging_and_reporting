v0.5.0 (2025-07-01)
===================

New Features
------------

- Update JQL in the Jira adapter to retrieve tickets created or updated within the selected dayobs range. (`OSW-556 <https://rubinobs.atlassian.net//browse/OSW-556>`_)
- Change creation and updated date formats for tickets returned by the Jira adapter and add a flag to indicate if the ticket was created within the selected dayobs range. (`OSW-556 <https://rubinobs.atlassian.net//browse/OSW-556>`_)
- Add FastAPI dependency for retrieving auth token from requests headers. (`OSW-575 <https://rubinobs.atlassian.net//browse/OSW-575>`_)


Bug Fixes
---------

- Make sure the status dict isn't overwritten in ExposurelogAdapter.get_records when there is an exception. This ensures the status remains accurate and errors are reported correctly to the frontend. (`OSW-576 <https://rubinobs.atlassian.net//browse/OSW-576>`_)


v0.4.0 (2025-06-17)
===================

New Features
------------

- Add a fastapi endpoint and related service to fetch Jira tickets created between two dates and for a certain instrument. The service utilises the Jira source adapter to query the Jira API and return the results in a structured format. (`OSW-500 <https://rubinobs.atlassian.net//browse/OSW-500>`_)


Bug Fixes
---------

- Flip greater than sign when comparing dates in SourceAdapter. (`OSW-513 <https://rubinobs.atlassian.net//browse/OSW-513>`_)
- Remove asserts and replace with switching the date or raising an error. (`OSW-513 <https://rubinobs.atlassian.net//browse/OSW-513>`_)
- Remove default option for EXTERNAL_INSTANCE_URL when choosing a server site. (`OSW-520 <https://rubinobs.atlassian.net//browse/OSW-520>`_)


Other Changes and Additions
---------------------------

- expose errors from source adapters to the FastAPI application and throw HTTP exceptions with appropriate status codes and messages. (`OSW-501 <https://rubinobs.atlassian.net//browse/OSW-501>`_)
- Create an Exposure Log service to query the messages associated with each exposure, written by the observers. (`OSW-537 <https://rubinobs.atlassian.net//browse/OSW-537>`_)


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
