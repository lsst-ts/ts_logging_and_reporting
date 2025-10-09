v0.9.0 (2025-10-09)
====================

New Features
------------

- Use rubin-nights repo to get context feed data. (`OSW-666 <https://rubinobs.atlassian.net//browse/OSW-666>`_)
- Use rubin-nights repo to get valid overhead for each exposure and dome open time (`OSW-992 <https://rubinobs.atlassian.net//browse/OSW-992>`_)


Bug Fixes
---------

- Data Log now supports AuxTel data. (`OSW-764 <https://rubinobs.atlassian.net//browse/OSW-764>`_)


Other Changes and Additions
---------------------------

- Bump rubin_nights to v0.6.1. (`OSW-1177 <https://rubinobs.atlassian.net//browse/OSW-1177>`_)
- Add rubin-scheduler to conda recipe. (`OSW-1178 <https://rubinobs.atlassian.net//browse/OSW-1178>`_)
- Remove notebooks directory, no longer needed and causing issues with ruff. (`OSW-1219 <https://rubinobs.atlassian.net//browse/OSW-1219>`_)
- Bump ts-conda-build to v0.5 in the conda recipe. (`OSW-1220 <https://rubinobs.atlassian.net//browse/OSW-1220>`_)
- Add rubin-nights to conda recipe. (`OSW-1220 <https://rubinobs.atlassian.net//browse/OSW-1220>`_)
- Change the build string in the conda recipe. (`OSW-1220 <https://rubinobs.atlassian.net//browse/OSW-1220>`_)


v0.8.0 (2025-09-02)
===================

New Features
------------

- Backend updates to get data for the Time Accounting applet. (`OSW-665 <https://rubinobs.atlassian.net//browse/OSW-665>`_)


Bug Fixes
---------

- Match dayobs query in get_transformed_efd to be exclusive of end day. (`OSW-903 <https://rubinobs.atlassian.net//browse/OSW-903>`_)


v0.7.2 (2025-08-20)
===================

New Features
------------

- Add night-reports endpoint. (`OSW-667 <https://rubinobs.atlassian.net//browse/OSW-667>`_)
- Update transformed efd query to reflect column changes. (`OSW-780 <https://rubinobs.atlassian.net//browse/OSW-780>`_)
- Fetch nautical twilight times rather than astronomical and calculate night hours accordingly (`OSW-832 <https://rubinobs.atlassian.net//browse/OSW-832>`_)


Bug Fixes
---------

- Fix narrative-log endpoint to follow observing day definition. (`OSW-831 <https://rubinobs.atlassian.net//browse/OSW-831>`_)


Other Changes and Additions
---------------------------

- Query exposure `seq_num` from consdb to be used in observing conditions applet tooltip (`OSW-853 <https://rubinobs.atlassian.net//browse/OSW-853>`_)


v0.7.1 (2025-08-07)
===================

Bug Fixes
---------

- Fix exposurelog-entries authorization setting and improve endpoints testing. (`OSW-808 <https://rubinobs.atlassian.net//browse/OSW-808>`_)


v0.7.0 (2025-08-05)
===================

New Features
------------

- Add function to query ConsDb's transformed efd tables. (`OSW-629 <https://rubinobs.atlassian.net//browse/OSW-629>`_)
- Update ConsDB and Almanac services to support Observing Conditions Applet (`OSW-646 <https://rubinobs.atlassian.net//browse/OSW-646>`_)


Bug Fixes
---------

- Swap out deprecated applymap method. (`OSW-703 <https://rubinobs.atlassian.net//browse/OSW-703>`_)
- Pass auth as dependency through FastAPI endpoint. (`OSW-704 <https://rubinobs.atlassian.net//browse/OSW-704>`_)


v0.6.1 (2025-07-19)
===================

Bug Fixes
---------

- Add auth token to exposure log service get_exposurelog_entries (`OSW-704 <https://rubinobs.atlassian.net//browse/OSW-704>`_)


v0.6.0 (2025-07-16)
===================

New Features
------------

- Add 'can_see_sky' to consdb exposures query and return no of on-sky exposures and total on-sky exposure time in the /exposures endpoint response (`OSW-541 <https://rubinobs.atlassian.net//browse/OSW-541>`_)
- Create backend services for Data Log page. (`OSW-572 <https://rubinobs.atlassian.net//browse/OSW-572>`_)


Bug Fixes
---------

- Count for multiple nights when returning night hours in the almanac service to correct telescope efficiency calculation in frontend. The service now iterates over the dayobs range and sums night hours for each day. (`OSW-579 <https://rubinobs.atlassian.net//browse/OSW-579>`_)
- Fix efficiency calculation by removing the extra loop in calculating night hours (`OSW-655 <https://rubinobs.atlassian.net//browse/OSW-655>`_)


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
