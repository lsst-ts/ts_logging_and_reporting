
# Backend Data Flow Overview

I am envisioning a REST API calling some potential combining functions, or directly the adapters to access or combine the necessary data from each appropriate data source.

The blue adapters I think should be our priority: Nightlog (summary), ConsDB, Almanac, Jira (but I forgot to star).

Some data sources offer multiple options, but have 'preferred methods' of accessing their data (EFD, ConsDB). ConsDB will prefer the TAP service. EFD will prefer EFDClient.
_To do: move EFDClient to tssw, use ConsDB TAP Service instead of pqserver_

I tried to add our prioritized widgets/applets in the second level, so we could see where they need data from, and prioritize appropriately.

We do not have an Adapter for Jira currently, but Sebastian has knowledge of how to access it properly because they do in LOVE (so we should take from there).

On the very far right, I show what data we need from each source.



```mermaid
graph LR

  REST[REST API] --> Summary
  REST --> TimeLoss
  REST --> ExposuresTaken["Exposures Taken"]
  REST --> Efficiency
  REST --> JiraTickets["Jira Tickets"]

  Summary --> EFDAdapter["EFD Adapter"]
  EFDAdapter --> |"EFDClient.selectTimeSeries()"| EFDClient
  EFDClient --> EFD[(EFD)]
  EFD --> EFDData["Scripts, Errors, Exposures, Telemetry"]

  Summary --> NightlogAdapter["NightLog Adapter"]
  NightlogAdapter -->|GET /usdf/nightlog/| Nightlog[(NightLog)]
  Nightlog --> NLData["Observer Summary"]

  Summary --> ExposureLogAdapter["ExposureLog Adapter"]
  ExposureLogAdapter --> |GET /usdf/explog/msg/| ExposureLog[(ExposureLog)]
  ExposureLog --> ELData["List of exposures, Annotations"]

  TimeLoss --> NarrAdapter["Narr Adapter"]
  NarrAdapter -->|GET /usdf/narralog/msg/| NarrativeLog[(NarrativeLog)]
  NarrativeLog --> NarrData["Observer Comments"]

  ExposuresTaken --> ConsDBAdapter["ConsDB Adapter"]
  ConsDBClient --> ConsDB
  ConsDBAdapter --> |POST /usdf/consdb/query| ConsDBPQServer["ConsDB pqserver"]
  ConsDBTAPService["ConsDB TAP Service"]
  ConsDBPQServer --> ConsDB[(ConsDB)]
  ConsDBTAPService --> ConsDB
  ConsDB --> ConsDBData["Exposures, Times, Calculated Data"]


  Efficiency --> AlmanacAdapter["Almanac Adapter"]
  Efficiency --> ConsDBAdapter
  AlmanacAdapter --> |"Astroplan.Observer()"| Almanac[(Almanac)]
  Almanac --> AlmData["Twilight, Dark hrs, Moon Illum"]

  JiraTickets --> ToDoJira["Needs to be created: Jira Adapter"]
  ToDoJira -->  Jira[(Jira)]
  Jira --> JiraData["Jira Tickets, Statuses"]

  style ConsDBAdapter fill:#003366,stroke:#0055aa
  style NightlogAdapter fill:#003366,stroke:#0055aa
  style AlmanacAdapter fill:#003366,stroke:#0055aa
  style ToDoJira fill:#003366,stroke:#0055aa

  classDef db fill:#3e2a56,stroke:#a080d9,stroke-width:2px,rx:15,ry:15
  class ConsDB,Almanac,DataStore,EFD,ExposureLog,NarrativeLog,Nightlog,Jira db;
```