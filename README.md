# EU Subsidies Data

Subsidystories.eu intends to increase transparency of EU-Funds by unravelling how the European Structural Investment Funds are spent. We collected and standardised all available beneficiary lists for the ERDF, ESF and CF for the 2007-2013 and 2014-2020 funding periods. These funds are partially funded by the EU and the Member States and allocated by regional authorities to local beneficiaries [policy](https://github.com/os-data/eu-structural-funds/blob/master/documentation/subsidyreport%20-%20policy%20and%20data.pdf) . Find out who has received funding in your country on [Subsidy Stories](http://subsidystories.eu/).

The European Structural and Cohesion Funds (ESIF) are the main financial instrumentfor the implementation of the EU's regional policy. They cover 44 % of the overall 1082 Billion 7-year EU budget. 

For the funding period 2007 - 2013 the budget for the European Regional Development Fund, the European Social Fund, and the Cohesion Fund together accounted for __€347 billion__. In the funding period 2014 - 2020 the budget is: __€477 billion__

## Project overview

This repo started while working on eu subsidy data on several hackatons over the summer of 2016. In October, Addessium funded this project and a data-pipeline was built to ingest all datasets in [Open-Spending](http:next.openspending.org). A front-end was built on [Subsidy Stories](http://subsidystories.eu/) to visualise and explore the data. 

This repository is an intergral part of the project. It contains the documentation, the raw data, the code and additional information that has been gathered in the project. 

## What's in the repository?

The repository contains the following:

- `codelists/`: codelist information (fields of type *category*) 
- `common/`: python code, including reusable project-wide pipeline processors
- `data/`: data sources broken down by [NUTS code](http://ec.europa.eu/eurostat/web/nuts/overview)
- `geography/`: geographical data (list of NUTS codes and associated shape files) 
- `documentation/`: project documentation
- `specifications/`: source description and Fiscal Data Package files

## Further information

- [Original project spreadsheet](https://docs.google.com/spreadsheets/d/1RkC_YuWWlhGxyDRc-bpD_zaWAXK78GpPr8nfPesQfSY/edit?pref=2&pli=1#gid=0)
- [Project wiki](https://github.com/os-data/eu-structural-funds/wiki/)
- [Slack channels](https://followthesubsidies.slack.com)
- [GitHub issues](https://github.com/os-data/eu-structural-funds/issues)

## Project Partners

This project is a collaborative effort between [Open-Knowledge Germany](https://www.okfn.de/en/), [Open-Knowledge International](http://okfn.org/) and a number of citizens, journalists and developers. You 
