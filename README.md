[![Build Status](https://travis-ci.org/os-data/eu-structural-funds.svg?branch=master)](https://travis-ci.org/os-data/eu-structural-funds)

[![Stories in Ready](https://badge.waffle.io/os-data/eu-structural-funds.png?label=ready&title=Ready)](https://waffle.io/os-data/eu-structural-funds)

# EU Subsidies Data

## Project overview

Structural and Cohesion Funds are financial tools set up to implement the regional policy of the European Union. They aim to reduce regional disparities in income, wealth and opportunities. The overall budget for the 2007-2013 period was __€347 billion__ [according to wikipedia](https://en.wikipedia.org/wiki/Structural_Funds_and_Cohesion_Fund). 

This project is a collaborative effort between [Open-Knowledge Germany](https://www.okfn.de/en/), [Open-Knowledge International](http://okfn.org/) and a number of citizens, journalists and developers. You can join us on our public [Slack channel](https://alleusubsidydata.slack.com/messages/general/).

This repository is a __data pipeline__. It channels beneficiary data from various national or regional sources into the [Open-Spending](http:next.openspending.org) datastore. The goal is to provide a unified european dataset for all subsidies funds for at least two funding periods: 2007-3013 and 2014-2020.

## What's in the repository?

The repository contains the following:

- `codelists/`: codelist information (fields of type *category*) 
- `common/`: python code, including reusable project-wide pipeline processors
- `data/`: data sources broken down by [NUTS code](http://ec.europa.eu/eurostat/web/nuts/overview)
- `geography/`: geographical data (list of NUTS codes and associated shape files) 
- `documentation/`: project documentation
- `specifications/`: source description and Fiscal Data Package files


## How do I contribute?

There are many ways to contribute, depending on your skills. You can:

- Source, document and download datasets
- Send FOAI requests for missing datasets
- Describe, translate and clean-up datasets
- Write python spiders to scrape datasets
- Write python ETL scripts to automate processes
- Raise issues about bugs in the code or the data

## Further information


- [Original project spreadsheet](https://docs.google.com/spreadsheets/d/1RkC_YuWWlhGxyDRc-bpD_zaWAXK78GpPr8nfPesQfSY/edit?pref=2&pli=1#gid=0)
- [Project wiki](https://github.com/os-data/eu-structural-funds/wiki/)
- [Slack channels](https://followthesubsidies.slack.com)
- [GitHub issues](https://github.com/os-data/eu-structural-funds/issues)
