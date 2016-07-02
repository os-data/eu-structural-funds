# EU Cohesion and Structural Funds 

## Overview

Structural and Cohesion Funds are financial tools set up to implement the regional policy of the European Union. They aim to reduce regional disparities in income, wealth and opportunities. The overal budget for the 2007-2013 period was €347 billion [wikipedia](https://en.wikipedia.org/wiki/Structural_Funds_and_Cohesion_Fund).

This repository is a data pipeline. It collects information about the beneficiaries of EU Cohesion and Structural Funds and loads  [Open-Spending](http:next.openspending.org) datastore.The goal of the project find out  all benis to get an overview of all these datasets (not just the portals) in one place, clean the data, and map them in one format. 

This project is a collaborative effort between [Open-Knowledge-Germany](https://www.okfn.de/en/), [Open-Knowledge International](http://okfn.org/) and a number of journalists and developers. This repository builds on previous research and collection work that was organised inside a [google spreadsheet](https://docs.google.com/spreadsheets/d/1RkC_YuWWlhGxyDRc-bpD_zaWAXK78GpPr8nfPesQfSY/edit?pref=2&pli=1#gid=0). 

## Pipeline architeture

### Philosophy

The data pipeline is a fine blend of manual and automated processing. We try to adhere to the following principles:

- Good communication between 
- Automize the pipeline as much as possible but avoid over-engineering automation 
- Use [Frictionless-Data](http://www.frictionlessdata.io/) standards wherever possible

### What humans do

- Colect 

### What the machines do
__ETL__ means Extact-Tranform-Load:

- Extract is extracting raw data from the web. 
- Transforming is cleaning and organising.
- Load is uploading into openspending

The 

## How to contribute?

There is still Per country or region, add the data files in the raw folder (in the country or region directories) and provide a description in a 'yaml directory'. You can copy this yaml example file and just fill it in and save it in the right directory: [README.yaml](https://github.com/os-data/eu-structural-funds/blob/master/example.yaml)






