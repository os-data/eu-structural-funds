# EU Cohesion and Structural Funds 

## Introduction

Structural and Cohesion Funds are financial tools set up to implement the regional policy of the European Union. They aim to reduce regional disparities in income, wealth and opportunities. The overal budget for the 2007-2013 period was €347 billion [wikipedia](https://en.wikipedia.org/wiki/Structural_Funds_and_Cohesion_Fund).

## Overview

This project is a collaborative effort between Open-Knowledge-Germany, journalists and developers. The repository is pipelines information about fund beneficiaries into the [Open-Spending](http:next.openspending.org) datastore.


The goal of the project collect all benis to get an overview of all these datasets (not just the portals) in one place, clean the data, and map them in one format. 

We have previously collect them [here][gdoc]

[gdoc]: https://docs.google.com/spreadsheets/d/1RkC_YuWWlhGxyDRc-bpD_zaWAXK78GpPr8nfPesQfSY/edit?pref=2&pli=1#gid=0


## Philosophy

The data pipeline is a fine blend of manual and automated processing. We try to adhere to the following principles:

- Be
- Avoid over-engineering the pipeline automation 
- Use [Frictionless-Data](http://www.frictionlessdata.io/) standards wherever possible

## Manual work



## ETL

__ETL__ means Extact-Tranform-Load:

- Extract is extracting raw data from the web. 
- Transforming is cleaning and organising.
- Load is uploading into openspending

The 

## How to contribute?



Per country or region, add the data files in the raw folder (in the country or region directories) and provide a description in a 'yaml directory'. You can copy this yaml example file and just fill it in and save it in the right directory: [README.yaml](https://github.com/os-data/eu-structural-funds/blob/master/example.yaml)


## Codelist

![Codelist](codelist.png "Required fields")

The codelists are in the following [google spreadsheet](https://docs.google.com/spreadsheets/d/1hvvajnagxtgzZ-I4SWarTCKfzVGko9ylKR_cJxTrgTo/edit?usp=sharing).

## Geography

We use the official *NUTS* nomenclature found on [Eurostat](http://ec.europa.eu/eurostat/portal/page/portal/nuts_nomenclature/introduction). For geo-wireframing data (useful for making maps), check-out [this page](http://ec.europa.eu/eurostat/portal/page/portal/nuts_nomenclature/introduction). 

The bottom line is that the raw data must be in a sub-folder that maps the NUTS structure. That means that we can collect raw data at any level of granularity, as long as we put it in the correct sub-folder. 

Organising the geography properly will ultimately ensure that we can put things on a map.




