# Cohesion and Structural funds datasets

## Intro

This is a collecting the datasets for the EU Structural and Cohesion funds from all the different national and regional portals. The goal is to get an overview of all these datasets (not just the portals) in one place, clean the data, and map them in one format. 

We have previously collect them [here][gdoc]

[gdoc]: https://docs.google.com/spreadsheets/d/1RkC_YuWWlhGxyDRc-bpD_zaWAXK78GpPr8nfPesQfSY/edit?pref=2&pli=1#gid=0

## How to contribute?

Per country or region, add the data files in the raw folder (in the country or region directories) and provide a description in a 'yaml directory'. You can copy this yaml example file and just fill it in and save it in the right directory: [README.yaml](http://https://github.com/os-data/eu-structural-funds/blob/master/example.yaml)

## Technical philosophy

The data pipeline should be automized but kept as simple as possible. 

## ETL

Means Extact Tranform and Load

- Extract is extracting raw data from the web. 
- Transforming is cleaning and organising.
- Load is uploading into openspending

## Codelist

The codelists are in the following [google spreadsheet](https://docs.google.com/spreadsheets/d/1hvvajnagxtgzZ-I4SWarTCKfzVGko9ylKR_cJxTrgTo/edit?usp=sharing).

## Geography

We use the official *NUTS* nomenclature found on [Eurostat](http://ec.europa.eu/eurostat/portal/page/portal/nuts_nomenclature/introduction). For geo-wireframing data (useful for making maps), check-out [this page](http://ec.europa.eu/eurostat/portal/page/portal/nuts_nomenclature/introduction). 

The bottom line is that the raw data must be in a sub-folder that maps the NUTS structure. That means that we can collect raw data at any level of granularity, as long as we put it in the correct sub-folder. 

Organising the geography properly will ultimately ensure that we can put things on a map.




