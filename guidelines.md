# Repository specifications

In order to automize some of the processes, it's necessary to agree onwe have agreed to organise the repository in the follwowing way.

## What are the files in the top level folder?

Well

- `datapackage.yml`: 
- __Data folders__: the folder hierarchy is follows the *NUTS* 
- `codelist.yml`: the list of fields that that we expect from publicly released data
- `datapackage.fiscal.json`: the specification for the fiscal data packages loaded into Open-Spending
- `nuts.`

## How do I add a new source of data?



## Codelist

The codelist is the ![Codelist](codelist.png "Required fields")

The codelists are in the following [google spreadsheet](https://docs.google.com/spreadsheets/d/1hvvajnagxtgzZ-I4SWarTCKfzVGko9ylKR_cJxTrgTo/edit?usp=sharing).

## Geography

We use the official *NUTS* nomenclature found on [Eurostat](http://ec.europa.eu/eurostat/portal/page/portal/nuts_nomenclature/introduction). For geo-wireframing data (useful for making maps), check-out [this page](http://ec.europa.eu/eurostat/portal/page/portal/nuts_nomenclature/introduction). 

The bottom line is that the raw data must be in a sub-folder that maps the NUTS structure. That means that we can collect raw data at any level of granularity, as long as we put it in the correct sub-folder. 

Organising the geography properly will ultimately ensure that we can put things on a map.