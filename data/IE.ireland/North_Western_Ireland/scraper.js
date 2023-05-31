/*

 Web-Scraper for:

 http://nwra.ie/operational-programme-2007-2013-2/

 */

var scrapyard = require("scrapyard");
var async = require("async");
var fs = require("fs");
var scraper = new scrapyard({
	debug: true,
	retries: 5,
	connections: 10,
	cache: '../../local/cache',
	bestbefore: "6000min"
});

var allitems = [];
var scrapeItem = function (item, next) {
	console.log('scraping profile', item._source);
	scraper({
			url: item._source,
			method: "GET",
			type: "html",
			encoding: "utf-8"
		},
		function (err, $) {
			if (err) return console.error(err);
			var applyProp = function (s) {
				var parts = s.split(':');
				item[parts[0].trim().toLowerCase().replace(/ /g, '_').replace(/'/g, '_')] = parts.slice(1).join(':').trim();
			};
			$('.group-right .field').each(function (i, elem) {
				applyProp($(elem).text());
			});
			applyProp('Organisatie: ' + $('.group-left .field-name-field-organisation .organization-name').text());
			applyProp('Organisatie street: ' + $('.group-left .field-name-field-organisation .adr .street-address').text());
			applyProp('Organisatie postal code: ' + $('.group-left .field-name-field-organisation .adr .postal-code').text());
			applyProp('Organisatie locality: ' + $('.group-left .field-name-field-organisation .adr .locality').text());
			applyProp('Organisatie url: ' + $('.group-left .field-name-field-organisation .adr .url').text());
			applyProp('Projectverantwoordelijke: ' + $('.group-left .field-name-mf-hcard .family-name').text());
			applyProp('Projectverantwoordelijke tel: ' + $('.group-left .field-name-mf-hcard .tel .value').text());
			applyProp('Projectverantwoordelijke email: ' + $('.group-left .field-name-mf-hcard .email .value').text());
			applyProp('Projectverantwoordelijke email: ' + $('.group-left .field-name-field-partner').text());

			var partners = [];
			$('.group-left .field-name-field-partner li').each(function (i, elem) {
				partners.push($(elem).text());
			});
			applyProp('Partners: ' + partners.join('\n'));

			applyProp($('.group-middle .field-name-field-co-financing-rate-european').text());
			$('.group-middle .group-finance-request .field').each(function (i, elem) {
				applyProp($(elem).text());
			});
			$('.group-middle .group-finance-approval .field').each(function (i, elem) {
				applyProp($(elem).text());
			});
			allitems.push(item);
			next();
		});
};

var scrapeYear = function (year, next) {
	console.log('scraping group', year._source);
	scraper({
			url: year._source,
			method: "GET",
			type: "html",
			encoding: "utf-8"
		},
		function (err, $) {
			if (err) return console.error(err);
			$('.gdl-accordion li').each(function (i, elem) {
				var title = $('.accordion-title', elem).text();
				var rows = [];
				$('.accordion-content table tr').each(function (i, elem) {
					var row = [];
					$('td', elem).each(function (i, elem) {
						row.push($(elem).text());
					});
					rows.push(row);
				});
				var keys = ["reference_of_beneficiary",
					"project_name",
					"amount_allocated"
				];
				rows.forEach(function (row) {
					var o = {_source: year._source, year: year.year, group: title};
					if (row.length == keys.length) {
						for (var i = 0; i < keys.length; i++) {
							o[keys[i]] = row[i];
						}
						if (o["reference_of_beneficiary"] !== "Reference of Beneficiary")
							allitems.push(o);
					} else {
						// console.log(JSON.stringify(row));
					}
				});

			});
			next();
		});
};

var scrapePage = function () {
	var url = 'http://nwra.ie/operational-programme-2007-2013-2/';
	console.log('scraping page', url);
	scraper(
		{
			url: url,
			method: "GET",
			type: "html",
			encoding: "utf-8"
		}
		, function (err, $) {
			if (err) return console.error(err);
			var list = $('.gdl-page-content p a');
			var groups = [];
			list.each(function (i, elem) {
				$a = $(elem);
				var url = $a.attr('href');
				if (url) {
					var o = {};
					o._source = url;
					o._title = $a.text().trim();
					var p = o._title.split(' ');
					o.year = p[p.length - 1];
					groups.push(o);
				}
			});
			async.forEachSeries(groups, scrapeYear, function () {
				fs.writeFileSync('data.json', JSON.stringify(allitems, null, '\t'));
				console.log('done');
			})
		});
};

scrapePage();
