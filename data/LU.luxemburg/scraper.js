/*

 Web-Scraper for:
 http://www.fonds-europeens.public.lu/fr/projets-cofinances/index.php

 */

var scrapyard = require("scrapyard");
var async = require("async");
var fs = require("fs");
var scraper = new scrapyard({
	debug: true,
	retries: 5,
	connections: 10,
	cache: '../../local/cache',
	bestbefore: "600min"
});

var allitems = [];

var scrapeItem = function (item, next) {
	scraper({
			url: item._source,
			method: "GET",
			type: "html",
			encoding: "utf-8"
		},
		function (err, $) {
			if (err) return console.error(err);
			var box = $('.box--project-summary dl');
			var lastCat = null;
			box.children().each(function (i, elem) {
				var $elem = $(elem);
				if ($elem.is('dt')) {
					lastCat = $elem.text().trim().toLowerCase().replace(':', '').replace(/ /g, '_');
				} else if ($elem.is('dd')) {
					item[lastCat] = $elem.text().trim();
				}
			});

			var box = $('.box--project-details');
			box.children().each(function (i, elem) {
				var $elem = $(elem);
				var lastCat = $('h3', $elem).text().trim().toLowerCase().replace(':', '').replace(/ /g, '_');
				var list = [];
				$('li', $elem).each(function (i, elem) {
					list.push($(elem).text().trim());
				});
				item[lastCat] = list.join('\n');
			});

			allitems.push(item);
			next();
		});
};

var scrapePage = function (page) {
	console.log('scraping page', page);
	scraper(
		{
			url: 'http://www.fonds-europeens.public.lu/fr/projets-cofinances/index.php' + page,
			method: "GET",
			type: "html",
			encoding: "utf-8"
		}
		, function (err, $) {
			if (err) return console.error(err);
			var nextlink = $('.pagination-next a').attr('href');

			var list = $('li.search-result');
			var items = [];
			list.each(function (i, elem) {
				$elem = $(elem);
				var o = {};
				o._source = $('.article-title a', $elem).attr('href');
				o.title = $elem.attr('title');
				items.push(o);
			});


			async.forEachSeries(items, scrapeItem, function () {
				if (nextlink) {
					return scrapePage(nextlink);
				}
				fs.writeFileSync('data.json', JSON.stringify(allitems, null, '\t'));
				console.log('done');
			})

		});
};

scrapePage('');