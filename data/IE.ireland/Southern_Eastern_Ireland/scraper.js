/*

 Web-Scraper for:

 www.southernassembly.ie/en/project/view_projects

 http://www.southernassembly.ie/en/project/view_projects/P0
	....
 http://www.southernassembly.ie/en/project/view_projects/P5393

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
			item['summary'] = $($('#col2')).text().trim();
			var box = $('p');
			box.each(function (i, elem) {
				var $elem = $(elem);
				var values = $elem.text().trim().split(':');
				var cat = values[0].trim().toLowerCase().replace(':', '').replace(/ /g, '_').replace(/\//g, '_');
				var value = values.slice(1).join(':').trim();
				item[cat] = value;
			});
			allitems.push(item);
			next();
		});
};

var scrapePage = function (page) {
	console.log('scraping page', 'http://www.southernassembly.ie/en/project/view_projects/P' + page);
	scraper(
		{
			url: 'http://www.southernassembly.ie/en/project/view_projects/P' + page,
			method: "GET",
			type: "html",
			encoding: "utf-8"
		}
		, function (err, $) {
			if (err) return console.error(err);
			var list = $('#view_projects tr');
			var items = [];
			list.each(function (i, elem) {
				$elem = $(elem);
				$a = $($('a', $elem)[0]);
				var o = {};
				o._source = $a.attr('href');
				o.title = $a.text().trim();
				if (o._source)
					items.push(o);
			});
			async.forEachSeries(items, scrapeItem, function () {
				if (page < 5393) {
					return scrapePage(page + 30);
				}
				fs.writeFileSync('data.json', JSON.stringify(allitems, null, '\t'));
				console.log('done');
			})
		});
};

scrapePage(0);
