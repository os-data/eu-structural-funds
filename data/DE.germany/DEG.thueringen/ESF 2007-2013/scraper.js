/*

 Thüringen ESF 2007-2013
 Download Link: http://www.esf-thueringen.de/imperia/md/content/esf/dastmwtaundderesf/listederbeguenstigten/liste_der_beguenstigten_2014.pdf
 Github Referenz: https://github.com/os-data/eu-structural-funds/issues/41

 */

var scrapyard = require("scrapyard");
var async = require("async");
var path = require("path");
var request = require("request");
var fs = require("fs");
var PDFExtract = require('pdf.js-extract').PDFExtract;

var debug = false;
var debugcache = '../../local/_pdf/';
if (!fs.existsSync('../../local/_pdf/')) {
	console.log('warning cache folder doesn\'t exists');
}

var isValidRow = function (row) {

	var _VALUE = 0;
	var _TEXT = 1;
	var _YEAR = 2;

	var valid = [
		[_TEXT, _TEXT, _YEAR, _VALUE, _VALUE]
	];

	var isYear = function (cell) {
		if (cell !== null && (cell.indexOf(' ') < 0) && (cell.trim().length == 4) && (/^\d+$/.test(cell.trim()))) {
			var i = parseInt(cell.trim(), 10);
			if (isNaN(i)) return false;
			return i > 1990 && i < 2017;
		}
		return false;
	};

	var isValue = function (cell) {
		return cell !== null && (cell.indexOf(',') >= 0) && !isNaN(cell.replace(/\./g, '').replace(/,/g, '.').trim());
	};

	var isText = function (cell) {
		return cell !== null && (!isValue(cell)) && (!isYear(cell));
	};

	var isType = function (cell, type) {
		if (type === null) {
			if (cell !== null) {
				return false;
			}
		} else if (type === _VALUE) {
			if (!isValue(cell)) {
				return false;
			}
		} else if (type === _YEAR) {
			if (!isYear(cell)) {
				return false;
			}
		} else if (type === _TEXT) {
			if (!isText(cell)) {
				return false;
			}
		} else if (typeof type === 'string') {
			if (type !== cell) {
				return false;
			}
		}
		return true;
	};

	var validateRow = function (format, row) {
		if (row.length !== format.length) return false;
		for (var j = 0; j < row.length; j++) {
			if (!isType(row[j], format[j])) {
				return false;
			}
		}
		return row.length > 0;
	};

	for (var i = 0; i < valid.length; i++) {
		var format = valid[i];
		if (validateRow(format, row)) {
			return true;
		}
	}
	return false;
};

var mergeMultiRows = function (rows) {
	for (var i = rows.length - 1; i > 0; i--) {
		var row = rows[i];
		if (row.length <= 2) {
			var rowbefore = rows[i - 1];
			if (row[0]) {
				if (!rowbefore[0]) rowbefore[0] = row[0];
				else rowbefore[0] = rowbefore[0] + '\n' + row[0];
			}
			if (row[1]) {
				if (!rowbefore[1]) rowbefore[1] = row[1];
				else rowbefore[1] = rowbefore[1] + '\n' + row[1];
			}
			rows[i] = [];
		}
	}
	return rows.filter(function (row) {
		return row.length > 0;
	})
};

var scrapePDF = function (item, cb) {
	var filename = path.basename(item).replace('.pdf', '');
	console.log('scraping pdf', filename);
	var rows_collect = [];
	var lines_collect = [];
	var pdfExtract = new PDFExtract();
	pdfExtract.extract(filename + '.pdf', {}, function (err, data) {
		if (err) return console.log(err);
		if (debug)
			fs.writeFileSync(debugcache + filename + '.pdf.json', JSON.stringify(data, null, '\t'));
		async.forEachSeries(data.pages, function (page, next) {
			if (page.pageInfo.num == 1) return next();
			var alllines = PDFExtract.utils.pageToLines(page, 0.3);
			var lines = PDFExtract.utils.extractLines(alllines, ['Name des Begünstigten'], ['-------------------'/*take all*/]);
			if (lines.length == 0)
				lines = PDFExtract.utils.extractLines(alllines, ['Begünstigten'], ['-------------------'/*take all*/]);
			if (lines.length == 0) {
				console.log('ALARM, page', page.pageInfo.num, 'without data');
			} else if (debug) {
				lines_collect = lines_collect.concat(lines);
				fs.writeFileSync(debugcache + filename + '-' + page.pageInfo.num + '.json', JSON.stringify(lines, null, '\t'));
			}
			// console.log(PDFExtract.utils.xStats(page));
			/*

			 0-150 col 1
			 Name des Begünstigten

			 150-500 col 2
			 BEZEICHNUNG DES VORHABENS

			 500-600 col 3
			 JAHR DER BEWILLIGUNG / RESTZAHLUNG

			 600-760 col 4
			 Bewilligter Betrag

			 760- col 5
			 BEI ABSCHLUSS DES VORHABENS GEZAHLTE GESAMTBETRÄGE

			 */

			// console.log(page.pageInfo.num);
			var offset1 = 0;
			var offset3 = 0;
			var offset4 = 0;
			if (page.pageInfo.num == 24) {
				offset3 = -5;
			} else if (page.pageInfo.num == 144) {
				offset3 = -20;
			} else if (page.pageInfo.num == 175) {
				offset3 = -30;
				offset4 = -10;
			} else if (page.pageInfo.num == 309) {
				offset3 = -20;
			} else if (page.pageInfo.num == 364) {
				offset3 = -10;
			} else if (page.pageInfo.num == 392) {
				offset3 = -20;
			} else if (page.pageInfo.num == 396) {
				offset3 = -40;
				offset4 = -15;
			} else if ((page.pageInfo.num == 56) || (page.pageInfo.num == 57)) {
				offset3 = -50;
				offset4 = -25;
			} else if (page.pageInfo.num == 515) {
				offset3 = -20;
			} else if (page.pageInfo.num == 582) {
				offset3 = -10;
			} else if (page.pageInfo.num == 583) {
				offset3 = -30;
			} else if (page.pageInfo.num == 584) {
				offset3 = -35;
				offset4 = -10;
			} else if (page.pageInfo.num == 590) {
				offset3 = -50;
				offset4 = -25;
			} else if (page.pageInfo.num == 600) {
				offset3 = -60;
				offset4 = -30;
			} else if (page.pageInfo.num == 601) {
				offset3 = -20;
			} else if (page.pageInfo.num == 602) {
				offset3 = -20;
			} else if (page.pageInfo.num == 603) {
				offset3 = -20;
			} else if (page.pageInfo.num == 657) {
				offset3 = -50;
				offset4 = -20;
			} else if (page.pageInfo.num == 678) {
				offset3 = -20;
			} else if (page.pageInfo.num == 721) {
				offset3 = -40;
				offset4 = -15;
			} else if (page.pageInfo.num == 801) {
				offset3 = -40;
				offset4 = -55;
			} else if (page.pageInfo.num == 802) {
				offset3 = -20;
				offset4 = -25;
			} else if (page.pageInfo.num == 119) {
				offset1 = -10;
			} else if (page.pageInfo.num == 120) {
				offset1 = -10;
			} else if (page.pageInfo.num == 121) {
				offset1 = -10;
			} else if (page.pageInfo.num == 134) {
				offset1 = -40;
			} else if (page.pageInfo.num == 135) {
				offset1 = -40;
			} else if (page.pageInfo.num == 136) {
				offset1 = -40;
			} else if (page.pageInfo.num == 137) {
				offset1 = -40;
			} else if ((page.pageInfo.num >= 371) && (page.pageInfo.num <= 400)) {
				offset1 = -40;
			} else if ((page.pageInfo.num >= 459) && (page.pageInfo.num <= 470)) {
				offset1 = -40;
			} else if (page.pageInfo.num == 707) {
				offset1 = -50;
			} else if ((page.pageInfo.num >= 736) && (page.pageInfo.num <= 737)) {
				offset1 = -40;
			} else if (page.pageInfo.num == 738) {
				offset1 = -50;
			} else if ((page.pageInfo.num >= 739) && (page.pageInfo.num <= 740)) {
				offset1 = -40;
			} else if ((page.pageInfo.num >= 835) && (page.pageInfo.num <= 836)) {
				offset1 = -40;
			} else if ((page.pageInfo.num >= 879) && (page.pageInfo.num <= 883)) {
				offset1 = -40;
			}
			var rows = PDFExtract.utils.extractColumnRows(lines, [140 + offset1, 500, 670 + offset3, 725 + offset4, 1200], 0.2);
			rows_collect = rows_collect.concat(rows);
			next();
		}, function (err) {
			if (err) return console.log(err);

			rows_collect = mergeMultiRows(rows_collect).filter(function (row) {
				if (!isValidRow(row)) {
					console.log('ALARM, invalid row', JSON.stringify(row));
					return false;
				} else {
					return true;
				}
			});

			if (debug) {
				fs.writeFileSync(debugcache + '_' + filename + '.items.json', JSON.stringify(lines_collect, null, '\t'));
				var sl = rows_collect.map(function (row) {
					return JSON.stringify(row);
				});
				fs.writeFileSync(debugcache + '_' + filename + ".rows.json", '[' + sl.join(',\n') + ']');
			}
			var cleanString = function (cell) {
				return (cell || '').trim();
			};


			var final = rows_collect.map(function (row) {
				return {
					_source: item,
					beneficiary: row[0] || '',
					name_of_operation: row[1] || '',
					years: row[2] || '',
					allocated_public_funding: cleanString(row[3]),
					on_finish_total_value: cleanString(row[4])
				};
			});
			fs.writeFileSync(filename + ".json", JSON.stringify(final, null, '\t'));
			cb(err);
		})
	});
};

var scrapeItem = function (item, next) {
	var filename = path.basename(item);
	if (!fs.existsSync(filename)) {
		console.log('scraping doc', item);
		var req = request(item);
		var stream = req.pipe(fs.createWriteStream(filename));
		stream.on('close', function () {
			scrapePDF(item, next);
		});
	} else {
		scrapePDF(item, next);
	}
};

var list = [
	'http://www.esf-thueringen.de/imperia/md/content/esf/dastmwtaundderesf/listederbeguenstigten/liste_der_beguenstigten_2014.pdf'
];

async.forEachSeries(list, scrapeItem, function () {
	console.log('done');
});
