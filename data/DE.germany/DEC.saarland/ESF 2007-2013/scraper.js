/*

 Saarland ESF 2007-2013
 Download link: http://www.saarland.de/dokumente/tp_sff/Beguenstigtenverzeichnis_13_14.pdf
 Github Referenz: https://github.com/os-data/eu-structural-funds/issues/37

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
	var _DATE = 2;

	var valid = [
		[_TEXT, _DATE, _DATE, _VALUE]
	];

	var isDate = function (cell) {
		return cell !== null && (cell.trim().length == 10) && (/^\d\d\.\d\d\.\d\d\d\d$/.test(cell.trim()));
	};

	var isInt = function (cell) {
		return cell !== null && !isNaN(parseInt(cell, 10)) && (/^\d+$/.test(cell.trim()));
	};


	var isValue = function (cell) {
		return cell !== null &&
			(
				(cell.indexOf(',') >= 0) && !isNaN(cell.replace(/\./g, '').replace(/,/g, '.').trim()) ||
				isInt(cell)
			);
	};

	var isText = function (cell) {
		return cell !== null && (!isValue(cell)) && (!isDate(cell));
	};

	var isType = function (cell, type) {
		if (type === null) {
			if (cell !== null) {
				console.log(cell, 'is not null');
				return false;
			}
		} else if (type === _VALUE) {
			if (!isValue(cell)) {
				console.log(cell, 'is not value');
				return false;
			}
		} else if (type === _DATE) {
			if (!isDate(cell)) {
				console.log(cell, 'is not date');
				return false;
			}
		} else if (type === _TEXT) {
			if (!isText(cell)) {
				console.log(cell, 'is not text');
				return false;
			}
		} else if (typeof type === 'string') {
			if (type !== cell) {
				console.log(cell, 'is not string');
				return false;
			}
		}
		return true;
	};

	var validateRow = function (format, row) {
		if (row.length !== format.length) {
			console.log(row.length, 'is not length ' + format.length);
			console.log(row);
			return false;
		}
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
		if (row.length == 1) {
			var rowbefore = rows[i - 1];
			if (row[0]) {
				if (!rowbefore[0]) rowbefore[0] = row[0];
				else rowbefore[0] = rowbefore[0] + '\n' + row[0];
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
			var alllines = PDFExtract.utils.pageToLines(page, 0.3);
			var lines = PDFExtract.utils.extractLines(alllines, ['Name des Projekte'], ['-------------------'/*take all*/]);
			if (lines.length == 0) lines = PDFExtract.utils.extractLines(alllines, ['Begünstigtenverzeichnis '], ['-------------------'/*take all*/]);
			if (lines.length == 0) {
				console.log('ALARM, page', page.pageInfo.num, 'without data');
			} else if (debug) {
				lines_collect = lines_collect.concat(lines);
				fs.writeFileSync(debugcache + filename + '-' + page.pageInfo.num + '.json', JSON.stringify(lines, null, '\t'));
			}
			// console.log(PDFExtract.utils.xStats(page));

			// console.log(page.pageInfo.num);

			lines = lines.filter(function (line) {
				if ((line.length == 1) && (line[0].str == ' ')) {
					return false;
				}
				if ((line.length == 2) && (!isNaN(parseInt(line[0].str.trim(), 10))) && (line[1].str == ' ')) {
					return false;
				}

				if ((line.length == 4) && (line[1] == null) && (line[3] == null)) {
					return false;
				}
				if ((line.length == 4) && (line[0].str == ' ') && (line[1].str == ' ') && (line[3].str == ' ')) {
					return false;
				}
				return true;
			});
			/*

			 0-450 col 1
			 Name des Begünstigten

			 450-500 col 2
			 Projektlaufzeit: von

			 500-550 col 3
			 Projektlaufzeit: bis

			 700- col 4
			 ESF Mittel

			 */

			var rows = PDFExtract.utils.extractColumnRows(lines, [450, 500, 550, 700, 1200], 0.4);
			rows = rows.map(function (row) {
				return row.filter(function (cell) {
					return (!cell) || (cell !== ' ');
				});
			});
			rows = mergeMultiRows(rows);
			rows_collect = rows_collect.concat(rows);
			next();
		}, function (err) {
			if (err) return console.log(err);

			rows_collect = rows_collect.filter(function (row) {
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
					year_from: row[1] || '',
					year_until: cleanString(row[2]),
					esf_funds: cleanString(row[3])
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
	'http://www.saarland.de/dokumente/tp_sff/Beguenstigtenverzeichnis_13_14.pdf'
];

async.forEachSeries(list, scrapeItem, function () {
	console.log('done');
});
