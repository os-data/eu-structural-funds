/*


 Austria. One scraper for the following six files:

 https://www.salzburg.gv.at/wirtschaft_/Documents/rwf-beguenstigte.pdf

 https://www.vorarlberg.at/pdf/verzeichnisderbeguenstigt.pdf

 http://www.regio13.at/redx/tools/mb_download.php/mid.x596b672f63676a5359416f3d/Verzeichnis.pdf

 http://www.innovation-steiermark.at/de/projekte/verzeichnis/Beguenstigte_25.08.15_R57.pdf

 http://kwf.at/downloads/deutsch/EU/Publizitaet_Kaernten.pdf

 http://www.raumordnung-noe.at/fileadmin/root_raumordnung/land/eu_regionalpolitik/RWB_Niederoesterreich.pdf

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
	var _CHAR = 2;

	var valid = [
		[_TEXT, _TEXT, _VALUE, _CHAR],
		[_TEXT, _TEXT],
		[null, _TEXT],
		[_TEXT]
	];

	var isValue = function (cell) {
		return cell !== null && (cell.indexOf(',') >= 0) && !isNaN(cell.trim().replace(/\./g, '').replace(/\,/g, '.'));
	};

	var isChar = function (cell) {
		return (cell !== null) && (['A', 'G'].indexOf(cell.trim()) >= 0);
	};

	var isText = function (cell) {
		return cell !== null &&
			(!isValue(cell)) && (!isChar(cell));
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
		} else if (type === _CHAR) {
			if (!isChar(cell)) {
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
	for (var i = rows.length - 1; i >= 0; i--) {
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
			var lines = PDFExtract.utils.pageToLines(page, 0.12);
			lines = PDFExtract.utils.extractLines(lines, ['(in Euro)'], ['Druckdatum: ']);
			if (lines.length == 0) {
				console.log('ALARM, page', page.pageInfo.num, 'without data');
			} else if (debug) {
				lines_collect = lines_collect.concat(lines);
				fs.writeFileSync(debugcache + filename + '-' + page.pageInfo.num + '.json', JSON.stringify(lines, null, '\t'));
			}
			// console.log(PDFExtract.utils.xStats(page));
			/*

			 0-150 col 1
			 Begünstigte/r

			 150-300 col 2
			 Bezeichnung des Vorhabens

			 300-390 col 3
			 öffentliche Beteiligung (EU+nationale Kofinanzierung)*

			 390-480 col 4
			 Status
			 G = genehmigt A = ausbezahlt

			 */

			var rows = PDFExtract.utils.extractColumnRows(lines, [280, 600, 710, 10000], 0.12);
			rows = mergeMultiRows(rows).filter(function (row) {
				if (!isValidRow(row)) {
					console.log('ALARM, invalid row', JSON.stringify(row));
					return false;
				} else {
					return true;
				}
			});
			rows_collect = rows_collect.concat(rows);
			next();
		}, function (err) {
			if (err) return console.log(err);
			if (debug) {
				fs.writeFileSync(debugcache + '_' + filename + '.items.json', JSON.stringify(lines_collect, null, '\t'));
				var sl = rows_collect.map(function (row) {
					return JSON.stringify(row);
				});
				fs.writeFileSync(debugcache + '_' + filename + ".rows.json", '[' + sl.join(',\n') + ']');
			}
			var final = rows_collect.map(function (row) {
				return {
					_source: item,
					beneficiary: row[0] || '',
					name_of_the_operation: row[1] || '',
					allocated_public_funding: row[2] || '',
					status: (row[3] == 'G' ? 'commited' : (row[3] == 'A' ? 'paid out' : row[3]))
				};
			});
			fs.writeFileSync(filename + ".json", JSON.stringify(final, null, '\t'));
			cb(err);
		})
	});
};

var scrapeItem = function (item, next) {
	console.log('scraping pdf', item);
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
	'https://www.salzburg.gv.at/wirtschaft_/Documents/rwf-beguenstigte.pdf',
	'https://www.vorarlberg.at/pdf/verzeichnisderbeguenstigt.pdf',
	'http://www.regio13.at/redx/tools/mb_download.php/mid.x596b672f63676a5359416f3d/Verzeichnis.pdf',
	'http://www.innovation-steiermark.at/de/projekte/verzeichnis/Beguenstigte_25.08.15_R57.pdf',
	'http://kwf.at/downloads/deutsch/EU/Publizitaet_Kaernten.pdf',
	'http://www.raumordnung-noe.at/fileadmin/root_raumordnung/land/eu_regionalpolitik/RWB_Niederoesterreich.pdf'
];

async.forEachSeries(list, scrapeItem, function () {
	console.log('done');
});
