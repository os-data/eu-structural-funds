/*

 ERDF Data NRW 2007-2013
 Download Link: http://www.ziel2.nrw.de/1_NRW-EU_Ziel_2_Programm_2007-2013/3_Ergebnisse/Verzeichnis_Beguenstigte_2014_12_31.pdf
 Startseite: https://www.efre.nrw.de/

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
		[_TEXT, _TEXT, _YEAR, _VALUE, _VALUE],
		[_TEXT, _TEXT, _YEAR, _VALUE],
		// [_TEXT, _TEXT, _TEXT, null, _VALUE],
		// [_TEXT, _TEXT, _TEXT, _VALUE],
		// [_TEXT, _TEXT],
		// [null, _TEXT],
		// [_TEXT]
	];

	var isYear = function (cell) {
		if (cell !== null && (cell.indexOf(' ') < 0) && (cell.trim().length == 4) && (/^\d+$/.test(cell))) {
			var i = parseInt(cell.trim(), 10);
			if (isNaN(i)) return false;
			return i > 1990 && i < 2017;
		}
		return false;
	};

	var isValue = function (cell) {
		if (cell !== null && /^\d+$/.test(cell.replace(/\./g, '').trim())) {
			var i = parseInt(cell.replace(/\./g, '').trim(), 10);
			if (isNaN(i)) return false;
			return true;// i < 1990 || i > 2017;
		}
		return false;
	};

	var isText = function (cell) {
		return cell !== null && (!isValue(cell)) && (!isYear(cell));
	};

	var isType = function (cell, type) {
		if (type === null) {
			if (cell !== null) {
				return false;
			}
		} else if (type === _YEAR) {
			if (!isYear(cell)) {
				return false;
			}
		} else if (type === _VALUE) {
			if (!isValue(cell)) {
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
	for (var i = 0; i < rows.length - 1; i++) {
		var row = rows[i];
		if (row.length < 4) {
			var rowafter = rows[i + 1];
			if (row[0]) {
				if (!rowafter[0]) rowafter[0] = row[0];
				else rowafter[0] = row[0] + '\n' + rowafter[0];
			}
			if (row[1]) {
				if (!rowafter[1]) rowafter[1] = row[1];
				else rowafter[1] = row[1] + '\n' + rowafter[1];
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
			var lines = PDFExtract.utils.pageToLines(page, 0.3);
			if (page.pageInfo.num == 1)
				lines = PDFExtract.utils.extractLines(lines, ['der Restzahlung'], ['Seite ']);
			else
				lines = PDFExtract.utils.extractLines(lines, ['Stand '], ['Seite ']);
			if (lines.length == 0) {
				console.log('ALARM, page', page.pageInfo.num, 'without data');
			} else if (debug) {
				lines_collect = lines_collect.concat(lines);
				fs.writeFileSync(debugcache + filename + '-' + page.pageInfo.num + '.json', JSON.stringify(lines, null, '\t'));
			}
			// console.log(PDFExtract.utils.xStats(page));
			/*

			 0-208 col 1
			 Name des Begünstigten

			 208-540 col 2
			 BEZEICHNUNG DES VORHABENS

			 540-650 col 3
			 JAHR DER BEWILLIGUNG / RESTZAHLUNG

			 650- col 4
			 Bewilligter Betrag
			 // string merged with
			 BEI ABSCHLUSS DES VORHABENS GEZAHLTE GESAMTBETRÄGE

			 */

			var rows = PDFExtract.utils.extractColumnRows(lines, [208, 540, 650, 1200], 0.6);

			rows.forEach(function (row) {

				[0, 2, 3].forEach(function (i) {
					if (row[i] && row[i].indexOf('    ') >= 0) {
						var parts = row[i].split('    ').filter(function (part) {
							return part.trim().length > 0;
						});
						if (parts.length !== 2) console.log('warning multiple merge cell!', row);
						else {
							row[i] = parts[0].trim();
							row[i + 1] = (parts[1] + (row[i + 1] || '')).trim();
						}
					}
				});
			});

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
	'http://www.ziel2.nrw.de/1_NRW-EU_Ziel_2_Programm_2007-2013/3_Ergebnisse/Verzeichnis_Beguenstigte_2014_12_31.pdf'
];

async.forEachSeries(list, scrapeItem, function () {
	console.log('done');
});
