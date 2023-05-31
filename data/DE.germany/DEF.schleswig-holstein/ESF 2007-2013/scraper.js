/*

 ESF Data Schleswig Holstein 2007-2013
 Download link: http://www.ib-sh.de/fileadmin/user_upload/downloads/Arbeit_Bildung/ZP_Arbeit/allgemein/vdb.pdf
 Startseite: http://www.ib-sh.de/die-ibsh/foerderprogramme-des-landes/landesprogramm-arbeit/
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
		[_TEXT, _TEXT, _YEAR, _VALUE, null],
		[_TEXT, _TEXT, _YEAR, null, _VALUE],
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
		return cell !== null && (cell.indexOf(',') >= 0) && !isNaN(cell.replace(/\./g, '').replace(/\,/g, '.').replace(/€/g, '').trim());
	};

	var isText = function (cell) {
		return cell !== null && (!isValue(cell));
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

var mergeMultiLines = function (lines) {

	var getY = function (line) {
		for (var i = 0; i < line.length; i++) {
			if (line[i]) return line[i].y;
		}
		return -1;
	};

	var result = [];
	for (var i = 0; i < lines.length; i++) {
		var line = lines[i];
		if (line.length == 0) {
			//this line is done
		} else if (line.length > 3) {
			//use the middle lines with a year to find multiline before & after
			var y = getY(line);
			var group_lines = [];
			var ydown = y;
			for (var j = i - 1; j >= 0; j--) {
				var linedown = lines[j];
				if ((linedown.length === 5) || (linedown.length === 0)) {
					break;
				} else {
					var ybefore = getY(linedown);
					var diff = ydown - ybefore;
					if (diff < 15) {
						lines[j] = [];
						group_lines.unshift(linedown);
					} else {
						break;
					}
					ydown = ybefore;
				}
			}
			group_lines.push(line);
			var yup = y;
			for (var j = i + 1; j < lines.length; j++) {
				var lineup = lines[j];
				if ((lineup.length === 5) || (lineup.length === 0)) {
					break;
				} else {
					var yafter = getY(lineup);
					var diff = yafter - yup;
					if (diff < 15) {
						lines[j] = [];
						group_lines.push(lineup);
					} else {
						break;
					}
					yup = yafter;
				}
			}
			var newline = [null, null, null, null, null];
			group_lines.forEach(function (gline) {
				for (var j = 0; j < newline.length; j++) {
					if (gline[j]) {
						if (!newline[j]) newline[j] = gline[j].str;
						else newline[j] = newline[j] + '\n' + gline[j].str;
					}
				}
			});
			lines[i] = [];
			result.push(newline);
		}
	}
	lines.forEach(function (line) {
		if (line.length > 0) {
			if (line[0] &&
				((line[0].str.indexOf('1) In diesem Betrag enthalten') == 0) || (line[0].str.indexOf('2) Der ausgezahlte Betrag wird') == 0))
			) {
				//ignore doc footer
			} else {
				console.log('warn lost line', line.length, JSON.stringify(line));
			}
		}
	});
	return result;
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
			var lines = PDFExtract.utils.pageToLines(page, 4);
			lines = PDFExtract.utils.extractLines(lines, ['Restzahlung'], ['Seite ']);

			/*

			 0-150 col 1
			 NAME DES/DER BEGÜNSTIGSTEN

			 150-300 col 2
			 BEZEICHNUNG DES VORHABENS

			 300-390 col 3
			 JAHR DER BEWILLIGUNG / RESTZAHLUNG

			 390-480 col 4
			 GEWÄHRTE BETRÄGE

			 480-
			 BEI ABSCHLUSS DES VORHABENS GEZAHLTE GESAMTBETRÄGE

			 */
			// console.log(PDFExtract.utils.xStats(page));

			var lines = PDFExtract.utils.extractColumnLines(lines, [250, 520, 590, 750, 1000], 0.12);

			if (lines.length == 0) {
				console.log('ALARM, page', page.pageInfo.num, 'without data');
			} else if (debug) {
				lines_collect = lines_collect.concat(lines);
				fs.writeFileSync(debugcache + filename + '-' + page.pageInfo.num + '.json', JSON.stringify(lines, null, '\t'));
			}
			var rows = mergeMultiLines(lines).filter(function (row) {
				if (!isValidRow(row)) {
					console.log('ALARM, invalid row', JSON.stringify(row));
					return false;
				}
				return true;
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
	'http://www.ib-sh.de/fileadmin/user_upload/downloads/Arbeit_Bildung/ZP_Arbeit/allgemein/vdb.pdf'
];

async.forEachSeries(list, scrapeItem, function () {
	console.log('done');
});
