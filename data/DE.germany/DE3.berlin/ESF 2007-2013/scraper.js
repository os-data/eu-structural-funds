/*

 https://github.com/os-data/eu-structural-funds/issues/28 hier das ESF 2007-2013 (Berlin)
 https://www.berlin.de/sen/wirtschaft/gruenden-und-foerdern/europaeische-strukturfonds/esf/informationen-fuer-verwaltungen-partner-eu/beguenstigtenverzeichnis_2014.pdf

 */

var scrapyard = require("scrapyard");
var async = require("async");
var path = require("path");
var request = require("request");
var fs = require("fs");
var PDFToolbox = require('../../lib/pdftoolbox');

var _VALUE = PDFToolbox.FIELDS.VALUE1;
var _DATE = PDFToolbox.FIELDS.DATE;
var _TEXT = function (cell) {
	return cell && !_VALUE(cell) && !_DATE(cell);
};
var rowspecs = [
	[_TEXT, _TEXT, _DATE, _VALUE, _VALUE],
	[_TEXT, _TEXT],
	[null, _TEXT],
	[_TEXT]
];

var scrapePDF = function (item, cb) {
	var pdf = new PDFToolbox();
	pdf.scrape(item, {
		debug: true,
		skipPage: [],
		pageToLines: function (page) {
			var lines = PDFToolbox.utils.pageToLines(page, 4);
			if (page.pageInfo.num == 1)
				lines = PDFToolbox.utils.extractLines(lines, ['Projekte (in'], ['-------------'/* take all */]);
			else {
				lines = PDFToolbox.utils.extractLines(lines, ['Begünstigtenverzeichnis '], ['-------------'/* take all */]);
			}
			return lines;
		},
		processLines: function (lines) {
			return lines.map(function (line) {
				if (line.length == 1 && line[0] && line[0].str.indexOf('ANLAGE zum Begünstigten') == 0) {
					return null;
				}
				if (line.length == 1 && line[0] && /\d+ von \d+/.test(line[0].str)) {
					return null;
				}
				if (line.length == 5 && line[0] && (line[0].str == 'Begünstigter')) {
					return null;
				}
				if (line.length == 4 && line[0] && (line[0].str == 'Begünstigter')) {
					return null;
				}
				if (line.length == 3 && line[0] && (line[0].str == 'Datum ')) {
					return null;
				}
				if (line.length == 3 && line[0] && (line[0].str == 'Restzahlung')) {
					return null;
				}
				if (line.length == 9 && line[2] && (['IMA 495.IC0510', 'IMA 495.IC0410'].indexOf(line[2].str) >= 0)) {
					line[1].str = line[1].str + line[2].str;
					return line.splice(2, 1);
				}
				return line;
			}).filter(function (line) {
				return line !== null;
			});
		},
		linesToRows: function (lines) {
			// console.log(PDFExtract.utils.xStats(page));
			/*

			 0-208 col 1
			 Begünstigter

			 208-540 col 2
			 Projektname

			 540-650 col 3
			 Datum Bewilligung / Restzahlung

			 650-700 col 4
			 Bewilligung öffentlicher Mittel (in €)

			 700- col 5
			 Gesamtauszahlungs-betrag abgeschlossener Projekte (in €)

			 */

			var rows = PDFToolbox.utils.extractColumnRows(lines, [200, 500, 600, 680, 1200], 5);

			var headerRows = [
				[null, null, null, "Projekte (in ", "€)"],
				[null, null, null, "Gesamtauszahlungs-"],
				[null, null, "Datum ", "Bewilligung "],
				[null, null, null, null, "betrag "],
				[null, null, null, "abgeschlossener "],
				[null, null, "Restzahlung", "(in €)"],
				[null, null, null, "Projekte (in €)"]
			];
			rows = rows.filter(function (row) {
				for (var i = 0; i < headerRows.length; i++) {
					if (PDFToolbox.utils.validateRow(row, headerRows[i])) {
						return false;
					}
				}
				return true;
			});

			//pdf parser bug
			// ["Aktionsbündnis Ernst-Reuter-Platz","Kapital - Sammelprojekt\")","28.03.2013         7.500,00  €          7.491,00  €"],
			rows.forEach(function (row, i) {
				if (row.length !== 5 && row.filter(function (cell) {
						return (cell && cell.indexOf('   ') > 0);
					}).length > 0) {
					// console.log('woop woop', JSON.stringify(row));
					var newrow = [];
					row.forEach(function (cell) {
						if (cell) {
							if (cell == '(DUB0 167    )')
								newrow.push('(DUB0 167    )');
							else if (cell.indexOf('   ') > 0) {
								var sl = cell.split('   ').filter(function (part) {
									return (part !== null) && (part.trim().length > 0);
								}).map(function (s) {
									return s.trim();
								});
								newrow = newrow.concat(sl);
							} else
								newrow.push(cell);
						}
					});
					if (newrow.length == 4) newrow.unshift(null);
					rows[i] = newrow;
				}
			});

			return rows;
		},
		processRows: function (rows) {
			var rows = rows.filter(function (row) {
				if (!PDFToolbox.utils.isValidRow(row, rowspecs)) {
					console.log('ALARM, invalid row', JSON.stringify(row));
					return false;
				} else {
					return true;
				}
			});
			return PDFToolbox.utils.mergeMultiRowsTopToBottom(rows, 3, [0, 1])
		},
		rowToFinal: function (row) {
			return {
				_source: item,
				beneficiary: row[0] || '',
				name_of_operation: row[1] || '',
				date: row[2] || '',
				allocated_public_funding: row[3] || '',
				on_finish_total_value: row[4] || ''
			};
		},
		processFinal: function (items) {
			return items;
		}
	}, function (err, items) {
		if (err) console.log(err);
		cb();
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
	'https://www.berlin.de/sen/wirtschaft/gruenden-und-foerdern/europaeische-strukturfonds/esf/informationen-fuer-verwaltungen-partner-eu/beguenstigtenverzeichnis_2014.pdf'
];

async.forEachSeries(list, scrapeItem, function () {
	console.log('done');
});
