

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html lang="EN">
<head>
	
	<meta http-equiv="X-UA-Compatible" content="IE=edge,chrome=1" />
	<meta http-equiv="content-type" content="text/html;charset=UTF-8">
	<meta name="Reference" content="COMM/ESTAT/RAMON/EN">
	<meta name="Title" content="Europa - RAMON - Classifications Download List">
	<meta name="Creator" content="COMM/ESTAT">
	<meta name="Language" content="EN">
	<meta name="Type" content="55">
	<meta name="Classification" content="00">
	<meta name="Keywords" content="europa, eu, ramon, classification, classifications, nomenclature server, international statistical classifications">
	<meta name="Description" content="Eurostat's classifications server aims at making available as much information as possible relating to the main international statistical classifications in various fields: economic analysis, environment, education, occupations, national accounts, etc.">
	
	<title>Europa - RAMON - Classifications Download List</title>
	
	<link rel="stylesheet" href="http://ec.europa.eu/eurostat/ramon/comm/stylesheets/commission.css" type="text/css" media="screen">
	<link rel="stylesheet" href="http://ec.europa.eu/eurostat/ramon/stylesheets/ramon.css" type="text/css" media="screen">
	
	
	<script language="javascript" src="http://ec.europa.eu/eurostat/ramon/comm/scripts/commission.js" type="text/javascript"></script>
	<script language="javascript" src="http://ec.europa.eu/eurostat/ramon/scripts/help.js" type="text/javascript"></script>
	<script language="JavaScript">
		function changeLanguage() {
			window.location.href = 'http://ec.europa.eu/eurostat/ramon/index.cfm?TargetUrl=ACT_OTH_SELECT_LANGUAGE&Language=' + document.LangForm.Language.value + '&path_info=' + escape(window.document.location.href);
			//window.location.href = 'http://ec.europa.eu/eurostat/ramon/index.cfm?TargetUrl=ACT_OTH_SELECT_LANGUAGE&Language=' + document.LangForm.Language.value + '&path_info=&qs=TargetUrl=LST_CLS_DLD&StrNom=NUTS_2013L&StrLanguageCode=EN&StrLayoutCode=HIERARCHIC';
		}
	</script>
</head>

<body onLoad="ResetShortcuts()">
<a name="top"></a>

<div id="wrapper" style="background: url(http://ec.europa.eu/eurostat/ramon/images/background_fullwidth.png) left top no-repeat
	,url(http://ec.europa.eu/eurostat/ramon/images/background_fullwidth_right.png) left top repeat-x;">
	<form name="LangForm" method="post" action="" style="margin-bottom:0">
	<header id="banner" role="banner">
		<div id="heading">
			<div id="signin"></div>
			<div id="links">
					<a href="http://ec.europa.eu/geninfo/legal_notices_en.htm">Legal notice</a>
					| <a href="mailto:estat-ramon@ec.europa.eu">Contact</a>
					
					
					<script type="text/javascript" language="JavaScript">
						document.write('&nbsp; <select name="Language" onChange="changeLanguage()" class="entryField">');
						
							document.write('<option value="DE" >Deutsch&nbsp;(de)<\/option>');
						
							document.write('<option value="EN" selected>English&nbsp;(en)<\/option>');
						
							document.write('<option value="FR" >Français&nbsp;(fr)<\/option>');
						
						document.write('<\/select>');
					</script>
					
			</div>

			<div id="title">
				<span id="env">&nbsp;</span>
				<h1>RAMON - Reference And Management Of Nomenclatures</h1>
			</div>
		</div>

		<div id="breadcrumbs">
			<ul aria-label="Breadcrumb" class="breadcrumb">
				<a href="http://ec.europa.eu/index_en.htm">European Commission</a>
				&gt; <a href="http://ec.europa.eu/eurostat">Eurostat</a>

				
					
					&gt; <strong><a href="http://ec.europa.eu/eurostat/ramon/index.cfm?TargetUrl=DSP_PUB_WELC" title="158.167.241.167:6081">RAMON</a></strong>

					
								&gt; <a href="http://ec.europa.eu/eurostat/ramon/index.cfm?TargetUrl=DSP_PUB_WELC">Metadata</a>
							
			</ul>
		</div>

	</header>
	</form>







<table border="0" cellspacing="0" cellpadding="0" width="100%">
	<tr height="20"><td align="right"><table border="0" cellspacing="0" cellpadding="0"><tr height="20" >
		
			
			<td class="menu">
				<a href="http://ec.europa.eu/eurostat/ramon/foreword/index.cfm?targetUrl=DSP_FOREWORD" class="menu" title="Introduction">Introduction</a>
			</td>
			<td class="menu">&nbsp;|&nbsp;</td>
		

		<td class="menuselected">
			<a href="http://ec.europa.eu/eurostat/ramon/index.cfm?TargetUrl=DSP_PUB_WELC" class="menuselected" title="Classifications, concepts and definitions, legislation, methodology, glossaries, thesauri, etc.">Metadata</a>
		</td>

		<td class="menu">&nbsp;|&nbsp;</td>
		<td class="menu">
			<a href="http://ec.europa.eu/eurostat/ramon/relations/index.cfm?TargetUrl=LST_REL" class="menu" title="List and content of the correspondence tables stored in this server.">Correspondence Tables</a>
		</td>

		<td class="menu">&nbsp;|&nbsp;</td>
		<td class="menu">
			<a href="http://ec.europa.eu/eurostat/ramon/search/index.cfm?TargetUrl=SRH_LABEL" class="menu" title="Search the database by keywords">Search Engine</a>
		</td>

		

		
			<td class="menu">&nbsp;|&nbsp;</td>
			
			<td class="menu">
				<a href="http://ec.europa.eu/eurostat/ramon/new/index.cfm?TargetUrl=DSP_NEW" class="menu" title="">What's new ?</a>
			</td>
		

		<td class="menu">&nbsp;</td>
	</tr></table></td></tr>
</table>


<script language="javascript" type="text/javascript">
	function localDownload(sFormat, sNomenclatureCode) {
		switch(sFormat) {
			case "HTML":
				if (document.FrmAct.RdoChoice[0].checked)
					window.location.href='index.cfm?TargetUrl=ACT_OTH_CLS_DLD&StrNom=' + sNomenclatureCode + '&StrFormat=HTML&StrLanguageCode=EN&IntLevel=' + document.FrmAct.CboLevel.value;
				else
					window.location.href='index.cfm?TargetUrl=ACT_OTH_CLS_DLD&StrNom=' + sNomenclatureCode + '&StrFormat=HTML&StrLanguageCode=EN';
				break;
			case "CSV":
				if (document.FrmAct.RdoChoice[0].checked)
					OpenWindow('index.cfm?TargetUrl=ACT_DELIMITER&StrNom=' + sNomenclatureCode + '&StrFormat=CSV&StrLanguageCode=EN&IntLevel=' + document.FrmAct.CboLevel.value,'ACT_DELIMITER', 500, 200);
				else
					OpenWindow('index.cfm?TargetUrl=ACT_DELIMITER&StrNom=' + sNomenclatureCode + '&StrFormat=CSV&StrLanguageCode=EN','ACT_DELIMITER', 500, 200);
				break;
			case "XML":
				if (document.FrmAct.RdoChoice[0].checked)
					window.location.href='index.cfm?TargetUrl=ACT_OTH_CLS_DLD&StrNom=' + sNomenclatureCode + '&StrFormat=XML&StrLanguageCode=EN&IntLevel=' + document.FrmAct.CboLevel.value;
				else
					window.location.href='index.cfm?TargetUrl=ACT_OTH_CLS_DLD&StrNom=' + sNomenclatureCode + '&StrFormat=XML&StrLanguageCode=EN';
				break;
		}
	}
</script>


<form action="" name="FrmAct" method="post">
<table border="0" cellpadding="2" cellspacing="2" width=95% align="center">
	<tr>
		<td class="title3" align="center" colspan="2">
			METADATA DOWNLOAD<br>
		</td>
	</tr>
	<tr>
		<td align="center" class="text1-b" colspan="2">
			NUTS (Nomenclature of Territorial Units for Statistics), by regional level, version 2013 (NUTS 2013)
			<br><br>
		</td>
	</tr>
	<tr>
		<td class="text" colspan="2">
			The downloadable files listed here contain the structure and, if any, the explanatory notes of the classifications. Other downloadable files linked to the classification (for instance, methodological documents, electronic copies of paper publications, etc.) are also listed here. The lists depends on the selected language.
			<br><br>
		</td>
	</tr>
	<tr>
		<td>
			
					<script type="text/javascript" language="JavaScript">
					document.write('<input type="Button" name="BtnTop" value="Back to classification" class="actbutton" onclick="window.location.href=\'index.cfm?TargetUrl=ACT_OTH_RESET&StrNom=NUTS_2013L&StrLanguageCode=EN&StrLayoutCode=HIERARCHIC\';">');
					</script>
					<noscript>
					<a href="index.cfm?TargetUrl=ACT_OTH_RESET&StrNom=NUTS_2013L&StrLanguageCode=EN&StrLayoutCode=HIERARCHIC" class="text">LNK_TOP_OF_CLASSIFICATION</a>
					</noscript>
				
		</td>
		<td align="right" class="text" valign="top">
			Select the language of the data 
				<script type="text/javascript" language="JavaScript">
					document.write('<select name="CboLanguage" class="entryfield" onchange="window.location.href=\'index.cfm?TargetUrl=LST_CLS_DLD&StrNom=NUTS_2013L&StrLanguageCode=\' + CboLanguage.value + \'&StrLayoutCode=HIERARCHIC\';">');
					
						document.write('<option value="DE" >Deutsch<\/option>');
					
						document.write('<option value="EN" selected>English<\/option>');
					
						document.write('<option value="FR" >Français<\/option>');
					
					document.write('<\/select>');
				</script>

				<noscript id="langsel_noscript">
				
						<a href="index.cfm?TargetUrl=LST_CLS_DLD&StrLanguageCode=DE&StrNom=NUTS_2013L&StrLayoutCode=HIERARCHIC" id="lsns_DE" class="datalanguage" lang="DE" title="Deutsch">DE</a>
					
						<span class="datalanguagenolink" lang="EN" title="English">EN</span>
					
						<a href="index.cfm?TargetUrl=LST_CLS_DLD&StrLanguageCode=FR&StrNom=NUTS_2013L&StrLayoutCode=HIERARCHIC" id="lsns_FR" class="datalanguage" lang="FR" title="Français">FR</a>
					
				</noscript>
			
		</td>
	</tr>
	<tr>
		<td colspan="2">
			<hr color="#1E42BF" width="100%">
		</td>
	</tr>

	
	
			<tr>
				<td class="text" colspan="2">
					<input type="radio" name="RdoChoice" value="level" onClick="document.FrmAct.CboLevel.disabled = false;">&nbsp;Download hierarchical level number
					&nbsp;
					<select name="CboLevel" class="entryField" disabled>
						
							<option value="1">1</option>
						
							<option value="2">2</option>
						
							<option value="3">3</option>
						
					</select>
				</td>
			</tr>
			<tr>
				<td class="text" colspan="2">
					<input type="radio" name="RdoChoice" value="all" onClick="document.FrmAct.CboLevel.disabled = true;" checked>&nbsp;Download all the classification
				</td>
			</tr>
		
		<tr>
			<td colspan="2">
				<table border="0" cellpadding="2" cellspacing="2" width="100%">
					<tr>
						<td class="text" colspan="3">
							<strong>Database generated download</strong>
						</td>
					</tr>
					<tr>
						<td class="toolsrule" colspan="3" align="center">Format</td>
					</tr>
					<tr>
						<td class="toolsrule" align="center">HTML</td>
						<td class="toolsrule" align="center">CSV</td>
						<td class="toolsrule" align="center">XML</td>
					</tr>
					
					<tr class="tablerowlist">
						<td class="text" align="center">
							
								<a href="#" onclick="localDownload('HTML','NUTS_2013L')">download</a>
							
						</td>
						<td class="text" align="center">
							
								<a href="#" onclick="localDownload('CSV','NUTS_2013L')">download</a>
							
						</td>
						<td class="text" align="center">
							
								<a href="#" onclick="localDownload('XML','NUTS_2013L')">download</a>
							
						</td>
					</tr>
					

					<tr><td class="text" colspan="3"><br><em>
The download files in HTML and CSV formats offer various options for recreating the structure of the classifications in your own database systems.
<br>Column "Order" is the most straightforward as it presents the records in their sequential order; so, sorting the records by this attribute guarantees that the records will be displayed in the right sequential order.
<br>If you need more information for creating the structure of the classification, various other fields are made available: The field "LEVEL" indicates the hierarchical level of a record in the hierarchy of the classification (the highest level being indicated with "1"); as the name indicates, the field "PARENT CODE" indicates the record just above in the hierarchy of codes. In some cases, you will find in the download files two columns called "Code" and "Parent"; this is due to the fact that the RAMON database does not accept empty codes (as is the case e.g. for Combined Nomenclature) or alphanumeric codes (as is the case e.g. for the NACE or ISIC classifications). In this case, we use a "dummy" code and a "dummy" parent which meet the requirements of the database. In such cases, users should use the first "Code" and "Parent" fields which are displayed in the download files (in general columns 3 and 4).
<br>Please note that the XML format does not presently propose such a detailed structure as this format will be revised in the near future.
<br><br><strong>XML</strong>: After clicking on the XML button it is recommended to use the "Save" functionality instead of the "Open" functionality.
Depending on the browser used this function might not work properly and user will be faced with an error message.
<br><br><strong>CSV</strong> format is scheduled for ASCII. For diacritic character it is recommended to use the "Save" functionality (instead of the "Open" functionality).
<br>You may then use softwares like NotePad++ / NotePad2 / NotePad / etc. to display properly the information (opening directly the file in MS-Excel will cause uncorrect display of some characters for instance accented characters).
<br>For importing the files into MS-Excel, it is recommended to use the "Import External Data" function (from menu option "Data").
					</em></td></tr>
				</table>
			</td>
		</tr>
		<tr><td colspan="2"><br>&nbsp;</td></tr>
	
	<tr>
		<td colspan="2">
			<table border="0" cellpadding="2" cellspacing="2" width="100%">
				<tr>
					<td class="text" colspan="7">
						<strong>Other documents relating to this classification</strong>
					</td>
				</tr>
				<tr>
					<td class="toolsrule" align="center">&nbsp;</td>
					<td class="toolsrule" align="center">HTML</td>
					<td class="toolsrule" align="center">DOC</td>
					<td class="toolsrule" align="center">MDB</td>
					<td class="toolsrule" align="center">PDF</td>
					<td class="toolsrule" align="center">TXT</td>
					<td class="toolsrule" align="center">XLS</td>
				</tr>
				
				<tr>
					<td class="text">
						NUTS Classification in MS-Excel format 
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
					<td class="text" align="center">
						
									<a href="http://ec.europa.eu/eurostat/ramon/documents/nuts/NUTS_2013.zip" target="_blank">
								
							<img src="http://ec.europa.eu/eurostat/ramon/images/xls.gif" alt="download" border="0" height="15" width="15">
							</a>
						
					</td>
				</tr>
				
				<tr>
					<td class="text">
						More information about NUTS 
					</td>
					<td class="text" align="center">
						
									<a href="http://ec.europa.eu/eurostat/web/nuts/overview" target="_blank">
								
							<img src="http://ec.europa.eu/eurostat/ramon/images/html.GIF" alt="download" border="0">
							</a>
						
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
				</tr>
				
				<tr>
					<td class="text">
						Translation into English (when such exist) of region names at levels 1 and 2 of the NUTS classification 
					</td>
					<td class="text" align="center">
						
									<a href="http://publications.europa.eu/code/en/en-5001000.htm" target="_blank">
								
							<img src="http://ec.europa.eu/eurostat/ramon/images/html.GIF" alt="download" border="0">
							</a>
						
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
					<td class="text" align="center">
						
						&nbsp;
						
					</td>
				</tr>
				
			</table>
		</td>
	</tr>
	
	

	<tr>
		<td colspan="2">
			<hr color="#1E42BF" width="100%">
		</td>
	</tr>
</table>
</form>
<!-- END OF CONTENT -->


<!-- BOTTOM NAVIGATION BAR -->
<table align="center" border="0" cellspacing="0" cellpadding="0" summary="Bottom navigation" id="bottom_nav">
	<tr>
		<td align="center" class="bottom-separator">
			<a href="#top" class="bottom-navigation">Top</a>
		</td>
	</tr>
</table>
</div >
</body>
</html> 