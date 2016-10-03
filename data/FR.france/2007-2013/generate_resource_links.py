"""Generate the regional resource links for source.description.yaml.

Usage:
    python3 generate_resource_links.py

"""


funds = ['FED', 'FSE']
regions = [971, 972, 973, 974, 72, 73, 91, 93, 82, 83,
           74, 54, 52, 24, 26, 43, 42, 41, 21, 11, 22, 31, 23, 25, 53]

title = 'EXCEL file for France 2007-2013 {fund} region code {region}'
url = ('http://cartobenef.asp-public.fr/cartobenef/'
       'liste_benef_excel.php?nivgeo=reg&codgeo={region}typeFonds={fund}')

for region in regions:
    for fund in funds:
        print('    - title: ' + title.format(fund=fund, region=region))
        print('      url:  ' + url.format(fund=fund, region=region))
        print()
