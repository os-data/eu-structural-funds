import requests
from lxml import html
from pprint import pprint

# Base URL is the host of the page
BASE_URL = 'https://investinginyourfuture.gov.mt'
# Projects are fetch from the paginated list
PAGINATION_URL = 'https://investinginyourfuture.gov.mt/ajax/loadProjects.ashx?page='

def get_text(html_node):
    if html_node is not None:
        return html_node.text

def scrape_project(url):
    data = {}
    url = BASE_URL + url

    res = requests.get(url)
    doc = html.fromstring(res.content)
    data['Code'] = get_text(doc.find('.//span[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectRefCode"]'))
    data['Title'] = get_text(doc.find('.//span[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectTitle"]'))
    data['Project Cost'] = get_text(doc.find('.//span[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectCostBeneficiaryItem_divCostValue"]'))
    data['Beneficiary'] = get_text(doc.find('.//span[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectCostBeneficiaryItem_divBeneficiaryValue"]'))
    data['Line Ministry'] = get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdLineMinistry"]'))
    data['Start Date'] = get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdStartDate"]'))
    data['End Date'] = get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdEndDate"]'))
    data['Non Technical Short Summary Of Project'] = get_text(doc.find('.//div[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_divNonTechnicalShortSummaryContent"]/p'))
    data['Operational Programme'] = get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdOperationalProgramme"]'))
    data['Fund'] = get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdFund"]'))
    data['Operational Objective'] = get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdOperationalObjective"]'))
    data['Priority Axis'] = get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdPriorityAxis"]'))
    data['Focus Area Of Intervention'] = get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdFocusAreaOfIntervention1"]'))
    data['Project Objectives'] = get_text(doc.find('.//div[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_divProjectObjectives"]/p'))
    data['Project Results'] = get_text(doc.find('.//div[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_divProjectResults"]/p'))
    data['Project Purpose'] = get_text(doc.find('.//div[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_divProjectPurpose"]/p'))

    pprint(data)


def scrape():
    page = 1
    while True:
        res = requests.get(PAGINATION_URL + str(page))
        if 'Content-Length' in res.headers and res.headers['Content-Length'] == '0':
            print("Exiting...")
            break

        doc = html.fromstring(res.content)

        for link in doc.findall('.//div[@class="project-listing-item-title"]/a'):
            scrape_project(link.get('href'))

        page = page + 1
        print("\n")


if __name__ == '__main__':
    scrape()
