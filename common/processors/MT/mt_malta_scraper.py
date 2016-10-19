"""A scraper for Malta 2007-2013."""

from datapackage_pipelines.wrapper import spew, ingest
from logging import info, debug
from lxml.html import fromstring
from requests import Session

BASE_URL = 'https://investinginyourfuture.gov.mt'
PAGINATION_URL = BASE_URL + '/ajax/loadProjects.ashx?page={counter}'
PROJECT_URLS_XPATH = './/div[@class="project-listing-item-title"]/a'
FIELD_XPATHS = {
    'Code': './/span[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectRefCode"]',
    'Title': './/span[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectTitle"]',
    'Project Cost': ".//*[@id='mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectCostBeneficiaryItem_divCostValue']",
    'Beneficiary': './/span[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectCostBeneficiaryItem_divBeneficiaryValue"]',
    'Line Ministry': './/td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdLineMinistry"]',
    'Start Date': './/td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdStartDate"]',
    'End Date': './/td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdEndDate"]',
    'Non Technical Short Summary Of Project': ".//*[@id='mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_divNonTechnicalShortSummaryContent']/p",
    'Operational Programme': './/td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdOperationalProgramme"]',
    'Fund': './/td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdFund"]',
    'Operational Objective': './/td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdOperationalObjective"]/p',
    'Priority Axis': './/td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdPriorityAxis"]',
    'Focus Area Of Intervention': './/td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdFocusAreaOfIntervention1"]',
    'Project Objectives': './/div[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_divProjectObjectives"]/p',
    'Project Results': './/div[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_divProjectResults"]/p',
    'Project Purpose': './/div[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_divProjectPurpose"]/p',
}

session = Session()


def scrape_project(url):
    """Return project data as a generator of tuples."""

    response = session.get(url)
    doc = fromstring(response.content)

    def get_text(html_node):
        if html_node is not None:
            return html_node.text

    for key, xpath in FIELD_XPATHS.items():
        node = doc.find(xpath)
        value = get_text(node)
        debug('Extracted %s = %s', key, value)
        yield key, value


def scrape_projects(paths):
    """Return generator of project dictionaries."""

    for path in paths:
        url = BASE_URL + path
        project_row = dict(scrape_project(url))
        info('Scraped %s', project_row)
        yield project_row


def get_project_urls():
    """Return the complete list of project URLS."""

    counter = 0
    paths = []

    while True:
        counter += 1

        project = PAGINATION_URL.format(counter=counter)
        response = session.get(project)

        if response.text:
            doc = fromstring(response.content)
            more_links = doc.findall(PROJECT_URLS_XPATH)
            more_paths = list(map(lambda x: x.get('href'), more_links))
            paths.extend(more_paths)
            info('Collected %s urls on page %s', len(more_paths), counter)

        else:
            return paths


if __name__ == '__main__':
    _, datapackage, _ = ingest()
    project_paths = get_project_urls()
    project_rows = scrape_projects(project_paths)
    spew(datapackage, [project_rows])
