"""A scraper for Malta 2007-2013."""

import requests
import lxml
import csv

from lxml import html

__author__ = 'Fernando Blat'


# Base URL is the host of the page
BASE_URL = 'https://investinginyourfuture.gov.mt'
# Projects are fetch from the paginated list
PAGINATION_URL = 'https://investinginyourfuture.gov.mt/ajax/loadProjects.ashx?page='


def get_text(html_node):
    if html_node is not None:
        return html_node.text


def scrape_project(url):
    project_data = []
    url = BASE_URL + url

    response = requests.get(url)
    doc = lxml.html.fromstring(response.content)
    # Code
    project_data.append(get_text(doc.find('.//span[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectRefCode"]')))
    # Title
    project_data.append(get_text(doc.find('.//span[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectTitle"]')))
    # Project Cost
    project_data.append(get_text(doc.find('.//span[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectCostBeneficiaryItem_divCostValue"]')))
    # Beneficiary
    project_data.append(get_text(doc.find('.//span[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectCostBeneficiaryItem_divBeneficiaryValue"]')))
    # Line Ministry
    project_data.append(get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdLineMinistry"]')))
    # Start Date
    project_data.append(get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdStartDate"]')))
    # End Date
    project_data.append(get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdEndDate"]')))
    # Non Technical Short Summary Of Project
    project_data.append(get_text(doc.find('.//div[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_divNonTechnicalShortSummaryContent"]/p')))
    # Operational Programme
    project_data.append(get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdOperationalProgramme"]')))
    # Fund
    project_data.append(get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdFund"]')))
    # Operational Objective
    project_data.append(get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdOperationalObjective"]')))
    # Priority Axis
    project_data.append(get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdPriorityAxis"]')))
    # Focus Area Of Intervention
    project_data.append(get_text(doc.find('.//td[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_tdFocusAreaOfIntervention1"]')))
    # Project Objectives
    project_data.append(get_text(doc.find('.//div[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_divProjectObjectives"]/p')))
    # Project Results
    project_data.append(get_text(doc.find('.//div[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_divProjectResults"]/p')))
    # Project Purpose
    project_data.append(get_text(doc.find('.//div[@id="mainPlaceHolder_coreContentPlaceHolder_mainContentPlaceHolder_projectDetails_divProjectPurpose"]/p')))

    return project_data


def scrape():
    headers = [
        'Code', 'Title', 'Project Cost', 'Beneficiary', 'Line Ministry', 'Start Date', 'End Date', 'Non Technical Short Summary Of Project', 'Operational Programme', 'Fund', 'Operational Objective',
        'Priority Axis', 'Focus Area Of Intervention', 'Project Objectives', 'Project Results', 'Project Purpose'
    ]

    with open('data.csv', 'w', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(headers)

        page = 1
        while True:
            res = requests.get(PAGINATION_URL + str(page))
            if 'Content-Length' in res.headers and res.headers['Content-Length'] == '0':
                print("Exiting...")
                break

            doc = html.fromstring(res.content)

            for link in doc.findall('.//div[@class="project-listing-item-title"]/a'):
                project_data_row = scrape_project(link.get('href'))
                writer.writerow(project_data_row)
                # row_dict = dict(zip(headers, project_data_row))
                # info('Scraped row = %s', row_dict)
                # yield row_dict

            page += 1
            print("\n")


def process_resources():
    yield scrape()


if __name__ == '__main__':
    scrape()
