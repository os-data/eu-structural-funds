import csv

import requests
from lxml import etree
from lxml import html

# pdb.set_trace()

# Base URL is the main page
# BASE_URL = 'http://www.fonds-europeens.public.lu/fr/projets-cofinances/index.php?~=do&q=%23all&r=&s=date&from=search&res_length=10&b='
BASE_URL = 'http://www.fonds-europeens.public.lu/fr/projets-cofinances/index.php'


def get_text(html_node):
    if html_node is not None:
        return html_node.text.strip()


def scrape_project(url):
    print(url)

    project_data = []

    res = requests.get(url)
    doc = html.fromstring(res.content)

    # Title
    project_data.append(get_text(doc.find('.//article/header/h1')))
    # Description
    e = doc.find('.//div[@id="content"]')
    project_data.append(etree.tostring(e))
    # Fonds
    project_data.append(get_text(doc.findall('.//div[@class="box-content"]//dd')[0]))
    # Objectifs thématiques
    project_data.append(get_text(doc.findall('.//div[@class="box-content"]//dd')[1]))
    # Programme
    project_data.append(get_text(doc.findall('.//div[@class="box-content"]//dd')[2]))
    # Porteur de project
    div_boxes = doc.findall('.//div[@class="box box--project-details"]/div[@class="box"]')
    box = div_boxes[0]
    names = []
    for link in box.findall('.//a'):
        names.append(link.text)
    project_data.append(','.join(names))
    # Porteur de project URL
    urls = []
    for link in box.findall('.//a'):
        urls.append(link.get('href'))
    project_data.append(','.join(urls))
    # Commune
    box = div_boxes[1]
    names = []
    for item in box.findall('.//li'):
        names.append(item.text)
    project_data.append(','.join(names))
    # Coût total
    box = div_boxes[2]
    cost = box.findall('.//li')[0]
    project_data.append(cost.text)
    # Coût de fonds
    cost = box.findall('.//li')[1]
    project_data.append(cost.text)
    # Durée
    box = div_boxes[3]
    duration = box.findall('.//li')[0]
    project_data.append(duration.text)
    # Catégorie d'intervention
    if len(div_boxes) > 4:
        box = div_boxes[4]
        category = box.findall('.//li')[0]
        project_data.append(category.text)
    else:
        project_data.append("")

    return project_data


def scrape():
    headers = [
        'Title', 'Description', 'Fonds', 'Objectifs thématiques',
        'Programme', 'Porteur de project', 'Porteur de project URL', 'Commune',
        'Coût total', 'Coût de fonds', 'Durée', "Catégorie d'intervention"
    ]

    with open('data.csv', 'w', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(headers)

        while True:
            res = requests.get(BASE_URL)
            doc = html.fromstring(res.content)

            next_page = doc.find(".//a[@class='icon-right']")
            if next_page is None:
                print("Exiting...")
                break

            for link in doc.findall('.//article/header/h2/a'):
                project_data_row = scrape_project(link.get('href'))
                writer.writerow(project_data_row)


if __name__ == '__main__':
    scrape()
