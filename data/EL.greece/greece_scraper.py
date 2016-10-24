import requests
# This is the default URL for the first level of the data
# Each page contains 50 results
url = "http://anaptyxi.gov.gr/GetData.ashx?queryType=projects&outputFormat=json&pagenum="


# The "projects_list" contains each page with their 50 results or main projects
# The API is extremely slow, so I could not check if it is gathering all the results
page = 0
projects_list = []
while True:
    response = requests.get(url + str(page))
    data = response.json()
    projects_list.append(data)
    page = page + 1
    if page is None:
        print("exit")
        break

# Extracting the code for each main project
position = 0
code_list = []
for item in projects_list:
    individual_project = item[position]
    code = individual_project["kodikos"]
    code_list.append(code)
    position = position + 1

# The "subprojects_list" includes each subproject for each project in the main projects' list
subprojects_list = []
for item in code_list:
    url = "http://anaptyxi.gov.gr/GetData.ashx?queryType=projectDetails&queryArgument=" + str(item) + "&projectDetails=subprojects"
    response = requests.get(url)
    data = response.json()
    subprojects_list.append(data)
