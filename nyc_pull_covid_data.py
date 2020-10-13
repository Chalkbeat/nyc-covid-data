import requests
import pandas as pd

SCHOOLS_BASE = "https://raw.githubusercontent.com/Chalkbeat/nyc-covid-data/main/schools_base.csv"
df = pd.read_csv(SCHOOLS_BASE)
df = df.loc[:,["BEDSCode","InstitutionName","RecTypeLong","StreetAddress","City","State","Zip+4"]]
df.columns = ["BEDSCode","InstitutionName","RecTypeLong","StreetAddress","City","State","Zip"]

def prepend(list, str):
    str += '{0}'
    list = [str.format(i) for i in list]
    return(list)

url = ""
bedscode = ""
rectype = ""
lookup_df = pd.DataFrame()

for row in df.itertuples():
  bedscode = row.BEDSCode
  rectype = row.RecTypeLong
  name = row.InstitutionName
  address = row.StreetAddress
  city = row.City
  state = row.State
  zip = row.Zip

  if rectype == "Public":
    url = "https://schoolcovidreportcard.health.ny.gov/data/public/school.300000."+bedscode+".json"
  if rectype == "Charter":
    url = "https://schoolcovidreportcard.health.ny.gov/data/charter/school.charter."+bedscode+".json"
  if rectype == "Private" or rectype == "Non-Public":
    url = "https://schoolcovidreportcard.health.ny.gov/data/private/school.private."+bedscode+".json"

  r = requests.get(url)

  if r:
    json = r.json()
    status = "reported"
    schoolType = json['schoolType']
    teachingModel = json['teachingModel']
    updateDate = json['updateDate']
    publishDate = json['publishDate']
    currentCounts = pd.DataFrame(json['currentCounts'], index=[0])
    currentCounts.columns = prepend(currentCounts.columns, 'currentCounts_')
    todayCounts = pd.DataFrame(json['todayCounts'], index=[0])
    todayCounts.columns = prepend(todayCounts.columns, 'todayCounts_')
    allTimeCounts = pd.DataFrame(json['allTimeCounts'], index=[0])
    allTimeCounts.columns = prepend(allTimeCounts.columns, 'allTimeCounts_')
    pastWeekCounts = pd.DataFrame(json['pastWeekCounts'], index=[0])
    pastWeekCounts.columns = prepend(pastWeekCounts.columns, 'pastWeekCounts_')

    new_row = pd.DataFrame([[bedscode, url, status, rectype, name, address, city, state, zip, schoolType, teachingModel, updateDate, publishDate]])
    new_row.columns=["BEDSCode", "url", "status", "recType", "name", "address", "city", "state", "zip", "schoolType", "teachingModel", "updateDate", "publishDate"]
    new_row = pd.concat([new_row, currentCounts, todayCounts, allTimeCounts, pastWeekCounts], axis=1)
    lookup_df = lookup_df.append([new_row], ignore_index=True)

  else:
    status = "not reported"
    new_row = pd.DataFrame([[bedscode, url, status, rectype, name, address, city, state, zip]])
    new_row.columns=["BEDSCode", "url", "status", "recType", "name", "address", "city", "state", "zip"]
    lookup_df = lookup_df.append([new_row], ignore_index=True)

lookup_df.to_csv("nyc_schools_covid_data.csv", index=False)
