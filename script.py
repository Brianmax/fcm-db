from client.client import EncompassClient
from transform.transform import insert_companies_to_db
from utils.utils import map_external_companies_encompass_to_company

import json

api_url = "https://api.elliemae.com"
client_id = "prdppfc"
client_secret = "kj*hqmp&EV5j04quDsP9*P8UMA^HjM8meV&sf@xtqE4E476fX9qFSsJMc#kqEf5g"
username = "ravn"
password = "65@FqF!5BMFwrHCQ"
instance = "TEBE11379286"
# client = EncompassClient(api_url, client_id, client_secret, username, password, instance)
# access_token = client.get_access_token()
# DATABASE_URL = "postgresql://postgres:pBSa1milH8fZAh7hMius4SbPhpL5FrM8@tpo-fcm-sst-staging-dbinstance-cskskeoz.ccacljf7dlc2.us-east-1.rds.amazonaws.com:5432/tpofcmstg"
DATABASE_URL = "postgresql://postgres:password@localhost:5432/first_colony"

# print("Access Token:", access_token)

# external_companies = client.fetch_external_org()



ids = []


#internal_companies = client.fetch_internal_org()


# externalCompaniesComplete = get_all_complete_companies(client, ids)

with open("external_companies.json", "r", encoding="utf-8") as f:
    externalCompaniesComplete = json.load(f)

externalCompanies = []
for external_company in externalCompaniesComplete:
    externalCompanies.append(external_company.get("basicInfo"))

for external_company in externalCompanies:
    ids.append(external_company.get("id"))

# df_external = transform_external_companies_encompass_to_dataframe_improved(external_companies)
externalCompaniesMapped = map_external_companies_encompass_to_company(externalCompaniesComplete)
insert_companies_to_db(externalCompaniesMapped, DATABASE_URL, externalCompanies, externalCompaniesComplete)
