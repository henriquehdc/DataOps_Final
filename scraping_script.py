# pip install pandas
# pip install requests
# pip install beautifulsoup4
# pip install sqlalchemy
# pip install mysql-connector-python

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Float, String
import numpy as np

engine = create_engine('mysql+mysqlconnector://root:root@34.31.69.51:3306/trabdataops')

def scrape_this(uri="/pages/forms/"):
  page = requests.get("https://scrapethissite.com" + uri)
  soup = BeautifulSoup(page.text, "html.parser")

  div = soup.find(id="hockey")  
  table = div.find("table")

  data_rows = table.find_all("tr", attrs={"class": "team"})
  parsed_data = list()
  stat_keys = [col.attrs["class"][0] for col in data_rows[0].find_all("td")]

  for row in data_rows:
    tmp_data = dict()
    for attr in stat_keys:
      attr_val = row.find(attrs={"class": attr}).text
      tmp_data[attr] = re.sub(r"^\s+|\s+$", "", attr_val)
    parsed_data.append(tmp_data)

  data_df = pd.DataFrame(parsed_data)
  return data_df

page = requests.get("https://scrapethissite.com/pages/forms/")
soup = BeautifulSoup(page.text, "html.parser")
pagination = soup.find("ul", attrs={"class": "pagination"})
link_elms = pagination.find_all("li")
links = [link_elm.find("a").attrs["href"] for link_elm in link_elms]
links = set(links)

temp_dfs = list()
for link in links:
  tmp_df = scrape_this(uri=link)
  temp_dfs.append(tmp_df)
hockey_team_df = pd.concat(temp_dfs, axis=0).reset_index()
hockey_team_df.sort_values(["year", "name"], inplace=True)

hockey_team_df.rename({'index': 'indice', 'ot-losses': 'ot_losses', 'name': 'nome'}, axis=1, inplace=True)
hockey_team_df.drop('indice', inplace=True, axis=1)
hockey_team_df = hockey_team_df.replace(r'^\s*$', np.nan, regex=True)
print(hockey_team_df)
print("dados coletados")

hockey_team_df.to_sql(
    "hockey_team",
    con=engine,  
    if_exists='replace', 
    index=False, 
    dtype={'nome': String(60), 'year': Integer(), 'wins': Integer(), 'losses': Integer(), 'ot_losses': Integer(), 'pct': Float(), 'gf': Integer(), 'ga': Integer(), 'diff': Integer()}, 
)
