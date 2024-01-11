import html5lib as html5lib
import scraper as scraper
import pip
import pandas as pd
import datetime
import requests
from bs4 import BeautifulSoup
import numpy as np
import player_game_log as p
import time

#### Code below to pull multiple years of data ####

### Inputs
start_year = 2012
year = start_year
current_year = 2023

defColumnSettings = {
    'axis': 1,
    'inplace': True
}

### Pull Passing Season Data - Web Scrape

passing_season_stats = []

while year < current_year:
    url = f"https://www.pro-football-reference.com/years/{year}/passing.htm"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find(name='table', id="passing")

    df = pd.read_html(str(table))[0]
    df["Season"] = year
    passing_season_stats.append(df)
    year += 1

passing_season_stats = pd.concat(passing_season_stats)
passing_season_stats.to_csv("passing_season.csv")

### Pull Rushing Season Data - Web Scrape

rushing_season_stats = []
year = start_year

while year < current_year:
    url = f"https://www.pro-football-reference.com/years/{year}/rushing.htm"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find(name='table', id="rushing")

    df = pd.read_html(str(table))[0]
    df["Season"] = year
    rushing_season_stats.append(df)
    year += 1

rushing_season_stats = pd.concat(rushing_season_stats)
rushing_season_stats.to_csv("rushing_season.csv")


### Pull Receiving Season Data - Web Scrape

receiving_season_stats = []
year = start_year

while year < current_year:
    url = f"https://www.pro-football-reference.com/years/{year}/receiving.htm"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find(name='table', id="receiving")

    df = pd.read_html(str(table))[0]
    df["Season"] = year
    receiving_season_stats.append(df)
    year += 1

receiving_season_stats = pd.concat(receiving_season_stats)
receiving_season_stats.to_csv("receiving_season.csv")

### Pull Defense Season Data - Web Scrape

defense_season_stats = []
year = start_year

while year < current_year:
    url = f"https://www.pro-football-reference.com/years/{year}/opp.htm"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find(name='table', id="team_stats")

    df = pd.read_html(str(table))[0]
    df["Season"] = year
    defense_season_stats.append(df)
    year += 1

defense_season_stats = pd.concat(defense_season_stats)
defense_season_stats.to_csv("defense_season.csv")

### Use for data manipulation after data scraped from web

passing_season = pd.read_csv("passing_season.csv")
receiving_season = pd.read_csv("receiving_season.csv")
rushing_season = pd.read_csv("rushing_season.csv")

## Passing data cleaning

passing_season.reset_index(inplace=True)
passing_season['Player'] = passing_season['Player'].str.rstrip('+')
passing_season['Player'] = passing_season['Player'].str.rstrip('*')

passing_season = pd.DataFrame(passing_season)
passing_season["Att"] = pd.to_numeric(passing_season["Att"], errors="coerce")
passing_season["Att"].replace('', np.nan, inplace=True)
passing_season.dropna(subset=["Att"], inplace=True)

passing_season = passing_season[passing_season["Att"] > 75]
passing_season = passing_season.drop(["index", "Rk", "GS", "1D", "Lng", "4QC", "GWD"], axis=1)
passing_season = passing_season.rename(columns={"Yds.1": "Skyds"})

passing_season["Tm"].replace('KAN', 'KC', inplace=True)
passing_season["Tm"].replace('NWE', 'NE', inplace=True)
passing_season["Tm"].replace('GNB', 'GB', inplace=True)
passing_season["Tm"].replace('NOR', 'NO', inplace=True)
passing_season["Tm"].replace('TAM', 'TB', inplace=True)
passing_season["Tm"].replace('SFO', 'SF', inplace=True)
passing_season["Tm"].replace('LVR', 'LV', inplace=True)
passing_season["Tm"].replace('OAK', 'LV', inplace=True)
passing_season["Tm"].replace('STL', 'LAR', inplace=True)
passing_season["Tm"].replace('SDG', 'LAC', inplace=True)

## Rushing data cleaning

rushing_season.reset_index(inplace=True)
rushing_season['Player'] = rushing_season['Player'].str.rstrip('+')
rushing_season['Player'] = rushing_season['Player'].str.rstrip('*')

rushing_season = pd.DataFrame(rushing_season)
rushing_season["Att"] = pd.to_numeric(rushing_season["Att"], errors="coerce")
rushing_season["Att"].replace('', np.nan, inplace=True)
rushing_season.dropna(subset=["Att"], inplace=True)

rushing_season = rushing_season.loc[rushing_season["Pos"].isin(["QB", "RB", "WR", "TE"])]
rushing_season = rushing_season.drop(["index", "Rk", "GS", "1D", "Lng"], axis=1)

rushing_season["Tm"].replace('KAN', 'KC', inplace=True)
rushing_season["Tm"].replace('NWE', 'NE', inplace=True)
rushing_season["Tm"].replace('GNB', 'GB', inplace=True)
rushing_season["Tm"].replace('NOR', 'NO', inplace=True)
rushing_season["Tm"].replace('TAM', 'TB', inplace=True)
rushing_season["Tm"].replace('SFO', 'SF', inplace=True)
rushing_season["Tm"].replace('LVR', 'LV', inplace=True)
rushing_season["Tm"].replace('OAK', 'LV', inplace=True)
rushing_season["Tm"].replace('STL', 'LAR', inplace=True)
rushing_season["Tm"].replace('SDG', 'LAC', inplace=True)

rushing_season.to_csv("rushing_2012_2022.csv")


## Receiving data cleaning

receiving_season.reset_index(inplace=True)
receiving_season['Player'] = receiving_season['Player'].str.rstrip('+')
receiving_season['Player'] = receiving_season['Player'].str.rstrip('*')

receiving_season = pd.DataFrame(receiving_season)
receiving_season["Rec"] = pd.to_numeric(receiving_season["Rec"], errors="coerce")
receiving_season["Rec"].replace('', np.nan, inplace=True)
receiving_season.dropna(subset=["Rec"], inplace=True)

receiving_season = receiving_season.loc[receiving_season["Pos"].isin(["RB", "WR", "TE"])]
receiving_season = receiving_season[receiving_season["Rec"] > 10]
receiving_season = receiving_season.drop(["index", "Rk", "GS", "1D", "Lng"], axis=1)

receiving_season["Tm"].replace('KAN', 'KC', inplace=True)
receiving_season["Tm"].replace('NWE', 'NE', inplace=True)
receiving_season["Tm"].replace('GNB', 'GB', inplace=True)
receiving_season["Tm"].replace('NOR', 'NO', inplace=True)
receiving_season["Tm"].replace('TAM', 'TB', inplace=True)
receiving_season["Tm"].replace('SFO', 'SF', inplace=True)
receiving_season["Tm"].replace('LVR', 'LV', inplace=True)
receiving_season["Tm"].replace('OAK', 'LV', inplace=True)
receiving_season["Tm"].replace('STL', 'LAR', inplace=True)
receiving_season["Tm"].replace('SDG', 'LAC', inplace=True)

receiving_season.to_csv("receiving_2012_2022.csv")

## Individual Players, Position, and Season

passing_season_stats = pd.read_csv("passing_2012_2022.csv")
pass_ = passing_season_stats[['Player', 'Pos', 'Season']]
pass_["Season"] = pd.to_numeric(pass_["Season"])
pass_ = pass_[pass_["Pos"] == "QB"]
passers = pass_.values.tolist()

rushing_season_stats = pd.read_csv("rushing_2012_2022.csv")
rush_ = rushing_season_stats[['Player', 'Pos', 'Season']]
rush_["Season"] = pd.to_numeric(rush_["Season"])
rush_ = rush_[rush_["Pos"] == "RB"]
rushers = rush_.values.tolist()

receiving_season_stats = pd.read_csv("receiving_2012_2022.csv")
receive_ = receiving_season_stats[['Player', 'Pos', 'Season']]
receive_["Season"] = pd.to_numeric(receive_["Season"])
receive_ = receive_.loc[receive_["Pos"].isin(["WR", "TE"])]
receivers = receive_.values.tolist()

# Passing Game Data for all years

passing_game_data = []

for i in range(0, len(passers)):
    try:
        game_log = p.get_player_game_log(player=passers[i][0], position=passers[i][1], season=passers[i][2])
        game_log["Player"] = passers[i][0]
        game_log["Pos"] = passers[i][1]
        game_log["Season"] = passers[i][2]
        game_log.rename(columns={'game_location': 'home'}, inplace=True)
        game_log['home'] = game_log['home'].replace(['@', ''], [0, 1])
        passing_game_data.append(game_log)
    except:
        pass

    complete = round((i + 1) / len(passers) * 100, 2)
    print(f"Passers {complete}% complete")
    time.sleep(6)

passing_game_data = pd.concat(passing_game_data)

passing_game_data.to_csv('./passing_game_data.csv')

# Rushing Game Data for all years

rushing_game_data = []

for i in range(0, len(rushers)):
    try:
        game_log = p.get_player_game_log(player=rushers[i][0], position=rushers[i][1], season=rushers[i][2])
        game_log["Player"] = rushers[i][0]
        game_log["Pos"] = rushers[i][1]
        game_log["Season"] = rushers[i][2]
        game_log.rename(columns={'game_location': 'home'}, inplace=True)
        game_log['home'] = game_log['home'].replace(['@', ''], [0, 1])
        rushing_game_data.append(game_log)
    except:
        pass

    complete = round((i + 1) / len(rushers) * 100, 2)
    print(f"Rushers {complete}% complete")
    time.sleep(6)

rushing_game_data = pd.concat(rushing_game_data)

rushing_game_data.to_csv('./rushing_game_data.csv')

# Receiving Game Data for all years

receiving_game_data = []

for i in range(0, len(receivers)):
    try:
        game_log = p.get_player_game_log(player=receivers[i][0], position=receivers[i][1], season=receivers[i][2])
        game_log["Player"] = receivers[i][0]
        game_log["Pos"] = receivers[i][1]
        game_log["Season"] = receivers[i][2]
        game_log.rename(columns={'game_location': 'home'}, inplace=True)
        game_log['home'] = game_log['home'].replace(['@', ''], [0, 1])
        receiving_game_data.append(game_log)
    except:
        pass

    complete = round((i + 1) / len(receivers) * 100, 2)
    print(f"Receivers {complete}% complete")
    time.sleep(6)

receiving_game_data = pd.concat(receiving_game_data)

receiving_game_data.to_csv('./receiving_game_data.csv')

