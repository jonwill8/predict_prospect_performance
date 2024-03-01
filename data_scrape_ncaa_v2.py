import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup
import unicodedata
from tqdm import tqdm
import time
import pickle

def get_tables(url):

    """
    Args:
    * URL to the player's NCAA stats page

    Returns:
    List the (per_game, per_min, per_poss, advanced) stats tables associated with the player 
      ^Assuming they exist
    """

    max_retries = 5

    for i in range(max_retries):
        try:
            response = requests.get(url, timeout=180)
            break
        except requests.exceptions.ConnectionError:
            if i < max_retries-1:
                print("Connection failed. Retrying in 30 seconds...")
                time.sleep(30)
            else:
                raise requests.exceptions.ConnectionError

    soup = BeautifulSoup(response.text, "html.parser")

    table_types = [table.get('id') for table in soup.find_all('table') if table.get('id') in ['players_per_game','players_per_min','players_per_poss','players_advanced']]

    tables = {table_type: soup.find("table", {"id": table_type}) for table_type in table_types}

    return tables

def parse_table_stats(player_name, stats_table):
    """
    Args:
    * Player's Name (string) 
    * Player's Stats Soup Table (per_game, per_min, per_poss, advanced)
    Returns:
    dict mapping per game stats to associated value for player
      Note: we only pull the per game stats for the last season the player played in college
    """

    rows = stats_table.find_all("tr")

    #init list of the stat field names
    column_names = [th.text.strip() for th in stats_table.find("tr").find_all("th")]
    column_names = column_names[3:]
    column_names.insert(0, 'Player Name')

    #pull stat data for last year in NCAA
    data = []

    for row in reversed(rows[:-1]):

        columns = row.find_all("td")

        if columns:
            player_stats = [column.text.strip() for column in columns[1:]]
            player_stats = player_stats[1:]
            player_stats.insert(0, player_name)
            data.append(player_stats)
            break

    player_per_stats_raw_df = pd.DataFrame(data, columns=column_names)

    per_game_stats_dict = player_per_stats_raw_df.to_dict('records')[0]

    #only retaining stats which have associated values
    remove_key_list = []

    for key in per_game_stats_dict.keys():

        if key!='Player Name' and per_game_stats_dict[key] != '':
            per_game_stats_dict[key] = float(per_game_stats_dict[key])

        elif key!='Player Name':
            remove_key_list.append(key)

    for key in remove_key_list:
        del per_game_stats_dict[key]

    return per_game_stats_dict


def scrape_player_stats(player_df):
    
    # init log of players who stat queries fail
    error_players_log = []

    # init data log lists
    per_game_raw_stats_log = []
    per_40_min_stats_log = []
    per_100_poss_stats_log = []
    per_game_advanced_stats_log = []

    # init count of number of players who have been processed (success or fail)
    processed_player_count = 0

    # init count of number of players whose data was successfully scrapped
    scrapped_player_count = 0

    # init a save interval
    save_interval = 10

    # init count of number of players who have been processed (success of fail)

    #pull data logs with start for each player
    for player, url in tqdm(zip(player_df['Name'],player_df['URL'])):

        processed_player_count += 1

        #logic to pause webscrape code execution to avoid rate-timeout
        time.sleep(6)

        player_tables = get_tables(url)

        if len(player_tables) == 0:
            print(f'Failed to Scrape Stats for: {player}')
            error_players_log.append(player)

        else:

            #updating console with number of players whose data has been scrapped
            scrapped_player_count+=1

            if scrapped_player_count%10==0:
                print(f'Scraped Data for {scrapped_player_count} Players')

            for table_type, table in player_tables.items(): 

                if table_type == 'players_per_game':
                    per_game_raw_stats_log.append(parse_table_stats(player, table))
                elif table_type == 'players_per_min':
                    per_40_min_stats_log.append(parse_table_stats(player, table))
                elif table_type == 'players_per_poss':
                    per_100_poss_stats_log.append(parse_table_stats(player, table))
                elif table_type == 'players_advanced':
                    per_game_advanced_stats_log.append(parse_table_stats(player, table))

        # periodically saving data logs to memory
        if processed_player_count%10 == 0:

            with open("error_player_list_round_2.pickle", "wb") as f:
                pickle.dump(error_players_log, f)

            with open("per_game_raw_stats_log_round_2.pickle", "wb") as f:
                pickle.dump(per_game_raw_stats_log, f)

            with open("per_40_min_stats_log_round_2.pickle", "wb") as f:
                pickle.dump(per_40_min_stats_log, f)

            with open("per_100_poss_stats_log_round_2.pickle", "wb") as f:
                pickle.dump(per_100_poss_stats_log, f)

            with open("per_game_advanced_stats_log_round_2.pickle", "wb") as f:
                pickle.dump(per_game_advanced_stats_log, f)


    return [error_players_log, per_game_raw_stats_log, per_40_min_stats_log, per_100_poss_stats_log, per_game_advanced_stats_log]


if __name__ == '__main__':
    
    #import CSV of players whose data we failed to scrape in round 1
    player_df = pd.read_csv('/Users/jonathan.williams/Desktop/CS229_Final_Project/error_players.csv')

    #save the list of players who data couldn't be accessed to memory
    error_players_log, per_game_raw_stats_log, per_40_min_stats_log, per_100_poss_stats_log, per_game_advanced_stats_log = scrape_player_stats(player_df)

    print(f'Error Players: {error_players_log}')
    print(f'Raw Stats Log: {per_game_raw_stats_log}')
    print(f'Per 40 Minutes Stats Log: {per_40_min_stats_log}')
    print(f'Per 100 Possessions Stats Log: {per_100_poss_stats_log}')
    print(f'Advanced Stats Log: {per_game_advanced_stats_log}')

    with open("error_player_list_round_2.pickle", "wb") as f:
        pickle.dump(error_players_log, f)

    with open("per_game_raw_stats_log_round_2.pickle", "wb") as f:
        pickle.dump(per_game_raw_stats_log, f)

    with open("per_40_min_stats_log_round_2.pickle", "wb") as f:
        pickle.dump(per_40_min_stats_log, f)

    with open("per_100_poss_stats_log_round_2.pickle", "wb") as f:
        pickle.dump(per_100_poss_stats_log, f)

    with open("per_game_advanced_stats_log_round_2.pickle", "wb") as f:
        pickle.dump(per_game_advanced_stats_log, f)