import pickle

if __name__ == '__main__':

    with open('error_player_list.pickle', 'rb') as f:
        error_player_list = pickle.load(f)

    with open('per_40_min_stats_log.pickle', 'rb') as f:
        per_40_min_stats_log = pickle.load(f)

    with open('per_100_poss_stats_log.pickle', 'rb') as f:
        per_100_poss_stats_log = pickle.load(f)

    with open('per_game_advanced_stats_log.pickle', 'rb') as f:
        per_game_advanced_stats_log = pickle.load(f)

    with open('per_game_raw_stats_log.pickle', 'rb') as f:
        per_game_raw_stats_log = pickle.load(f)

    print(error_player_list)
    print(len(error_player_list))
    #print(per_40_min_stats_log)
    #print(per_100_poss_stats_log)
    #print(per_game_advanced_stats_log)
    #print(per_game_raw_stats_log)