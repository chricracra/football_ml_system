from understatapi import UnderstatClient

# Use 'with' instead of 'async with'
with UnderstatClient() as understat:
    # get data for every player playing in the Premier League in 2023
    league_player_data = understat.league(league="EPL").get_player_data(season="2023")
    # Print info for the first 2 players to verify
    print(league_player_data[0:2])
