import pandas as pd
import json
import time
from datetime import datetime, timedelta
from nba_api.stats.endpoints import leaguegamefinder, teamgamelogs, scoreboardv2

# --- CONFIGURATION ---
SEASON_STR = '2025-26'
ROLLING_WINDOW = 10

# Disguise headers
HEADERS = {
    'Host': 'stats.nba.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://stats.nba.com/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

def fetch_with_retry(endpoint_class, retries=4, **kwargs):
    for attempt in range(retries):
        try:
            endpoint = endpoint_class(**kwargs, headers=HEADERS, timeout=120)
            return endpoint.get_data_frames()[0]
        except Exception as e:
            if attempt == retries - 1:
                raise e
            delay = 5 * (attempt + 1)
            print(f"API blocked. Sleeping {delay}s...")
            time.sleep(delay)

def run_pipeline():
    print("Starting background data pipeline...")
    
    # 1. Fetch Stats
    print("Fetching basic stats...")
    basic_df = fetch_with_retry(
        leaguegamefinder.LeagueGameFinder, league_id_nullable='00', 
        season_nullable=SEASON_STR, season_type_nullable='Regular Season'
    )
    basic_df = basic_df[basic_df['WL'].notna()]
    
    time.sleep(4) # Firewall breather
    
    print("Fetching advanced stats...")
    adv_df = fetch_with_retry(
        teamgamelogs.TeamGameLogs, league_id_nullable='00', 
        season_nullable=SEASON_STR, season_type_nullable='Regular Season', 
        measure_type_player_game_logs_nullable='Advanced'
    )
    
    # 2. Merge Data
    adv_cols = ['GAME_ID', 'TEAM_ID', 'OFF_RATING', 'DEF_RATING', 'PACE', 'TS_PCT', 'AST_TO', 'REB_PCT']
    basic_df['GAME_ID'], basic_df['TEAM_ID'] = basic_df['GAME_ID'].astype(str), basic_df['TEAM_ID'].astype(str)
    adv_df['GAME_ID'], adv_df['TEAM_ID'] = adv_df['GAME_ID'].astype(str), adv_df['TEAM_ID'].astype(str)
    
    df = pd.merge(basic_df, adv_df[adv_cols], on=['GAME_ID', 'TEAM_ID'])
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
    df = df.sort_values(by=['TEAM_ID', 'GAME_DATE'])
    
    # 3. Calculate Form & Save as JSON
    stats_cols = [
        'PTS', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT',
        'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 
        'STL', 'BLK', 'TOV', 'PF', 'PLUS_MINUS',
        'OFF_RATING', 'DEF_RATING', 'PACE', 'TS_PCT', 'AST_TO', 'REB_PCT'
    ]
    current_stats = {}
    for team_id, team_data in df.groupby('TEAM_ID'):
        last_stats = team_data[stats_cols].rolling(window=ROLLING_WINDOW, min_periods=1).mean().iloc[-1].to_dict()
        current_stats[str(team_id)] = {f"{k}_ROLL": v for k, v in last_stats.items()}
        
    with open('team_form.json', 'w') as f:
        json.dump(current_stats, f)
    print("Saved team_form.json")

    # 4. Fetch Tomorrow's Schedule & Save as CSV
    target_date = datetime.now() + timedelta(days=1)
    date_str = target_date.strftime('%Y-%m-%d')
    board = scoreboardv2.ScoreboardV2(game_date=date_str, headers=HEADERS, timeout=60)
    games = board.game_header.get_data_frame()
    
    # Add the date to the dataframe so the app knows what day this is for
    games['SCHEDULED_DATE'] = date_str 
    games.to_csv('schedule.csv', index=False)
    print("Saved schedule.csv")
    print("Pipeline complete!")

if __name__ == "__main__":
    run_pipeline()