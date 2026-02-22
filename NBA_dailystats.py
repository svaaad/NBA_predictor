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

import requests
import random
import time

def get_free_proxy():
    """Fetches a random free proxy, trying multiple APIs as backups."""
    proxy_apis = [
        "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=us&ssl=yes&anonymity=elite",
        "https://www.proxy-list.download/api/v1/get?type=http&anon=elite&country=US"
    ]
    
    for url in proxy_apis:
        try:
            print(f"Asking {url.split('/')[2]} for a disguise...")
            # We use custom headers here too, just in case the proxy sites are picky
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                # Clean up the response and split by line breaks
                proxies = response.text.strip().replace('\r', '').split('\n')
                proxies = [p for p in proxies if p] # Remove empty strings
                
                if proxies:
                    chosen_proxy = random.choice(proxies)
                    return f"http://{chosen_proxy}"
        except Exception as e:
            print(f" -> Failed to pull from this proxy source: {e}")
            
    return None

def fetch_with_retry(endpoint_class, retries=5, **kwargs):
    for attempt in range(retries):
        
        current_proxy = get_free_proxy()
        
        if current_proxy is None:
            print("❌ CRITICAL: Could not find any free proxies. The API sources are blocking GitHub Actions.")
            # If we don't have a proxy, we sleep and hope a source unblocks us on the next loop
            time.sleep(10)
            continue 
            
        print(f"\nAttempt {attempt+1}: Routing traffic through proxy {current_proxy}...")
        
        try:
            endpoint = endpoint_class(**kwargs, headers=HEADERS, timeout=120, proxy=current_proxy)
            df = endpoint.get_data_frames()[0]
            print("✅ Data successfully bypassed the NBA firewall!")
            return df
            
        except Exception as e:
            if attempt == retries - 1:
                print("❌ All proxy attempts failed to pierce the firewall.")
                raise e
            
            delay = 3 * (attempt + 1)
            print(f"Proxy timeout or NBA rejected the IP. Grabbing a new one in {delay} seconds...")
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
