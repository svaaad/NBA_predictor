import streamlit as st
import pandas as pd
import joblib
import json
import os

st.set_page_config(page_title="NBA Predictor", page_icon="🏀", layout="centered")

st.title("🏀 NBA Advanced Game Predictor")
st.write("Predicting outcomes using advanced ML metrics (Pace, True Shooting, Def Rating).")

# --- LOAD STATIC DATA INSTANTLY ---
@st.cache_resource
def load_model():
    return joblib.load('nba_advanced_predictor.pkl')

@st.cache_data
def get_team_map():
    from nba_api.stats.static import teams
    return {str(t['id']): t['full_name'] for t in teams.get_teams()}

@st.cache_data
def load_local_data():
    if not os.path.exists('team_form.json') or not os.path.exists('schedule.csv'):
        return None, None
    with open('team_form.json', 'r') as f:
        team_stats = json.load(f)
    schedule = pd.read_csv('schedule.csv')
    return team_stats, schedule

# --- MAIN APP LOGIC ---
model = load_model()
team_map = get_team_map()
team_stats_dict, todays_games = load_local_data()

if team_stats_dict is None or todays_games is None:
    st.warning("⚠️ Data files not found. Please run `updater.py` to fetch the latest NBA data.")
else:
    date_str = todays_games['SCHEDULED_DATE'].iloc[0] if not todays_games.empty else "Unknown Date"
    st.markdown("---")
    st.subheader(f"Upcoming Slate: {date_str}")
    
    if todays_games.empty:
        st.info(f"No games scheduled for {date_str}.")
    else:
        # Build dropdown options
        game_options = {}
        for _, game in todays_games.iterrows():
            home_id = str(game['HOME_TEAM_ID'])
            away_id = str(game['VISITOR_TEAM_ID'])
            home_name = team_map.get(home_id, "Unknown")
            away_name = team_map.get(away_id, "Unknown")
            game_options[f"{away_name} @ {home_name}"] = (home_id, away_id)
            
        selected_matchup = st.selectbox("Select a Matchup:", list(game_options.keys()))
        
        if selected_matchup:
            home_id, away_id = game_options[selected_matchup]
            
            if home_id in team_stats_dict and away_id in team_stats_dict:
                home_features = team_stats_dict[home_id]
                away_features = {f"{k}_OPP": v for k, v in team_stats_dict[away_id].items()}
                
                features = {**home_features, **away_features}
                features['Is_Home'] = 1 
                
                # Predict
                feature_names = model.get_booster().feature_names
                input_df = pd.DataFrame([features]).reindex(columns=feature_names, fill_value=0)
                
                probs = model.predict_proba(input_df)[0]
                win_prob = probs[1]
                
                home_name = team_map.get(home_id, "Unknown")
                away_name = team_map.get(away_id, "Unknown")
                
                winner = home_name if win_prob > 0.5 else away_name
                confidence = max(win_prob, 1 - win_prob)
                
                st.markdown(f"### 🏆 Predicted Winner: **{winner}**")
                st.metric(label="Model Confidence", value=f"{confidence:.1%}")
            else:
                st.warning("Insufficient advanced data to predict this matchup.")