import sqlite3
from sqlite3 import Error
import requests
import json
import pandas as pd
import time


def get_config_value(key):
    with open(config_file) as r1:
        content = json.load(r1)
        value = content[key]
    return value



def get_league_matches():
	league_id = get_config_value('league_id')
	url = get_config_value('url_matches').format(league_id)
	r = requests.get(url).json()
	return r


def create_connection(dbFile):
    try:
        conn = sqlite3.connect(dbFile)
        return conn
    except Error as e:
        print (e)

def delete_query(query):
	conn = create_connection(db)
	curr = conn.cursor()
	curr.execute(query)
	conn.commit()
	affected_rowcount = curr.rowcount
	curr.close()
	conn.close()
	print('Number of rows deleted: {}'.format(affected_rowcount))

def run_query(query):
	conn = create_connection(db)
	curr = conn.cursor()
	result = curr.execute(query).fetchall()
	curr.close()
	conn.close()
	return result

def drop_table(tableName):
    conn = create_connection(db)
    curr = conn.cursor()
    curr.execute("DROP TABLE {}".format(tableName))
    conn.commit()
    curr.close()
    conn.close()


def drop_all_tables():
	try:
		table = 'pick_ban'
		drop_table(table)
		print('Table dropped: {}'.format(table))
	except:
		pass
	try:
		table = 'match'
		drop_table(table)
		print('Table dropped: {}'.format(table))
	except:
		pass
	try:
		table = 'hero_ref'
		drop_table(table)
		print('Table dropped: {}'.format(table))
	except:
		pass
	try:
		table = 'player'
		drop_table(table)
		print('Table dropped: {}'.format(table))
	except:
		pass



def load_hero():
	conn = create_connection(db)
	url = get_config_value('url_heros')
	r = requests.get(url).json()
	df = pd.json_normalize(r, sep="_")
	df.astype(str).to_sql('hero_ref', conn, if_exists='replace', index=False)
	print('hero_ref table has been refreshed. Heroes added: {}'.format(len(df)))
	conn.close()

def load_match_details():
	conn = create_connection(db)
	m_id = [x['id'] for x in get_league_matches()]

	counter = 0
	for m in m_id:
		check_exist = 0
		try:
			query = "select count(*) from match where match_id = '{}'".format(m)
			check_exist = run_query(query)[0][0]
		except Exception as e:
			print("*****\tSomething wrong!\n\t" + str(e))
		if check_exist > 0 :
			pass
            #print('{} already exists'.format(m))
		elif check_exist == 0:	
			url = get_config_value('url_details').format(str(m))
			md = requests.get(url).json()

			# get match info
			df = pd.json_normalize(md, sep="_")
			fil = ['match_id', 'dire_score', 'dire_team_id','duration','first_blood_time','leagueid','radiant_score','radiant_team_id','radiant_win','start_time','league_name','league_tier','radiant_team_team_id','radiant_team_name','radiant_team_logo_url','dire_team_team_id','dire_team_name','dire_team_logo_url']
			df_fil = df.loc[:, fil]
			df_fil['duration'] = (df_fil.loc[:, 'duration']/60).round(2)

			# get hero info
			df_h = df.loc[:, ['picks_bans', 'radiant_team_id', 'dire_team_id', 'radiant_team_name', 'dire_team_name']].explode('picks_bans')
			df_h = pd.concat([df_h.loc[:, 'picks_bans'].apply(pd.Series), df_h.drop('picks_bans', axis = 1)], axis = 1)
			df_h['team_id'] = [x.get('radiant_team_id') if x.get('team') == 0 else x.get('dire_team_id') for i, x in df_h.iterrows()]
			df_h['team_name'] = [x.get('radiant_team_name') if x.get('team') == 0 else x.get('dire_team_name') for i, x in df_h.iterrows()]

			# get player info
			df_p = df[['players']].explode('players')
			df_p = df_p['players'].apply(pd.Series)
			fil = ['match_id', 'player_slot', 'account_id', 'assists', 'camps_stacked', 'creeps_stacked', 'damage', 'damage_inflictor', 'damage_inflictor_received', 'damage_taken', 'damage_targets', 'deaths', 'denies', 'gold', 'gold_per_min', 'gold_spent', 'hero_damage', 'hero_healing', 'hero_hits', 'hero_id', 'kills', 'last_hits', 'net_worth', 'tower_damage', 'xp_per_min', 'personaname', 'name', 'patch', 'isRadiant', 'win', 'lose', 'total_gold', 'total_xp', 'kda']
			df_p = df_p.loc[:, fil]

			# load match, hero, player info to sqlite
			df_fil.astype(str).to_sql('match', conn, if_exists='append', index=False)
			df_h.astype(str).to_sql('pick_ban', conn, if_exists='append', index=False)
			df_p.astype(str).to_sql('player', conn, if_exists='append', index=False)

			time.sleep(2)
			counter += 1
			print('{} inserted successfully'.format(m))
	print('Total new matches inserted: {}'.format(counter))
	conn.close()

if __name__ == '__main__':
	config_file = 'config.json'
	db = get_config_value('database')

	#drop_all_tables()
	#load_hero()
	load_match_details()