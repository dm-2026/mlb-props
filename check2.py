import json 
d = json.load(open('data/data.json')) 
hou = [g for g in d['games'] if g['away_team']=='HOU' or g['home_team']=='HOU'] 
[print(g['away_team'],'@',g['home_team'], g.get('away_probable',{}).get('name','TBD'), g.get('home_probable',{}).get('name','TBD')) for g in hou] 
