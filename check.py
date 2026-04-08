import json 
d = json.load(open('data/data.json')) 
all_ids = {t['batter_id'] for t in d['targets']} 
print('670541 in targets:', 670541 in all_ids) 
