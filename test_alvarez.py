from pybaseball import statcast_batter 
df = statcast_batter('2025-03-20', '2025-10-05', 670541) 
print('2025 rows:', len(df)) 
print('Pitch types:', df['pitch_type'].value_counts().head()) 
