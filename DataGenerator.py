import MySQLdb
import csv
import sys
from random import randrange

############################################################################################################
##	Usage: python DataGenerator.py <Tourneys.csv> <Players.csv> <participant.csv> <tournament.csv>
############################################################################################################

# Connect to the KSU MySQL database
print("connecting")
db = MySQLdb.connect(	host = "mysql.cis.ksu.edu", 
						user = "vanlan", 
						passwd = "insecurepassword", 
						db = "vanlan")
print("connected")

# We need a cursor object to perform all of our insertions.
cur = db.cursor()
print("cursor set...")

###################################
##	SQL QUERIES
###################################

# (mid, pid1, pid2, cid1, cid2, seed, tid) * Auto Increment on 'mid'
insert_match_sql = "INSERT INTO match VALUES ('', %s, %s, %s, %s, %s, %s)"

# (oid, mid, winner, loser, score, time) * Auto Increment on 'oid'
insert_outcome_sql = "INSERT INTO outcome VALUES ('', %s, %s, %s, %s, %s))"

# (tid, pid)
insert_participant_sql = "INSERT INTO participant VALUES (%s, %s)"

# (pid, name) * Auto Increment on 'pid'
insert_player_sql = "INSERT INTO player VALUES ('', %s)"

# (tid, name, date) * Auto Increment on 'tid'
insert_tournament_sql = "INSERT INTO tournament VALUES ('', %s, %s)"

collect_pids = "SELECT COUNT(*) FROM player";

###################################
##	FUNCTIONS
###################################

def insert_match(sql_query, pid1, pid2, cid1, cid2, seed, tid):
	print("called: insert_match(%s, %s, %s, %s, %s, %s, %s)" % (sql_query, pid1, pid2, cid1, cid2, seed, tid))

def insert_outcome(sql_query, mid, winner, loser, score, time):
	print("called: insert_outcome(%s, %s, %s, %s, %s, %s)" % (sql_query, mid, winner, loser, score, time))

def insert_participant(sql_query, tid, pid):
	print("called: insert_participant(%s, %s, %s)" % (sql_query, tid, pid))

def insert_player(sql_query, name):
	print("called: insert_player(%s, %s)" % (sql_query, name))
	#cur.execute(sql_query, (name,))

def insert_tournament(sql_query, name, date):
	print("called: insert_tournament(%s, %s, %s)" % (sql_query, name, date))
	cur.execute(sql_query, (name, date,))


def generate_rand_date():
	year = "2014"
	month = randrange(1, 12)
	day = ""
	if month == 2:
		day = randrange(1, 28)
	elif month in [1, 3, 5, 7, 8, 10, 12]:
		day = randrange(1, 31)
	elif month in [2, 4, 6, 9, 11]:
		day = randrange(1, 30)
	return year + "-" + str(month) + "-" + str(day)

def generate_rand_character():
	return str(randrange(1, 12))

def generate_rand_players():
	row = cur.execute(collect_pids)
	maxPlayer = row[0]
	rand_player1 = randrange(1, maxPlayer)
	rand_player2 = randrange(1, maxPlayer) 
	# We want two separate players.
	while rand_player2 == rand_player1:
		rand_player2 = randrange(1, maxPlayer)

	return rand_player1, rand_player2

#def create_match():
#	pid1, pid2 = generate_rand_players()
#	cid1 = generate_rand_character()
#	cid2 = generate_rand_character()



def perform_insertions():
	# Tourneys
	tourney_csv = open(sys.argv[1], 'rt')
	try:
		tourney_reader = csv.DictReader(tourney_csv)
		for row in tourney_reader:
			insert_tournament(insert_tournament_sql, row['word'], generate_rand_date())
	finally:
		tourney_csv.close()
		db.commit()
	
	# Players
	player_csv = open(sys.argv[2], 'rt')
	try:
		player_reader = csv.DictReader(player_csv)
		for row in player_reader:
			insert_player(insert_player_sql, row['Names'])
	finally:
		player_csv.close()
		db.commit()


perform_insertions()
