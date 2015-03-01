__author__ = 'xant'
import MySQLdb
import csv
import sys
from random import randrange

################################################################
##      Usage: python DataGenerator.py Tourneys.csv Players.csv
################################################################
# Connect to the KSU MySQL database
print("connecting")
db = MySQLdb.connect(   host = "host_name_here",
                                                user = "user",
                                                passwd = "password",
                                                db = "user")
print("connected")

# We need a cursor object to perform all of our insertions/queries.
cur = db.cursor()
print("cursor set...")

###################################
##      SQL QUERIES
###################################

# (mid, pid1, pid2, cid1, cid2, seed, tid) * Auto Increment on 'mid'
insert_match_sql = "INSERT INTO `match` VALUES ('', %s, %s, %s, %s, %s, %s)"

# (oid, mid, winner, loser, score, time) * Auto Increment on 'oid'
insert_outcomes_sql = "INSERT INTO `outcomes` VALUES ('', %s, %s, %s, %s, %s)"

# (tid, pid)
insert_participant_sql = "INSERT INTO `participant` VALUES (%s, %s)"

# (pid, name) * Auto Increment on 'pid'
insert_player_sql = "INSERT INTO `player` VALUES ('', %s)"

# (tid, name, date) * Auto Increment on 'tid'
insert_tournament_sql = "INSERT INTO `tournament` VALUES ('', %s, %s)"

collect_pids = "SELECT * FROM `player`"

count_pids = "SELECT COUNT(*) FROM `player`"

collect_tier_sql = "SELECT mid, pid1, pid2, cid1, cid2 FROM `match` WHERE tid = %s AND seed = %s"

###################################
##      FUNCTIONS
###################################

def insert_match(sql_query, pid1, pid2, cid1, cid2, seed, tid):
        print("called: insert_match(%s, %s, %s, %s, %s, %s, %s)" % (sql_query, pid1, pid2, cid1, cid2, seed, tid))
        cur.execute(sql_query, (pid1, pid2, cid1, cid2, seed, tid,))

def insert_outcomes(sql_query, mid, winner, loser, score, time):
        print("called: insert_outcomes(%s, %s, %s, %s, %s, %s)" % (sql_query, mid, winner, loser, score, time))
        cur.execute(sql_query, (mid, winner, loser, score, time,))

def insert_participant(sql_query, tid, pid):
        print("called: insert_participant(%s, %s, %s)" % (sql_query, tid, pid))
        cur.execute(sql_query, (tid, pid,))

def insert_player(sql_query, name):
        print("called: insert_player(%s, %s)" % (sql_query, name))
        cur.execute(sql_query, (name,))

def insert_tournament(sql_query, name, date):
        print("called: insert_tournament(%s, %s, %s)" % (sql_query, name, date))
        cur.execute(sql_query, (name, date,))

# Returns set of players and their match in the given tournament and match seed
def collect_tier(sql_query, tid, seed):
        print("called: collect_tier(%s, %s, %s)" % (sql_query, tid, seed))
        cur.execute(sql_query, (tid, seed,))
        return cur.fetchone()

def generate_rand_date():
        year = "2014"
        month = randrange(1, 13)
        day = ""
        if month == 2:
                day = randrange(1, 29)
        elif month in [1, 3, 5, 7, 8, 10, 12]:
                day = randrange(1, 32)
        elif month in [2, 4, 6, 9, 11]:
                day = randrange(1, 31)
        return year + "-" + str(month) + "-" + str(day)

def generate_rand_character():
        return randrange(1, 13)

def generate_rand_players():
        players = []
        tourney_bracket = []
        cur.execute(collect_pids)
        rows = cur.fetchall()

        # Create our list of players
        for row in rows:
                players.insert(int(row[0]), int(row[0]))

        print(players)
        cur.execute(count_pids)
        row = cur.fetchone()
        maxPlayer = row[0]
        print(maxPlayer)

        bracket_done = bool(0)
        counter = 0
        # Generate 16 distinct players to fill the bracket
        while bracket_done != bool(1):
                rand_player = randrange(1, maxPlayer + 1)
                print("Random player: %s" % (rand_player))
                if rand_player in players:
                        tourney_bracket.append(players.pop(players.index(rand_player)))
                        counter += 1
                        if counter == 16:
                                bracket_done = bool(1)
                        print(players)
        print(tourney_bracket)
        return tourney_bracket

# Create tournament, inserting random matches with outcomes
# and inserting all participants into the tournament.
# Assuming 5 stock between 0 and 20 minutes per game.
def generate_tournament_tiers(tid):
        tourney_bracket = generate_rand_players()

        seed = 15
        # Generate initial random tier 1
        for i in range(8):
                pid1 = tourney_bracket.pop()
                pid2 = tourney_bracket.pop()
                cid1 = generate_rand_character()
                cid2 = generate_rand_character()
                insert_match(insert_match_sql, pid1, pid2, cid1, cid2, seed, tid)
                insert_participant(insert_participant_sql, tid, pid1)
                insert_participant(insert_participant_sql, tid, pid2)
                seed -= 1

        db.commit()
        # Generate tier 2
        create_tournament_matches(tid, 1, 8, 2, 15)

        # Generate tier 3
        create_tournament_matches(tid, 9, 12, 2, 7)

        # Generate tier 4
        create_tournament_matches(tid, 13, 14, 2, 3)

        # Generate winner
        row = collect_tier(collect_tier_sql, tid, 1)
        final_mid = row[0]
        score = randrange(1, 6)
        time = randrange(0, 21)
        rand_winner = randrange(1, 3)
        winning_pid = row[rand_winner]
        if rand_winner == 1:
                losing_pid = row[2]
        else:
                losing_pid = row[1]
        insert_outcomes(insert_outcomes_sql, final_mid, winning_pid, losing_pid, score, time)
        db.commit()

def create_tournament_matches(tid, beginning, end, increment, seed):
        for i in range(beginning, end, increment):
                row = collect_tier(collect_tier_sql, tid, seed)
                mid = row[0]
                score = randrange(1, 6)
                time = randrange(0, 21)
                rand_winner = randrange(1, 3)
                winning_pid1 = row[rand_winner]
                if rand_winner == 1:
                        losing_pid = row[2]
                        character1 = row[3]
                else:
                        losing_pid = row[1]
                        character1 = row[4]

                insert_outcomes(insert_outcomes_sql, mid, winning_pid1, losing_pid, score, time)

                row2 = collect_tier(collect_tier_sql, tid, seed - 1)
                mid = row2[0]
                score = randrange(1, 6)
                time = randrange(0, 21)
                rand_winner = randrange(1, 3)
                winning_pid2 = row2[rand_winner]
                if rand_winner == 1:
                        losing_pid = row2[2]
                        character2 = row2[3]
                else:
                        losing_pid = row2[1]
                        character2 = row2[4]
                insert_outcomes(insert_outcomes_sql, mid, winning_pid2, losing_pid, score, time)
                insert_match(insert_match_sql, winning_pid2, winning_pid1, character2, character1, (seed - 1) / 2, tid)
                seed -= 2
        db.commit()

def create_tournaments(num_tournaments):
        for i in range(num_tournaments):
                generate_tournament_tiers(i + 1)

def perform_insertions():
        # Load tourneys
        tourney_csv = open(sys.argv[1], 'rt')
        try:
                tourney_reader = csv.DictReader(tourney_csv)
                for row in tourney_reader:
                        insert_tournament(insert_tournament_sql, row['word'], generate_rand_date())
        finally:
                tourney_csv.close()
                db.commit()

        # Load players
        player_csv = open(sys.argv[2], 'rt')
        try:
                player_reader = csv.DictReader(player_csv)
                for row in player_reader:
                        insert_player(insert_player_sql, row['Names'])
        finally:
                player_csv.close()
                db.commit()

        # Create 20 tournament brackets with matches and outcomes.
        create_tournaments(100)


perform_insertions()
