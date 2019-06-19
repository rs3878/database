import pymysql
from social_graph.fan_comment import FanGraph


cnx = pymysql.connect(host='localhost',
                             user='dbuser',
                             password='dbuserdbuser',
                             db='lahman2017',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

fg = FanGraph(auth=('neo4j', '19980126'),
                  host="localhost",
                  port=7687,
                  secure=False)


def load_players():
    """
    Load a subset of the player data. We only load a subset of the columns and the rows.
    """

    q = "SELECT playerID, nameLast, nameFirst FROM People where  " + \
        "exists (select * from appearances where appearances.playerID = " + \
        " people.playerID and yearID >= 2010)"


    curs = cnx.cursor()
    curs.execute(q)

    # This is an example of using a cursor. We could have used fetchall() but
    # I wanted to give you an example of iterating using a cursor.
    r = curs.fetchone()
    cnt = 0

    # Loop until we are out of rows.
    while r is not None:
        print(r)
        cnt += 1
        if r is not None:
            p = fg.create_player(player_id=r['playerID'], last_name=r['nameLast'], first_name=r['nameFirst'])
            print("Created player = ", p)

        r = curs.fetchone()

    print("Loaded ", cnt, "records.")
def load_teams():
    q = "SELECT distinct teamid, name from teams where yearid >= 2000"

    curs = cnx.cursor()
    curs.execute(q)

    cnt = 0
    r = curs.fetchone()
    while r is not None:

        print(r)
        cnt += 1

        if r is not None:
            p = fg.create_team(team_id=r['teamid'], team_name=r['name'])
            print("Created team = ", p)

        r = curs.fetchone()

    print("Loaded ", cnt, "records.")
def load_appearances():

    q = \
    "SELECT distinct playerid, teamid, yearid, g_all as games " + \
    " from appearances where yearid >= 2010"

    curs = cnx.cursor()
    curs.execute(q)

    r = curs.fetchone()
    cnt = 0
    while r is not None:
        print(r)
        cnt += 1

        if r is not None:
            try:
                p = fg.create_appearance_all(team_id=r['teamid'], player_id=r['playerid'], \
                                        games=r['games'], year=r['yearid'])
                print("Created appearances = ", json.dumps(p))
            except Exception as e:
                print("Could not create.")
        r = curs.fetchone()

    print("Loaded ", cnt, "records.")
def load_fans():
    fg.create_fan(uni="js1", last_name="Smith", first_name="John")
    fg.create_fan(uni="ja1", last_name="Adams", first_name="John")
    fg.create_fan(uni="tj1", last_name="Jefferson", first_name="Thomas")
    fg.create_fan(uni="gw1", last_name="Washing", first_name="George")
    fg.create_fan(uni="jm1", last_name="Monroe", first_name="James")
    fg.create_fan(uni="al1", last_name="Lincoln", first_name="Abraham")
def load_follows_fans():
    fg.create_follows(follower="gw1", followed="js1")
    fg.create_follows(follower="tj1", followed="gw1")
    fg.create_follows(follower="ja1", followed="gw1")
    fg.create_follows(follower="jm1", followed="gw1")
    fg.create_follows(follower="tj1", followed="gw1")
    fg.create_follows(follower="al1", followed="jm1")
def create_supports():
    fg.create_supports("gw1", "WAS")
    fg.create_supports("ja1", "BOS")
    fg.create_supports("tj1", "WAS")
    fg.create_supports("jm1", "NYA")
    fg.create_supports("al1", "CHA")
    fg.create_supports("al1", "CHN")

load_players()
load_teams()
load_appearances()
load_fans()
load_follows_fans()
create_supports()