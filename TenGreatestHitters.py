import pymysql
import sys

sys.path.append('/Users/roxanne/PycharmProjects/db')
from RDBDataTable import RDBDataTable
import csv

conn = pymysql.connect(host='localhost', user='root', password='', db='hw1', port=3306, charset='utf8')
cursor = conn.cursor()

def top10_sql():
    #tbl= RDBDataTable("people",['playerID'])
    #q2 = "ALTER TABLE People ADD PRIMARY KEY (playerID);"
    #cursor.execute(q2)
    #q3 = "ALTER TABLE `4111DB`.`batting` \
    #CHANGE COLUMN `playerID` `playerID` VARCHAR(16) NOT NULL ,\
    #CHANGE COLUMN `yearID` `yearID` VARCHAR(16) NOT NULL ,\
    #CHANGE COLUMN `stint` `stint` VARCHAR(16) NOT NULL ,\
    #CHANGE COLUMN `teamID` `teamID` VARCHAR(16) NOT NULL ,\
    #ADD PRIMARY KEY (`playerID`, `yearID`, `stint`, `teamID` );"
    #cursor.execute(q3)
    q = "SELECT Batting.playerID, (SELECT People.nameFirst FROM People WHERE People.playerID=Batting.playerID) as first_name, \
    (SELECT People.nameLast FROM People WHERE People.playerID=Batting.playerID) as last_name, \
    sum(Batting.h)/sum(batting.ab) as career_average, \
    sum(Batting.h) as career_hits, \
    sum(Batting.ab) as career_at_bats,\
    min(Batting.yearID) as first_year, \
    max(Batting.yearID) as last_year \
    FROM \
    Batting \
    GROUP BY \
    playerId \
    HAVING \
    career_at_bats > 200 AND last_year >= 1960 \
    ORDER BY \
    career_average DESC \
    LIMIT 10;"
    cursor.execute(q)
    result = cursor.fetchmany(10)

    return result

def top10_without_sql():
    # read csv
    data = []
    people = []
    with open('Batting.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            data.append(row)
    with open('People.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            people.append(row)

    # remove headers
    df = data[1:]
    people = people[1:]

    # Find players who have at least one record on or after 1960
    select_player = []
    for i in range(len(df)):
        if int(df[i][1]) >= 1960:
            select_player.append(data[i][0])
    select_player = set(select_player)

    # Find all the records for selected players
    selected = []
    for i in range(len(df)):
        if df[i][0] in select_player:
            selected.append(df[i])

    # Compute their batting average
    result = []

    for i in select_player:
        AB = 0
        H = 0
        first_year = None
        last_year = None
        for j in range(len(df)):
            if df[j][0] == i:
                # Compute their first year and last year
                if first_year is None:
                    first_year = int(df[j][1])
                else:
                    if int(df[j][1]) < first_year:
                        first_year = int(df[j][1])
                if last_year is None:
                    last_year = int(df[j][1])
                else:
                    if int(df[j][1]) > last_year:
                        last_year = int(df[j][1])

                # Compute sum of avg
                AB += float(df[j][6])
                H += float(df[j][8])

        # Only if the player's career_at_bats > 200
        if AB < 200:
            batting_avg = 0
        else:
            batting_avg = H / AB
        result.append([i, AB, H, first_year, last_year, batting_avg])

    # Append players' last name and first name
    for i in range(len(result)):
        for j in range(len(people)):
            if result[i][0] == people[j][0]:
                result[i].append(people[j][14])
                result[i].append(people[j][13])

    # Sort by batting average, find the top 10 and add labels
    ans = sorted(result, key=lambda x: x[5], reverse=True)
    answer = ans[:10]
    answer.insert(0, ['playerID', 'career_at_bats', 'career_hits', 'first_year', 'last_year',
                      'career_average', 'nameFirst', 'nameLast'])
    return answer



