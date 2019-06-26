import redis
import mysql.connector
from mysql.connector import errorcode
import time
import configparser

# Akatsuki-cron-py version number.
VERSION = 1.05

# Console colours
CYAN		= '\033[96m'
MAGENTA     = '\033[95m'
GREEN 		= '\033[92m'
RED 		= '\033[91m'
ENDC 		= '\033[0m'

# Configuration.
config = configparser.ConfigParser()
config.sections()
config.read('config.ini')

# Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# MySQL
try:
    cnx = mysql.connector.connect(
        user       = str(config['mysql']['user']),
        password   = str(config['mysql']['passwd']),
        host       = str(config['mysql']['host']),
        database   = str(config['mysql']['db']),
        autocommit = True)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print(RED + "Something is wrong with your username or password." + ENDC)
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print(RED + "Database does not exist." + ENDC)
    else:
        print(RED + err + ENDC)
else:
    SQL = cnx.cursor()


def calculateRanks(): # Calculate hanayo ranks based off db pp values.
    print(CYAN + "Calculating ranks for all users in all gamemodes." + ENDC)

    start_time = time.time()

    tables = ["rx", "users"]

    modes = {
        0: "std",
        1: "taiko",
        2: "ctb",
        3: "mania"
    }

    for table in tables:
        for gamemode in range(0, 3):
            sql_prepare = """
            SELECT {t}_stats.id, {t}_stats.pp_{gm}, {t}_stats.country
            FROM {t}_stats
            ORDER BY pp_{gm}""".format(t=table, gm=modes.get(gamemode))

            SQL.execute(sql_prepare)
            query = SQL.fetchall()

            for do in query:
                userID  = do[0]
                pp      = do[1]
                country = do[2].lower()

                if country != "xx" and country != "":
                    r.zincrby("hanayo:country_list", country, 1)

                r.zadd("ripple:{rx}board:".format(rx="relax" if table == "rx" else "leader") + modes.get(gamemode), int(userID), float(pp))

                if country != "xx" and country != "":
                    r.zadd("ripple:{rx}board:".format(rx="relax" if table == "rx" else "leader") + modes.get(gamemode) + ":" + country, int(userID), float(pp))

    print(GREEN + "Successfully completed rank calculations." + ENDC)


def updateTotalScores(): # Update the main page values for total scores.
    print(CYAN + "Updating total score values." + ENDC)

    # Vanilla.
    SQL.execute("SELECT SUM(playcount_std) + SUM(playcount_taiko) + SUM(playcount_ctb) + SUM(playcount_mania) FROM users_stats WHERE 1")
    r.set("ripple:submitted_scores", str(round(int(SQL.fetchone()[0]) / 1000000, 2)) + "m")

    # Relax.
    SQL.execute("SELECT SUM(playcount_std) + SUM(playcount_taiko) + SUM(playcount_ctb) + SUM(playcount_mania) FROM rx_stats WHERE 1")
    r.set("ripple:submitted_scores_relax", str(round(int(SQL.fetchone()[0]) / 1000000, 2)) + "m")

    print(GREEN + "Successfully completed updating total score values." + ENDC)


if __name__ == "__main__":
    print(CYAN + "Akatsuki's cron - v{}.".format(VERSION) + ENDC)
    start_time = time.time()
    calculateRanks()
    updateTotalScores()
    print(GREEN + "Cronjob execution completed.\n" + MAGENTA + "Time: {}ms.".format(round((time.time() - start_time) * 1000, 2)) + ENDC)