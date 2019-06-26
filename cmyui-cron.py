import redis
import mysql.connector
from mysql.connector import errorcode
import time
import configparser

# Console colours
CYAN		= '\033[96m'
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


if __name__ == "__main__":
    print(CYAN + "Running cron to calculate PP for all users in all gamemodes.\n" + ENDC)

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

    print(GREEN + "Calculations complete.\n\nTime taken: {}".format(time.time() - start_time) + ENDC)