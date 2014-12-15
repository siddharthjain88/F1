########################################################################################
#	IMPORTS
########################################################################################

import MySQLdb
import pymongo
import datetime

########################################################################################
#	CONSTANTS
########################################################################################

# MONGO CONFIGURATION
mongoHost = "localhost"
mongoPort = 27017

# SQL CONFIGURATION
sqlHost = "localhost"
sqlUser = "root"
sqlPassword = "root"
sqlDB = "f1"
					
########################################################################################
#	MAIN
########################################################################################

def main():
	print("\n## STARTING PROGRAM ##\n")
	lapTimes = sqlGetLapTimes()
	storeInMongo(lapTimes)

########################################################################################
#	SQL FUNCTIONS
########################################################################################	
	
def sqlGetLapTimes():
	print("## CONNECTING TO SQL DATABASE %s ON HOST %s ##\n"%(sqlDB,sqlHost))
	db = MySQLdb.connect(host=sqlHost, user=sqlUser, passwd=sqlPassword, db=sqlDB)
	cur = db.cursor()
	cur.execute("SELECT DISTINCT(raceId) FROM lapTimes")
	print("## RETREIVING RACES ##\n")
	races = []
	for row in cur.fetchall():
		races.append(int(row[0]))
	print("## RETREIVING DRIVERS ##\n")
	raceD = {}
	for race in races:
		drivers = []
		cur.execute("SELECT DISTINCT(driverId) FROM lapTimes WHERE raceId = %d"%race)
		for row in cur.fetchall():
			drivers.append(int(row[0]))
		raceD[race] = drivers
	print("## RETREIVING LAPTIMES ##\n")
	lapTimes = {}
	for race in races:
		dlapD = {}
		drivers = raceD[race]
		for driver in drivers:
			lap = []
			cur.execute("SELECT DISTINCT(milliseconds) FROM lapTimes WHERE raceId = %d AND driverId = %d"%(race,driver))
			for row in cur.fetchall():
				lap.append(int(row[0]))
			dlapD[driver] = lap
		lapTimes[race] = dlapD
	cur.close()
	db.close()
	return lapTimes

########################################################################################
#	MONGODB FUNCTIONS
########################################################################################	
	
def storeInMongo(lapTimes):
	print("## CONNECTING TO MONGO DATABASE %s ON PORT %d ##\n"%(mongoHost,mongoPort))
	client = pymongo.MongoClient(mongoHost, mongoPort)
	db = client.f1_db
	print("## STORING LAPTIMES ##\n")
	for raceId in lapTimes.keys():
		print "Race " + str(raceId)
		race = db.seasons.find_one({"races.raceId":raceId},{"races.$.raceId":1})["races"][0]
		for constructor in race["constructors"]:
			if ("drivers" in constructor.keys()):
				for driver in constructor["drivers"]:
					if (driver["driverId"] in lapTimes[raceId].keys()):
						lapTime = lapTimes[raceId][driver["driverId"]]
						driver["lapTimes"] = lapTime
		db.seasons.update({"races.raceId":raceId},{"$set":{"races.$.constructors":race["constructors"]}},True,False)
		if ("drivers" in race.keys()):
			for driver in race["drivers"]:
				if (driver["driverId"] in lapTimes[raceId].keys()):
					lapTime = lapTimes[raceId][driver["driverId"]]
					driver["lapTimes"] = lapTime
			db.seasons.update({"races.raceId":raceId},{"$set":{"races.$.drivers":race["drivers"]}},True,False)
	client.close()
		
########################################################################################
#	PYTHON CONFIGURATION
########################################################################################
		
if __name__ == "__main__":
	main()