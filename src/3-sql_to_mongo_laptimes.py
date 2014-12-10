########################################################################################
#	IMPORTS
########################################################################################

import MySQLdb
import pymongo
import datetime
import pprint

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
	client.drop_database('f1_db_new')
	client.copy_database('f1_db', 'f1_db_new')
	assert set(client['f1_db'].collection_names()) == set(client['f1_db_new'].collection_names())
	for collection in client['f1_db'].collection_names():
		assert client['f1_db'][collection].count() == client['f1_db_new'][collection].count()
	db = client.f1_db
	
	
	
	
	#print("## CLEANING DBS ##")
	#newdb.drop_collection('races')
	#db.connection.drop_database('f1_db')
	#client.copy_database('f1_db_new', 'f1_db')
	#assert set(client['f1_db_new'].collection_names()) == set(client['f1_db'].collection_names())
	#for collection in client['f1_db_new'].collection_names():
	#	assert client['f1_db_new'][collection].count() == client['f1_db'][collection].count()
	#client.drop_database('f1_db_new')
	client.close()
		
########################################################################################
#	PYTHON CONFIGURATION
########################################################################################
		
if __name__ == "__main__":
	main()