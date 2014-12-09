########################################################################################
#	IMPORTS
########################################################################################

import pymongo
import datetime

########################################################################################
#	CONSTANTS
########################################################################################

# MONGO CONFIGURATION
mongoHost = "localhost"
mongoPort = 27017
					
########################################################################################
#	MAIN
########################################################################################

def main():
	print("\n## STARTING PROGRAM ##\n")
	[client,db,newdb] = connectToMongo()
	refactorRaces(db,newdb)
	refactorSeasons(db,newdb)
	client.close()

def connectToMongo():
	print("## CONNECTING TO MONGO DATABASE %s ON PORT %d ##\n"%(mongoHost,mongoPort))
	client = pymongo.MongoClient(mongoHost, mongoPort)
	client.drop_database('f1_db_new')
	newdb = client.f1_db_new
	db = client.f1_db
	return client,db,newdb
	
def refactorRaces(db,newdb):
	print("## REFACTORING CONSTRUCTORS ##")
	cursor = db.races.find()
	for race in cursor:
		constructors = race["constructors"]
		drivers = race["drivers"]
		for driver in drivers:
			if("constructor" in driver.keys()):
				cId = driver["constructor"]["constructorId"]
				for constructor in constructors:
					if(constructor["constructorId"] == cId):
						if("drivers" in constructor.keys()):
							cdrivers = constructor["drivers"]
						else:
							cdrivers = []
						cdrivers.append(driver)
						constructor["drivers"] = cdrivers
						break
			#else:
				#print("Error: No constructor for driver id %s"%driver["driverId"])
		race["constructors"] = constructors
		def f(x): return not("constructor" in x.keys())
		ndrivers = filter(f,drivers)
		race["drivers"] = ndrivers
		for constructor in race["constructors"]:
			if ("drivers" in constructor.keys()):
				for driver in constructor["drivers"]:
					del driver["constructor"]
		newdb.races.insert(race)
	print
	
def refactorSeasons(db,newdb):
	print("## REFACTORING SEASONS ##")
	cursor = db.seasons.find()
	raceCursor = newdb.races.find()
	for season in cursor:
		year = season["year"]
		print year
		races = []
		for race in raceCursor:
			if (race["season"]["year"] == year):
				races.append(race)
		sortedRaces = sorted(races, key=lambda race: race["round"])
		season["races"] = sortedRaces
		newdb.seasons.insert(season)
		
########################################################################################
#	PYTHON CONFIGURATION
########################################################################################
		
if __name__ == "__main__":
	main()