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
	print("## CONNECTING TO MONGO DATABASE %s ON PORT %d ##\n"%(mongoHost,mongoPort))
	client = pymongo.MongoClient(mongoHost, mongoPort)
	client.drop_database('f1_db_new')
	newdb = client.f1_db_new
	db = client.f1_db
	
	refactorRaces(db,newdb)
	refactorSeasons(db,newdb)
	
	print("## CLEANING DBS ##")
	newdb.drop_collection('races')
	db.connection.drop_database('f1_db')
	client.copy_database('f1_db_new', 'f1_db')
	assert set(client['f1_db_new'].collection_names()) == set(client['f1_db'].collection_names())
	for collection in client['f1_db_new'].collection_names():
		assert client['f1_db_new'][collection].count() == client['f1_db'][collection].count()
	client.drop_database('f1_db_new')
	client.close()

########################################################################################
#	MONGODB FUNCTIONS
########################################################################################
	
def refactorRaces(db,newdb):
	print("## REFACTORING CONSTRUCTORS ##")
	cursor = db.races.find()
	for race in cursor:
		constructors = race["constructors"]
		drivers = race["drivers"]
		for driver in drivers:
			if("constructor" in driver.keys()):
				cId = driver["constructor"]["constructorId"]
				found = False
				for constructor in constructors:
					if(constructor["constructorId"] == cId):
						found = True
						if("drivers" in constructor.keys()):
							cdrivers = constructor["drivers"]
						else:
							cdrivers = []
						cdrivers.append(driver)
						constructor["drivers"] = cdrivers
						break
				if (not found):
					c = driver["constructor"]
					cdrivers = [driver]
					constructor["drivers"] = cdrivers
					constructors.append(c)
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
	for season in cursor:
		raceCursor = newdb.races.find()
		year = season["year"]
		races = []
		for race in raceCursor:
			if (race["season"]["year"] == year):
				races.append(race)
		print("Year %d Races %d"%(year,len(races)))
		if (len(races) == 0):
			break
		sortedRaces = sorted(races, key=lambda race: race["round"])
		season["races"] = sortedRaces
		newdb.seasons.insert(season)
	cursor = newdb.seasons.find()
	for season in cursor:
		races = season["races"]
		for race in races:
			del race["season"]
		
########################################################################################
#	PYTHON CONFIGURATION
########################################################################################
		
if __name__ == "__main__":
	main()