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

# SQL DATA CONFIGURATION
allSQLTableNames = ["circuits",
					"constructorResults",
					"constructorStandings",
					"constructors",
					"driverStandings",
					"drivers",
					"lapTimes",
					"pitStops",
					"qualifying",
					"races",
					"results",
					"seasons",
					"status"]
					
########################################################################################
#	MAIN
########################################################################################

def main():
	print("\n## STARTING PROGRAM ##\n")
	[sqlData,sqlColumns] = retrieveSQLData()
	storeInMongo(sqlData,sqlColumns)	

########################################################################################
#	SQL FUNCTIONS
########################################################################################
	
def retrieveSQLData():
	print("## CONNECTING TO SQL DATABASE %s ON HOST %s ##\n"%(sqlDB,sqlHost))
	db = MySQLdb.connect(host=sqlHost, user=sqlUser, passwd=sqlPassword, db=sqlDB)
	cur = db.cursor()
	sqlTables = allSQLTableNames
	sqlData = {}
	sqlColumns = {}
	for sqlTable in sqlTables:
		data, columns = getSQLTableData(cur,sqlTable, False)
		sqlData[sqlTable] = data
		sqlColumns[sqlTable] = columns
	cur.close()
	db.close()
	return sqlData,sqlColumns
	
def getSQLTableData(sqlCursor, tableName, toPrint):
	print("## RETREIVING SQL TABLE %s ##\n"%(tableName))
	sqlCursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '" + tableName + "'")
	columns = []
	for row in sqlCursor.fetchall():
		columns.append(row[0])
	sqlCursor.execute("SELECT * from " + tableName)
	data = []
	for row in sqlCursor.fetchall():
		rowData = {}
		for col in range(0,len(columns)):
			rowData[columns[col]] = row[col]
			if (toPrint): 
				print(row[col]),
		data.append(rowData)
		if (toPrint):
			print
	return data,columns
			
########################################################################################
#	MONGODB FUNCTIONS
########################################################################################

def storeInMongo(sqlData,sqlColumns):
	print("## CONNECTING TO MONGO DATABASE %s ON PORT %d ##\n"%(mongoHost,mongoPort))
	client = pymongo.MongoClient(mongoHost, mongoPort)
	client.drop_database('f1_db')
	db = client.f1_db
	storeInMongoTableStatuses(db,"status",sqlData,sqlColumns)
	storeInMongoTableDrivers(db,"drivers",sqlData,sqlColumns)
	storeInMongoTableCircuits(db,"circuits",sqlData,sqlColumns)
	storeInMongoTableConstructors(db,"constructors",sqlData,sqlColumns)
	storeInMongoTableSeasons(db,"seasons",sqlData,sqlColumns)
	storeInMongoTableRaces(db,"races",sqlData,sqlColumns)
	storeInMongoTableResults(db,"results",sqlData,sqlColumns)
	storeInMongoTableDriversStandings(db,"driverStandings",sqlData,sqlColumns)
	storeInMongoTableConstructorResults(db,"constructorResults",sqlData,sqlColumns)
	storeInMongoTableConstructorStandings(db,"constructorStandings",sqlData,sqlColumns)
	storeInMongoTableQualifying(db,"qualifying",sqlData,sqlColumns)
	storeInMongoTablePitStops(db,"pitStops",sqlData,sqlColumns)
	client.close()
	
def storeInMongoTableStatuses(mongoDB,tableName,sqlData,sqlColumns):
	print("## STORING %s DATA TO MONGO DATABASE ##\n"%(tableName))
	columns = sqlColumns[tableName]
	for sqlRow in sqlData[tableName]:
		mongoDB.statuses.insert({columns[0]:sqlRow[columns[0]],columns[1]:sqlRow[columns[1]]})
		
def storeInMongoTableDrivers(mongoDB,tableName,sqlData,sqlColumns):
	print("## STORING %s DATA TO MONGO DATABASE ##\n"%(tableName))
	columns = sqlColumns[tableName]
	for sqlRow in sqlData[tableName]:
		doc = {}
		for col in range(0,len(columns)):
			if (columns[col] == "dob"):
				dob = sqlRow[columns[col]]
				if (dob != None):
					sqlRow[columns[col]] = datetime.datetime(dob.year,dob.month,dob.day)
			if (columns[col] == "driverId"):
				if (sqlRow[columns[col]] != None):
					sqlRow[columns[col]] = int(sqlRow[columns[col]])
			if (isinstance(sqlRow[columns[col]],basestring)):
				str = sqlRow[columns[col]].decode('iso-8859-1').encode('utf8')
				sqlRow[columns[col]] = str
			doc[columns[col]] = sqlRow[columns[col]]
		mongoDB.drivers.insert(doc)
		
def storeInMongoTableCircuits(mongoDB,tableName,sqlData,sqlColumns):
	print("## STORING %s DATA TO MONGO DATABASE ##\n"%(tableName))
	columns = sqlColumns[tableName]
	for sqlRow in sqlData[tableName]:
		doc = {}
		for col in range(0,len(columns)):
			column = columns[col]
			if (columns[col] == "lat"):
				column = "latitude"
			if (columns[col] == "lng"):
				column = "longitude"
			if (columns[col] == "alt"):
				column = "altitude"
				if (sqlRow[columns[col]] != None):
					data = int(sqlRow[columns[col]])
					sqlRow[columns[col]] = data
			if (columns[col] == "circuitId"):
				if (sqlRow[columns[col]] != None):
					data = int(sqlRow[columns[col]])
					sqlRow[columns[col]] = data
			if (isinstance(sqlRow[columns[col]],basestring)):
				str = sqlRow[columns[col]].decode('iso-8859-1').encode('utf8')
				sqlRow[columns[col]] = str
			doc[column] = sqlRow[columns[col]]
		mongoDB.circuits.insert(doc)

def storeInMongoTableConstructors(mongoDB,tableName,sqlData,sqlColumns):
	print("## STORING %s DATA TO MONGO DATABASE ##\n"%(tableName))
	columns = sqlColumns[tableName]
	for sqlRow in sqlData[tableName]:
		doc = {}
		for col in range(0,len(columns)):
			if (columns[col] == "constructorId"):
				if (sqlRow[columns[col]] != None):
					sqlRow[columns[col]] = int(sqlRow[columns[col]])
			if (isinstance(sqlRow[columns[col]],basestring)):
				str = sqlRow[columns[col]].decode('iso-8859-1').encode('utf8')
				sqlRow[columns[col]] = str
			doc[columns[col]] = sqlRow[columns[col]]
		mongoDB.constructors.insert(doc)

def storeInMongoTableSeasons(mongoDB,tableName,sqlData,sqlColumns):
	print("## STORING %s DATA TO MONGO DATABASE ##\n"%(tableName))
	columns = sqlColumns[tableName]
	for sqlRow in sqlData[tableName]:
		doc = {}
		for col in range(0,len(columns)):
			if (columns[col] == "year"):
				data = int(sqlRow[columns[col]])
				sqlRow[columns[col]] = data
			if (isinstance(sqlRow[columns[col]],basestring)):
				str = sqlRow[columns[col]].decode('iso-8859-1').encode('utf8')
				sqlRow[columns[col]] = str
			doc[columns[col]] = sqlRow[columns[col]]
		mongoDB.seasons.insert(doc)	
		
def storeInMongoTableRaces(mongoDB,tableName,sqlData,sqlColumns):
	print("## STORING %s DATA TO MONGO DATABASE ##\n"%(tableName))
	columns = sqlColumns[tableName]
	for sqlRow in sqlData[tableName]:
		doc = {}
		for col in range(0,len(columns)):
			column = columns[col]
			data = sqlRow[columns[col]]
			if (column == "round"):
				if (data != None):
					data = int(data)
			if (column == "raceId"):
				if (data != None):
					data = int(data)
			if (columns[col] == "year"):
				column = "season"
				data = mongoDB.seasons.find_one({columns[col]: sqlRow[columns[col]]})
			if (columns[col] == "circuitId"):
				column = "circuit"
				data = mongoDB.circuits.find_one({columns[col]: sqlRow[columns[col]]})
			if (columns[col] == "date"):
				data = sqlRow[columns[col]]
				time = sqlRow["time"]
				if (data != None and time != None):
					data = datetime.datetime(data.year,data.month,data.day,time.seconds//3600,(time.seconds//60)%60,0)
				elif (data != None and time == None):
					data = datetime.datetime(data.year,data.month,data.day)
			if (isinstance(sqlRow[columns[col]],basestring)):
				data = sqlRow[columns[col]].decode('iso-8859-1').encode('utf8')
			if (column != "time"):
				doc[column] = data
			doc["drivers"] = []
			doc["constructors"] = []
		mongoDB.races.insert(doc)
		
def storeInMongoTableResults(mongoDB, tableName, sqlData, sqlColumns):
	print("## STORING %s DATA TO MONGO DATABASE ##"%(tableName))
	columns = sqlColumns[tableName]
	totalLength = len(sqlData[tableName])
	count = 0
	print("Completed %d of %d"%(count,totalLength))
	for sqlRow in sqlData[tableName]:
		count = count + 1
		if (count % 1000 == 0):
			print("Completed %d of %d"%(count,totalLength))
		race = mongoDB.races.find_one({"raceId": sqlRow[columns[1]]})
		driver = mongoDB.drivers.find_one({"driverId": sqlRow[columns[2]]})
		for col in range(0,len(columns)):
			column = columns[col]
			data = sqlRow[columns[col]]
			if (column == "number" or column == "grid" or column == "positionOrder" or column == "points" or column == "laps" or column == "fastestLap" or column == "rank"):
				if (data != None):
					data = int(data)
				if (column == "points"):
					column = "racePoints"
				if (column == "rank"):
					column = "fastestLapRank"
				if (column == "positionOrder"):
					column = "position"
				driver[column] = data
			if (column == "positionText" or column == "time" or column == "fastestLapTime"):
				if (data != None):
					data = sqlRow[columns[col]].decode('iso-8859-1').encode('utf8')
					driver[column] = data
			if (column == "fastestLapSpeed"):
				if (data != None):
					data = float(data)
					driver[column] = data
			if (column == "milliseconds"):
				if (data != None):
					data = int(data)
					driver[column] = data
			if (column == "constructorId"):
				data = mongoDB.constructors.find_one({column: data})
				column = "constructor"
				driver[column] = data
			if (column == "statusId"):
				data = mongoDB.statuses.find_one({column: data})["status"]
				column = "status"
				driver[column] = data
		driver["lapTimes"] = []
		race["drivers"].append(driver)
		mongoDB.races.update({'_id':race["_id"]}, {"$set": race}, upsert=False)
	print
	
def storeInMongoTableDriversStandings(mongoDB, tableName, sqlData, sqlColumns):
	print("## STORING %s DATA TO MONGO DATABASE ##"%(tableName))
	columns = sqlColumns[tableName]
	totalLength = len(sqlData[tableName])
	count = 0
	print("Completed %d of %d"%(count,totalLength))
	for sqlRow in sqlData[tableName]:
		count = count + 1
		if (count % 1000 == 0):
			print("Completed %d of %d"%(count,totalLength))
		driverRec = mongoDB.races.find_one({"raceId": sqlRow[columns[1]], "drivers.driverId": sqlRow[columns[2]]},{"_id" : 0, "drivers": {"$elemMatch" : {"driverId":sqlRow[columns[2]]}}})
		if (driverRec == None):
			driver = mongoDB.drivers.find_one({"driverId": sqlRow[columns[2]]})
		else:
			driver = driverRec["drivers"][0]
		mongoDB.races.update({"raceId": sqlRow[columns[1]]},{"$pull" :{"drivers":{"driverId":sqlRow[columns[2]]}}})
		for col in range(0,len(columns)):
			column = columns[col]
			data = sqlRow[columns[col]]
			if (column == "points" or column == "position" or column == "wins"):
				if (data!= None):
					driver[column] = int(data)
		mongoDB.races.update({"raceId": sqlRow[columns[1]]},{"$push" :{"drivers":driver}})
	print
	
def storeInMongoTableConstructorResults(mongoDB, tableName, sqlData, sqlColumns):
	print("## STORING %s DATA TO MONGO DATABASE ##"%(tableName))
	columns = sqlColumns[tableName]
	totalLength = len(sqlData[tableName])
	count = 0
	print("Completed %d of %d"%(count,totalLength))
	for sqlRow in sqlData[tableName]:
		count = count + 1
		if (count % 1000 == 0):
			print("Completed %d of %d"%(count,totalLength))
		race = mongoDB.races.find_one({"raceId": sqlRow[columns[1]]})
		constructor = mongoDB.constructors.find_one({"constructorId": sqlRow[columns[2]]})
		if (constructor == None):
			continue
		for col in range(0,len(columns)):
			column = columns[col]
			data = sqlRow[columns[col]]
			if (column == "points"):
				if (data != None):
					data = int(data)
				if (column == "points"):
					column = "racePoints"
				constructor[column] = data
		race["constructors"].append(constructor)
		mongoDB.races.update({'_id':race["_id"]}, {"$set": race}, upsert=False)
	print
		
def storeInMongoTableConstructorStandings(mongoDB, tableName, sqlData, sqlColumns):
	print("## STORING %s DATA TO MONGO DATABASE ##"%(tableName))
	columns = sqlColumns[tableName]
	totalLength = len(sqlData[tableName])
	count = 0
	print("Completed %d of %d"%(count,totalLength))
	for sqlRow in sqlData[tableName]:
		count = count + 1
		if (count % 1000 == 0):
			print("Completed %d of %d"%(count,totalLength))
		constructorRec = mongoDB.races.find_one({"raceId": sqlRow[columns[1]], "constructors.constructorId": sqlRow[columns[2]]},{"_id" : 0, "constructors": {"$elemMatch" : {"constructorId":sqlRow[columns[2]]}}})
		if (constructorRec == None):
			constructor = mongoDB.constructors.find_one({"constructorId": sqlRow[columns[2]]})
		else:
			constructor = constructorRec["constructors"][0]
		if (constructor == None):
			continue
		mongoDB.races.update({"raceId": sqlRow[columns[1]]},{"$pull" :{"constructors":{"constructorId":sqlRow[columns[2]]}}})
		for col in range(0,len(columns)):
			column = columns[col]
			data = sqlRow[columns[col]]
			if (column == "points" or column == "position" or column == "wins"):
				if (data!= None):
					constructor[column] = int(data)
		mongoDB.races.update({"raceId": sqlRow[columns[1]]},{"$push" :{"constructors":constructor}})
	print
	
def storeInMongoTableQualifying(mongoDB, tableName, sqlData, sqlColumns):
	print("## STORING %s DATA TO MONGO DATABASE ##"%(tableName))
	columns = sqlColumns[tableName]
	totalLength = len(sqlData[tableName])
	count = 0
	print("Completed %d of %d"%(count,totalLength))
	for sqlRow in sqlData[tableName]:
		count = count + 1
		if (count % 1000 == 0):
			print("Completed %d of %d"%(count,totalLength))
		driverRec = mongoDB.races.find_one({"raceId": sqlRow[columns[1]], "drivers.driverId": sqlRow[columns[2]]},{"_id" : 0, "drivers": {"$elemMatch" : {"driverId":sqlRow[columns[2]]}}})
		if (driverRec == None):
			driver = mongoDB.drivers.find_one({"driverId": sqlRow[columns[2]]})
		else:
			driver = driverRec["drivers"][0]
		if (driver == None):
			continue
		mongoDB.races.update({"raceId": sqlRow[columns[1]]},{"$pull" :{"drivers":{"driverId":sqlRow[columns[2]]}}})
		qualifying = {}
		for col in range(0,len(columns)):
			column = columns[col]
			data = sqlRow[columns[col]]
			if (column == "position"):
				if (data!= None):
					qualifying[column] = int(data)
			if (column == "q1" or column == "q2" or column == "q3"):
				if (data != None):
					data = sqlRow[columns[col]].decode('iso-8859-1').encode('utf8')
					qualifying[column] = data
		driver["qualifying"] = qualifying
		mongoDB.races.update({"raceId": sqlRow[columns[1]]},{"$push" :{"drivers":driver}})
	print

def storeInMongoTablePitStops(mongoDB, tableName, sqlData, sqlColumns):
	print("## STORING %s DATA TO MONGO DATABASE ##"%(tableName))
	columns = sqlColumns[tableName]
	totalLength = len(sqlData[tableName])
	count = 0
	print("Completed %d of %d"%(count,totalLength))
	for sqlRow in sqlData[tableName]:
		count = count + 1
		if (count % 1000 == 0):
			print("Completed %d of %d"%(count,totalLength))
		race = mongoDB.races.find_one({"raceId": sqlRow[columns[0]]})
		driverRec = mongoDB.races.find_one({"raceId": sqlRow[columns[0]], "drivers.driverId": sqlRow[columns[1]]},{"_id" : 0, "drivers": {"$elemMatch" : {"driverId":sqlRow[columns[1]]}}})
		if (driverRec == None):
			driver = mongoDB.drivers.find_one({"driverId": sqlRow[columns[1]]})
		else:
			driver = driverRec["drivers"][0]
		if (driver == None):
			continue
		mongoDB.races.update({"raceId": sqlRow[columns[0]]},{"$pull" :{"drivers":{"driverId":sqlRow[columns[1]]}}})
		if ("pitStops" in driver.keys()):
			pitStops = driver["pitStops"]
		else:
			pitStops = []
		pitStop = {}
		for col in range(0,len(columns)):
			column = columns[col]
			data = sqlRow[columns[col]]
			if (column == "lap" or column == "stop"):
				if (data!= None):
					pitStop[column] = int(data)
			if (column == "time"):
				if (data != None):
					data = datetime.datetime(race["date"].year,race["date"].month,race["date"].day,data.seconds//3600,(data.seconds//60)%60,data.seconds%60)
				else:
					data = datetime.datetime(data.year,data.month,data.day)
				pitStop[column] = data
			if (column == "milliseconds"):
				if (data != None):
					data = int(data)
					pitStop[column] = data
			if (column == "duration"):
				if (data != None):
					data = sqlRow[columns[col]].decode('iso-8859-1').encode('utf8')
					pitStop[column] = data
		pitStops.append(pitStop)
		driver["pitStops"] = pitStops
		mongoDB.races.update({"raceId": sqlRow[columns[0]]},{"$push" :{"drivers":driver}})
	print
		
########################################################################################
#	PYTHON CONFIGURATION
########################################################################################
		
if __name__ == "__main__":
	main()