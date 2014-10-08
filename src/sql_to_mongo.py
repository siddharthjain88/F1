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
	print("## CONNECTING TO SQL DATABASE %s ON HOST %s##\n"%(sqlDB,sqlHost))
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
	print("## CONNECTING TO MONGO DATABASE %s ON PORT %d##\n"%(mongoHost,mongoPort))
	client = pymongo.MongoClient(mongoHost, mongoPort)
	client.drop_database('f1_db')
	db = client.f1_db
	storeInMongoTableStatuses(db,"status",sqlData,sqlColumns)
	storeInMongoTableDrivers(db,"drivers",sqlData,sqlColumns)
	storeInMongoTableCircuits(db,"circuits",sqlData,sqlColumns)
	storeInMongoTableConstructors(db,"constructors",sqlData,sqlColumns)
	storeInMongoTableSeasons(db,"seasons",sqlData,sqlColumns)
	client.close()
	
def storeInMongoTableStatuses(mongoDB,tableName,sqlData,sqlColumns):
	print("## STORING %s DATA TO MONGO DATABASE ##\n"%(tableName))
	columns = sqlColumns[tableName]
	for sqlRow in sqlData[tableName]:
		mongoDB.statuses.insert({columns[1]:sqlRow[columns[1]]})
		
def storeInMongoTableDrivers(mongoDB,tableName,sqlData,sqlColumns):
	print("## STORING %s DATA TO MONGO DATABASE ##\n"%(tableName))
	columns = sqlColumns[tableName]
	for sqlRow in sqlData[tableName]:
		doc = {}
		for col in range(1,len(columns)):
			if (columns[col] == "dob"):
				dob = sqlRow[columns[col]]
				if (dob != None):
					sqlRow[columns[col]] = datetime.datetime(dob.year,dob.month,dob.day)
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
		for col in range(1,len(columns)):
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
		for col in range(1,len(columns)):
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
			
########################################################################################
#	PYTHON CONFIGURATION
########################################################################################
		
if __name__ == "__main__":
	main()