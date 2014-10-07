########################################################################################
#	IMPORTS
########################################################################################

import MySQLdb
import pymongo

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
	
	# Connect to MongoDB
	mongoClient = connectToMongo()
	mongoDB = createNewMongoClient(mongoClient)

	# Connect to SQL Database
	#sqlCursor = connectToSQL()
	
	# Print all SQL Tables
	#[sqlTableRowDict,sqlTableColDict] = printAllSQLTableData(sqlCursor)

########################################################################################
#	SQL FUNCTIONS
########################################################################################

def connectToSQL():
	print("## CONNECTING TO SQL DATABASE ##\n")
	db = MySQLdb.connect(host=sqlHost, user=sqlUser, passwd=sqlPassword, db=sqlDB)
	cur = db.cursor()
	return cur
	
def printAllSQLTableData(sqlCursor):
	sqlTables = allSQLTableNames
	sqlTableRowDict = {}
	sqlTableColDict = {}
	for sqlTable in sqlTables:
		colCount = getSQLTableData(sqlCursor,sqlTable)
		rowCount = printSQLTableData(sqlCursor,sqlTable,0,colCount,False)
		sqlTableRowDict[sqlTable] = rowCount
		sqlTableColDict[sqlTable] = colCount
	return sqlTableRowDict,sqlTableColDict
	
def getSQLTableData(sqlCursor, tableName):
	print("## RETRIEVING FROM SQL TABLE : %s ##\n"%tableName)
	count = 0
	sqlCursor.execute("SELECT count(*) FROM information_schema.columns WHERE table_name = '" + tableName + "'")
	for row in sqlCursor.fetchall():
		count = row[0]
	sqlCursor.execute("SELECT * from " + tableName)
	return count
	
def printSQLTableData(sqlCursor, tableName, numRows, numColumns, toPrint):
	if (toPrint):
		message = "## PRINTING"
	else:
		message = "## STORING"
	print(message + " SQL TABLE %s WITH %d COLUMNS ##\n"%(tableName,numColumns))
	rowCount = 0
	allRows = False
	if (numRows == 0):
		allRows = True
	for row in sqlCursor.fetchall():
		rowCount = rowCount + 1
		for col in range(0,numColumns):
			if (toPrint): 
				print(row[col]),
		if (toPrint):
			print
		if ((not allRows) and (rowCount > numRows)):
			break
			
########################################################################################
#	MONGODB FUNCTIONS
########################################################################################

def connectToMongo():
	print("## CONNECTING TO MONGO DATABASE ##\n")
	client = pymongo.MongoClient(mongoHost, mongoPort)
	
def createNewMongoDB(client):
	client.drop_database('f1_db')
	db = client.f1_db
	return db
	
			
########################################################################################
#	PYTHON CONFIGURATION
########################################################################################
		
if __name__ == "__main__":
	main()