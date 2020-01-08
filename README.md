# cric bot

### Description

- Uses BeautifulSoap python package and scraps all the **cricket statistics** 
- Data sync to mongo DB
- Includes active player stats for below,

		- ipl
		- t20
		- odi
		- tests
		- bio 
	
#### version v0.0.1:  
	- Getting bio stats for player
	- Getting the active player across the globe

#### version v1.0.0:  
	 - Integrated nosql 
	 - data scraping logic integrated for player and series played by player
	 - data stats includes player bio, t20 stats, international one day stats, IPL stats, tests match stats
	 
####  version v1.0.1:  
	 - Implemented validation to handle different status code
	 - Implemented CLOUD BASED Mongo DB server
	 - Sync with cloud DB

##### cricbot.py:
	- supports only nosql
	- data stored as key value pair
	- please MAKE SURE to update config.ini file
	- gametheory.log will be auto generated at root dir

##### Note: In order to push the data to local mongo DB, udpate instance=local in the config.ini file, supported default the host  for mongo DB is 127.0.0.1 and port is 27017.

##### Make sure to run the DB either in atlas or local.
