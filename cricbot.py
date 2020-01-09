"""
Module to web scrap the cricket stats content.

author : vivekganesan01@gmail.com


version v0.0.1:
    - 2020/01/03
    - Getting bio stats for player
    - Getting the active player across the globe

version v1.0.0:
    - 2020/01/05
    - Integrated nosql
    - data scraping logic integrated for player and series played by player
    - data stats includes player bio, t20 stats, international one day stats, IPL stats, tests match stats

version v1.0.1:
    - 2020/01/07
    - Implemented validation to handle different status code
    - Implemented CLOUD BASED Mongo DB server
    - Sync with cloud DB


version v2.0.0:
    - 2020/01/09
    - Included docker file
    - Updated mongo DB data structure
    - Updated DB collections

Note: Make sure nosql DB is configured, up and running

Todo: v2 multiprocessing and multithreading
Todo: v2 update sql query from update to replace
Todo: v2 update default values or null to DB fields if blank
"""
import logging
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import re
from datetime import datetime
import time
import configparser


class GameTheory:
    """ class to control player stats web scraping"""

    def __init__(self):
        # logging
        logging.basicConfig(level=logging.INFO, filename='gametheory.log', filemode='w',
                            format='%(name)s - %(levelname)s - %(message)s')
        logging.info('**** Migration *****')
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        # url'slogging.info
        self.base_url = "http://howstat.com/cricket/Statistics/Players/"
        self.overview = "PlayerOverviewSummary.asp?PlayerID="
        self.test_series = "PlayerOverview.asp?PlayerID="
        self.odi = "PlayerOverview_ODI.asp?PlayerID="
        self.t20 = "PlayerOverview_T20.asp?PlayerID="
        self.ipl = "PlayerOverview.asp?PlayerID="
        # series to be considered web scraping
        self.SERIES = {
            'tests': 0,
            'one_day_internationals': 0,
            'twenty20_internationals': 0,
            'indian_premier_league': 0,
            'datetime': 'null',
        }
        # db dependencies
        self.db_client = None
        self.db_name = self.config['db']['dbname']
        self.test_collection = 'test_match'
        self.ipl_collection = 'ipl'
        self.odi_collection = 'odi'
        self.t20_collection = 't20'
        self.player_bio_collection = 'player_bio'
        self.player_id_collection = 'player_id'
        self.DB_UPDATE_ONE = 'update_one'
        self.DB_UPDATE_MANY = 'update_many'

    def get_current_active_player_id(self):
        """
        Fetches the active player id from howstats.
        Note: id's are used to fetch player stats.

        :return: (type list) list of active player id and name
        """
        logging.info('getting the current active player id\'s')
        current_active_player_html = "{}{}".format(self.base_url, "PlayerListCurrent.asp")
        current_active_player_info = requests.get(current_active_player_html)
        parser = BeautifulSoup(current_active_player_info.content, 'html.parser')
        active_player = parser.select('.TableLined tr')
        player_id_array = []  # stores only the player id
        player_id = []  # stores the player id's and respective name
        counter = 0  # count the active player
        logging.info('getting unique player')
        for unique_player in active_player:
            unique_record = {}  # temp
            player_info = unique_player.select_one('.LinkNormal')
            if player_info is not None:
                unique_record[str(player_info['href']).strip().split("=")[1]] = player_info.text.strip().lower()
                player_id.append(unique_record)
                player_id_array.append(str(player_info['href']).strip().split("=")[1])
                counter += 1
                logging.info('{}. adding player record - {}'.format(counter, unique_record))
        # inserting into DB
        logging.info('updating the data set')
        dataset = {'_id': 2020, 'param': player_id, 'player_id': player_id_array}  # prepare for the data set.
        self.db_sync(dataset=dataset, db_name=self.db_name, db_collection=self.player_id_collection,
                     db_operation=self.DB_UPDATE_ONE)
        logging.info('done: Total player count: {}'.format(counter))
        return player_id

    def get_player_bio(self, player_id):
        """
        Fetches the player bio stats.

        :param player_id: unique player ID
        :return:  (type dict) dictionary contains player bio status
        """
        logging.info('updating player profile : {}'.format(player_id))
        player_profile_summary_url = "{}{}{}".format(self.base_url, self.overview, player_id)
        bio_page = requests.get(player_profile_summary_url)
        if int(bio_page.status_code) != 200:
            logging.info('**********************************************************************')
            logging.info('ERROR: NOTE: URL returned error code - {}'.format(bio_page.status_code))
            logging.info(player_profile_summary_url)
            logging.info('**********************************************************************')
            return None
        parser = BeautifulSoup(bio_page.content, 'html.parser')
        player_profile = parser.select('table table table')
        bio_profile = self.SERIES.copy()  # coping global object
        bio_profile['_id'] = player_id  # variable acts as a primary key in DB
        for unique_items in player_profile:
            bio_profile_name = re.sub(r'[^\x00-\x7F]+', ' ',
                                      str(unique_items.select_one('.TextGreenBold12').text).strip())
            bio_profile['name'] = bio_profile_name.replace('-', ' ').lower()
            bio_profile['country'] = (bio_profile_name.strip().split('(')[1]).split(')')[0].replace(' ', '').lower()
            for each_bio in unique_items.select('.FieldName'):
                bio_profile[each_bio.text.strip().replace(':', '').replace(' ', '_').lower()] = 'null' \
                    if each_bio.find_next_sibling('td').text.strip() == '' \
                    else each_bio.find_next_sibling('td').text.strip()
            break
        # Getting series information
        logging.info('updating player series history')
        series_played = player_profile[1].select('.TextBlackBold10')
        series_match = player_profile[1].select('.TextBlack10')
        for each_series in range(0, len(series_played)):
            series = series_played[each_series].text.strip().replace(' ', '_').lower()
            temp_match_count = " ".join(
                re.sub(r'[^\x00-\x7F]+', ' ', str(series_match[each_series].text).strip()).split())
            match_count = int(re.findall(r'\d+', temp_match_count)[0])
            logging.info('&&&&&&&&&&&&&&&&&&&&&&')
            logging.info('{}/{}'.format(series, temp_match_count))
            if 'tests' in series:
                bio_profile['tests'] = match_count
            elif 'one_day_internationals' in series:
                bio_profile['one_day_internationals'] = match_count
            elif 'twenty20_internationals' in series:
                bio_profile['twenty20_internationals'] = match_count
            elif 'indian_premier_league' in series:
                bio_profile['indian_premier_league'] = match_count
        bio_profile['datetime'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        logging.info('done: updating player stats')
        return bio_profile

    @staticmethod  # Helper function
    def _field_extract(url):
        """
        Helps to web scrap the player stats fields from the passed url.

        :param url: player stats html
        :return:  (type dict) series stats
        """
        logging.info('extracting player stats from url: {}'.format(url))
        player_summary = requests.get(url)
        parser = BeautifulSoup(player_summary.content, 'html.parser')
        player_profile = parser.select('tr')
        list_of_fields = ['Innings', 'Not Outs', 'Aggregate', 'Highest Score', '50s', '100s', 'Ducks', '4s',
                          '6s', 'Scoring Rate', 'Overs', 'Runs Conceded', 'Wickets', 'Average', '4 Wickets in Innings',
                          '5 Wickets in Innings', 'Best', 'Economy Rate', 'Strike Rate', 'Catches',
                          'Most Catches in Innings', 'Stumpings', 'Most Catches in Innings',
                          'Most Dismissals in Innings',
                          'Won/Lost', 'Matches/Won/Lost', 'Tosses Won', 'Runs Scored', 'Batting Average']
        mapped_fields = {}  # holds series level stats
        stats_header = ''  # holds series stats metric header
        for each_field in range(0, len(player_profile)):
            # get stats header
            try:
                stats = player_profile[each_field].select_one('.ProfileSection').text.strip()
                if stats in ['Batting', 'Fielding', 'Bowling', 'Wicket Keeping', 'Captaincy']:
                    stats_header = stats
            except Exception as e:
                str(e)  # just ignore the exception
            # update stats data
            try:
                field = player_profile[each_field].select_one('.FieldName').text.split(':')[0]
                value = player_profile[each_field].select_one('.FieldValue').text.strip()
                if field in list_of_fields:
                    mapped_fields['{}_{}'.format(stats_header.lower(), field.replace(' ', '_').lower())] = value
            except AttributeError as ae:
                logging.info('skip: May be html tree doesn\'t find search - {}'.format(ae))
        logging.info('extract completed for url: {} ..... /200'.format(url))
        return mapped_fields

    def diverge(self, player_bio):  # Helper function
        """
        Helps diverging the web scraping based on the player series records
        Based on the series in the player bio, respective function will be triggered.

        :param player_bio: player stats
        :return: None
        """
        logging.info('collecting status data for player: {}'.format(player_bio['_id']))
        if player_bio['tests'] != 0:
            self._update_tests_collection(player_bio['_id'])
        if player_bio['one_day_internationals'] != 0:
            self._update_odi_collection(player_bio['_id'])
        if player_bio['twenty20_internationals'] != 0:
            self._update_t20_collection(player_bio['_id'])
        if player_bio['indian_premier_league'] != 0:
            self._update_ipl_collection(player_bio['_id'])
        self._update_bio_collection(player_bio)

    def _update_tests_collection(self, player_id):
        """
        Extracts test serious stats and updates the DB.

        :param player_id: player unique id
        :return: None
        """
        logging.info('updating tests stats for {}'.format(player_id))
        url = '{}{}{}'.format(self.base_url, self.test_series, player_id)
        extracted_fields = self._field_extract(url)
        extracted_fields['_id'] = player_id
        extracted_fields['serious'] = 'tests'
        self.db_sync(dataset=extracted_fields, db_name=self.db_name, db_collection=self.test_collection,
                     db_operation=self.DB_UPDATE_ONE)

    def _update_odi_collection(self, player_id):
        """
        Extracts test serious stats and updates the DB.

        :param player_id: player unique id
        :return: None
        """
        logging.info('updating odi stats for {}'.format(player_id))
        url = '{}{}{}'.format(self.base_url, self.odi, player_id)
        extracted_fields = self._field_extract(url)
        extracted_fields['_id'] = player_id
        extracted_fields['serious'] = 'odi'
        self.db_sync(dataset=extracted_fields, db_name=self.db_name, db_collection=self.odi_collection,
                     db_operation=self.DB_UPDATE_ONE)

    def _update_t20_collection(self, player_id):
        """
        Extracts test serious stats and updates the DB.

        :param player_id: player unique id
        :return: None
        """
        logging.info('updating t20 stats for {}'.format(player_id))
        url = '{}{}{}'.format(self.base_url, self.t20, player_id)
        extracted_fields = self._field_extract(url)
        extracted_fields['_id'] = player_id
        extracted_fields['serious'] = 't20'
        self.db_sync(dataset=extracted_fields, db_name=self.db_name, db_collection=self.t20_collection,
                     db_operation=self.DB_UPDATE_ONE)

    def _update_ipl_collection(self, player_id):
        """
        Extracts test serious stats and updates the DB.

        :param player_id: player unique id
        :return: None
        """
        logging.info('updating ipl stats for {}'.format(player_id))
        url = '{}{}{}'.format(self.base_url, self.ipl, player_id)
        extracted_fields = self._field_extract(url)
        extracted_fields['_id'] = player_id
        extracted_fields['serious'] = 'ipl'
        self.db_sync(dataset=extracted_fields, db_name=self.db_name, db_collection=self.ipl_collection,
                     db_operation=self.DB_UPDATE_ONE)

    def _update_bio_collection(self, player_bio_profile):
        """
        Extracts player bio stats and updates the DB.

        :param player_bio_profile: player bio stats dictionary
        :return: None
        """
        self.db_sync(dataset=player_bio_profile, db_name=self.db_name, db_collection=self.player_bio_collection,
                     db_operation=self.DB_UPDATE_ONE)

    def connect_to_mongo(self, host='127.0.0.1', port=27017, instance='local'):
        """
        Helps to connect to Mongo DB, default to host as local host and port to mongo default 17012.

        :param host: host IP
        :param port: mongo port
        :param instance: DB env instance
        :return: None
        """
        if instance == 'prod':
            logging.info('connecting to mongo Atlas')
            self.db_client = MongoClient('mongodb+srv://{}:{}@{}/'
                                         '{}?retryWrites=true&w=majority'.format(self.config['db']['username'],
                                                                                 self.config['db']['password'],
                                                                                 self.config['db']['atlas'],
                                                                                 self.db_name))
        else:
            logging.info('connecting to local Atlas')
            self.db_client = MongoClient(host, port)

    def db_sync(self, db_name, db_collection, db_operation, dataset):
        """
        Execute mongo DB queries.

        :param db_name: name of the DB
        :param db_collection: db collection name
        :param db_operation: db operation
        :param dataset: record to be inserted or updated
        :return: None
        """
        if type(dataset) is not dict:
            logging.info('Only dictionary data will be written to the DB')
        else:
            _db = self.db_client[db_name]
            _collection = _db[db_collection]
            try:
                if db_operation == 'update_one':
                    _collection.update_one({"_id": dataset['_id']}, {"$set": dataset}, upsert=True)
                    logging.info('`{}` document id successfully updated'.format(dataset['_id']))
                elif db_operation == ' update_many':
                    result = _collection.update_many(dataset, upsert=True)
                    logging.info('`{}` document successfully updated'.format(result.modified_count))
            except Exception as error:
                logging.info(error)

    def run(self):
        """main executor/controller"""
        # initial setup
        self.connect_to_mongo(instance=self.config['db']['instance'])
        logging.info("Connection made")
        # get id's
        player_id_list = self.get_current_active_player_id()
        # update stats
        counter = 1
        for each_id in player_id_list:
            for k, _ in each_id.items():
                logging.info(
                    "** {}/{} *************************************************".format(counter, len(player_id_list)))
                updated_bio = self.get_player_bio(k)
                if updated_bio is not None:
                    self.diverge(updated_bio)
                    logging.info("** {}/{} *********************************************/200".format(counter, len(
                        player_id_list)))
                    counter += 1
        logging.info('** end')


if __name__ == '__main__':
    start = time.time()
    gm = GameTheory()
    gm.run()
    end = time.time()
    logging.info("execution time : {} s".format(round(float(end - start), 2)))
    logging.info("----------------------------------------------------------")
