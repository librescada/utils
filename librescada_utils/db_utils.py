import pymongo
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import logging
import datetime
import pandas as pd

from librescada.web_interface.layout_utils import generate_alert

class database():
        
    def __init__(self, connection_string, database_name, collection_name, create_if_not_exist=False):
        self.isConnected = False
        self.dBconnectionString = connection_string
        self.db_name = database_name
        self.collection_name = collection_name
        
        self.logger = logging.getLogger(__name__)
        
        self.connect(create_if_not_exist)
    
    def set_cache(self, cache):
        self.cache = cache
    
    def connect(self, create_if_not_exist=False):
        try: 
                self.db_client = MongoClient(self.dBconnectionString, serverSelectionTimeoutMS=1000, tz_aware=True)
                self.db_client.server_info()
                self.isConnected = True
                 
        except Exception as e:
            self.logger.error(f'Could not connect to database: {e}')
            self.logger.error(f'Data is not being stored in the database, panic')
            # Generate error message in the alert and change color, include exception
            return generate_alert(f'Could not connect to database: {e}', 'danger')
        
        else:
            # Connect to database or create it if "create_if_not_exist" is True
            try:
                if self.db_name not in self.db_client.list_database_names(): raise Exception
            except Exception:
                if not create_if_not_exist:
                    err_message = f'Database {self.db_name} does not exist, available options are: {self.db_client.list_database_names()}'
                    self.logger.error(err_message)
                    self.db_client.close()
                    # Generate error message in the alert and change color, include exception
                    return generate_alert(err_message, 'danger')
                else:
                    err_message = f'Database {self.db_name} does not exist, creating it'
                    self.db = self.db_client[self.db_name]
                    
                    self.logger.warning(err_message)

            else:
                self.db = self.db_client.get_database(self.db_name)

            # Connect to or create collection
            try:
                if self.collection_name not in self.db.list_collection_names(): raise Exception
            except Exception:
                if self.collection_name == 'operation_data':
                    self.col = self.db.create_collection(self.collection_name, timeseries={'timeField':'time'})
                    self.col.create_index("time", unique=True)
                    log_msg = f"Collection {self.collection_name} created in database {self.db_name} of type timeseries, with unique fields: 'time'" 
                elif self.collection_name == 'operation_points':
                    self.col = self.db.create_collection(self.collection_name)
                    self.col.create_index("start_time", unique=True)
                    self.col.create_index("end_time", unique=True)
                    log_msg = f"Collection {self.collection_name} created in database {self.db_name} with unique fields: 'start_time','end_time'" 
                else:
                    self.col = self.db.create_collection(self.collection_name, timeseries={'timeField':'time'})
                    log_msg = f"Collection {self.collection_name} created in database {self.db_name}" 
                    
                self.logger.info(log_msg)
                
            else:
                self.col = self.db.get_collection(self.collection_name)
            
    def check_connection(self, return_type='alert'):
        if self.isConnected == False:
            return self.connect()
        
        else:
            try:
                self.db_client.server_info()
                self.isConnected = True
                # Generate info alert with last time the db received a new entry
                
                lastDate = self.get_newest_datetime()[0]
                if lastDate:
                    timeDif = datetime.datetime.now(tz=datetime.timezone.utc) - lastDate
                    timeDif_secs = timeDif.total_seconds()
                    
                    if timeDif_secs/86400 > 1:
                        message = f"INFO: Last database update took place {timeDif_secs/86400:.2f} days ago"
                    elif timeDif_secs/3600 > 1:
                        message = f"INFO: Last database update took place {timeDif_secs/3600:.2f} hours ago"
                    elif timeDif_secs/60 > 1:
                        message = f"INFO: Last database update took place {timeDif_secs/60:.2f} minutes ago"
                    else:
                        message = f"INFO: Last database update took place {timeDif_secs:.2f} seconds ago"
                        
                    if return_type == 'alert':
                        return generate_alert(message, 'info')
                    else: return message
                else:
                    if return_type == 'alert':
                        return generate_alert('No data in database', 'warning')
                    else: return 'No data in database'
                    
            except Exception:
                return self.connect()
    
    def get_oldest_datetime(self):
        data = self.col.find({}, {'time':1, '_id':0}).sort('time', pymongo.ASCENDING).limit(1)
        # data = data[0]['time'].replace(tzinfo=pytz.UTC)
        return [d['time'] for d in data]
    
    def get_newest_datetime(self):
        data = self.col.find({}, {'time':1, '_id':0}).sort('time', pymongo.DESCENDING).limit(1)
        # data = data[0]['time'].replace(tzinfo=pytz.UTC)
        return [d['time'] for d in data]
    
    def check_for_data(self, initial_date, final_date):
        initial_datetime = datetime.datetime.combine(initial_date, datetime.time(0,0,0))
        final_datetime = datetime.datetime.combine(final_date,   datetime.time(23,59,59))
        
        data = self.col.find({'time':{'$lt':final_datetime, '$gt':initial_datetime}},{'time':1, '_id':0}).limit(1)
        
        if [d['time'] for d in data]:
            return True
        else: 
            return False
        
    def check_available_variables(self, date):
        check_datetime = datetime.datetime.combine(date, datetime.time(0,0,0))
        data = self.col.find({'time':{'$gte':check_datetime}},{'time':0, '_id':0}).sort('time', pymongo.ASCENDING).limit(1)
        
        return [list(d.keys()) for d in data][0]        
        
    def get_newest_datetime_in_date(self, date):
        check_datetime_min = datetime.datetime.combine(date, datetime.time(0,0,0))
        check_datetime_max = datetime.datetime.combine(date, datetime.time(23,59,59))

        data = self.col.find({'time':{'$gte':check_datetime_min},
                              'time':{'$lte':check_datetime_max}},{'time':1, '_id':0}).sort('time', pymongo.DESCENDING).limit(1)
        
        result = [d['time'] for d in data][0]
        print(result)
        return result
    
    def get_oldest_datetime_in_date(self, date):
        check_datetime_min = datetime.datetime.combine(date, datetime.time(0,0,0))
        check_datetime_max = datetime.datetime.combine(date, datetime.time(23,59,59))
        data = self.col.find({'time':{'$gte':check_datetime_min},
                              'time':{'$lte':check_datetime_max}},{'time':1, '_id':0}).sort('time', pymongo.ASCENDING).limit(1)
        
        result = [d['time'] for d in data][0]
        print(result)
        return result
        
    
    def get_data(self, 
                 initial_datetime:datetime.datetime, 
                 final_datetime:datetime.datetime, 
                 vars=None, serialized=False) -> pd.DataFrame:
        
        @self.cache.memoize()
        def query_and_serialize_data(date_key) -> pd.DataFrame:
            """ Function that when faced with the same input (date_key), returns cached value.
                It will only be called once and then return cached value """
                
            vars = self.check_available_variables(initial_datetime)
            vars.append('time')
            data = self.col.find({'time':{'$lt':final_datetime, '$gt':initial_datetime}}, vars).sort('time', pymongo.ASCENDING)
            data = pd.DataFrame(iter(data)) # Very slow, need to find a better way to do this
            
            # # Chapuzillas https://stackoverflow.com/questions/54825098/datetime-pandas-and-timezone-woes-attributeerror-datetime-timezone-object
            # data['time'] = pd.to_datetime(data.time.astype(str))
        
            data.set_index('time', inplace=True)
            data = data.tz_convert('UTC')
                
            data.drop('_id', axis=1, inplace=True)
            
            return data
        
        # Create a dictionary with the variables to export        
        varsToExport = {'_id':0, 'time':1}
        if vars=='all' or vars==None:
            vars = self.check_available_variables(initial_datetime)
        
        [varsToExport.update({var:1}) for var in vars]
            
        # Create a key to uniquely identify the query
        date_key = f"{initial_datetime.strftime('%Y%m%d%H%M%S')}_{final_datetime.strftime('%Y%m%d%H%M%S')}"
                
        data = query_and_serialize_data(date_key)
        
        # Filter data to only include the variables specified in varsToExport
        varsToExport.pop('_id'); varsToExport.pop('time')
        data = data[list( varsToExport.keys() )]
        # # Generated by copilot, no idea if it will work:
        # data = [{k:v for k,v in d.items() if k in varsToExport} for d in data]
                
        if serialized:
            return data.to_json(orient='table')
        else: 
            return data
        
    def get_test_days(self, initial_date:datetime.date=None, final_date:datetime.date=None):
        if not initial_date:
            intial_date = self.get_oldest_datetime()[0].date()
        if not final_date:
            final_date = self.get_newest_datetime()[0].date()
            
        # Define the aggregation pipeline
        pipeline = [
            {"$group": {
                "_id": {"year": {"$year": "$time"}, "month": {"$month": "$time"}, "day": {"$dayOfMonth": "$time"}}
            }},
            {"$project": {
                "date": {"$dateFromParts": {"year": "$_id.year", "month": "$_id.month", "day": "$_id.day"}},
                "_id": 0
            }},
            {"$group": {
                "_id": "$date"
            }},
            {"$sort": {
                "_id": 1
            }}
        ]

        # Run the aggregation pipeline
        results = list(self.col.aggregate(pipeline))
        
        unique_dates = [result['_id'] for result in results]
        
        return unique_dates
    