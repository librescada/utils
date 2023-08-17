import copy
import logging
import os
import time
import argparse
# import requests
import aiohttp
import asyncio


class api_logging_handler(logging.Handler):
    """Custom log handler to send log messages and alerts to API

    Args:
        logging (_type_): _description_
    """

    def __init__(self, api_url):
        # Initialize logger
        self.session = aiohttp.ClientSession()
        logging.Handler.__init__(self)

        # Set API url
        self.api_url = api_url

    async def send_request(self, alert_data):
        # Use async version of requests to send the HTTP request
        try:
            async with self.session.post(self.api_url, json=alert_data) as resp:
                pass
        except Exception as e:
            # Log any exceptions as warnings in the stream handler
            logging.warning(f"Failed to send log message to API: {e}")

    def emit(self, log_record):
        # Every time the logger is called it will format the message and send it to the API

        alert_data = {
            "level": log_record.levelname,
            "title": "hola",
            "message": log_record.msg,
            "source": log_record.name,
        }

        # Use asyncio to run the send_request method asynchronously
        asyncio.create_task(self.send_request(alert_data))

    def close(self):
        # Close the session when the handler is closed
        self.session.close()
        super().close()

class logger_librescada(logging.Logger):
    TELEGRAM_BOT = logging.INFO + 5
    
    def __init__(self, name, api_url=None):
        super(logger_librescada, self).__init__(name)
        
        # # Define logging configuration parameters in a dictionary
        # config = {
        #     "level": logging.INFO,
        #     "format": "%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
        #     "datefmt": "%d-%b-%y %H:%M:%S",
        #     "handlers": []
        # }
        
        # # Add API logging handler if API URL is provided
        # if api_url is not None:
        #     api_handler = api_logging_handler(api_url)
        #     api_handler.setLevel(self.TELEGRAM_BOT)
        #     config["handlers"].append(api_handler)
        
        # # Add stream handler
        # stream_handler = logging.StreamHandler()
        # stream_handler.setLevel(logging.INFO)
        # stream_formatter = logging.Formatter(config["format"], datefmt=config["datefmt"])
        # stream_handler.setFormatter(stream_formatter)
        # config["handlers"].append(stream_handler)
        
        # # Configure the logging system
        # logging.basicConfig(**config)
        
        # # Use a custom formatter that includes the logger name in the log message
        # formatter = logging.Formatter(config["format"], datefmt=config["datefmt"])
        # for handler in self.handlers:
        #     handler.setFormatter(formatter)
        
        # # Set the log level for all loggers under the "asyncua" and "asyncio" package prefixes
        # logging.getLogger("asyncua").setLevel(logging.WARNING)
        # logging.getLogger("asyncio").setLevel(logging.ERROR)
    
    def telegram_bot(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'TELEGRAM_BOT'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.telegram_bot("Houston, we have a %s", "major problem", exc_info=1)
        """
        if self.isEnabledFor(self.TELEGRAM_BOT):
            self._log(self.TELEGRAM_BOT, msg, args, **kwargs)
            
def get_logger_librescada(name=None, custom_logger=True, api_url=None):
    
    if not custom_logger:
        return logging.getLogger(name)
    
    logging_class = logging.getLoggerClass()  # store the current logger factory for later
    logging._acquireLock()  # use the global logging lock for thread safety
    try:
        logging.setLoggerClass(logger_librescada)  # temporarily change the logger factory
        logger = logging.getLogger(name)
        logging.setLoggerClass(logging_class)  # be nice, revert the logger factory change
        return logger
    finally:
        logging._releaseLock()
        
    formatter = logging.Formatter("%(asctime)s - [%(levelname)s] - %(name)s - %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    
    if api_url:
        telegram_handler = api_logging_handler(api_url)
        telegram_handler.setLevel(logger_librescada.TELEGRAM_BOT)

    logger = getLogger(__name__)
    logger.addHandler(stream_handler)
    
    if api_url:
        logger.addHandler(telegram_handler)

    # Set the log level for all loggers under the "asyncua" and "asyncio" package prefixes
    logging.getLogger("asyncua").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.ERROR)
    
    return logger
    
    
def argparser_librescada():
    """Utility function to parse common command line arguments used in librescada:
        - Configuration file to use by the module
        - Whether to connect to a secure server (using certificates and encryption)
        - Whether to use localhost, IP address or docker container name in configuration file
          for connecting to the opc server

    Returns:
        parser: Initialized argument parser
    """
    parser = argparse.ArgumentParser()
        
    # Opcional. Nombre del archivo de configuración a usar
    parser.add_argument("-f","--conf_file", help="Configuration filename and path",
                        required=False, type=str, default='configuration_files/simulated_sytem.hjson', )
    parser.add_argument('--secure', action='store_true', help="Secure server (using certificates and encryption)")
    parser.add_argument('--no-secure', dest='secure', action='store_false', help="Secure server (using certificates and encryption)")
    parser.set_defaults(secure=False)

    parser.add_argument('--local', action='store_true', help="Use localhost instead of IP address in configuration file")
    parser.add_argument('--no-local', dest='local', action='store_false', help="Use localhost instead of IP address in configuration file")
    parser.set_defaults(local=True)

    parser.add_argument('--docker', action='store_true', help="Use container name instead of IP address or localhost in configuration file")
    parser.add_argument('--no-docker', dest='local', action='store_false', help="Use container name instead of IP address or localhost in configuration file")
    parser.set_defaults(docker=False)
    
    return parser

def show_welcome_message(delay=1, clear_screen=False):
    try:
        
        f = open(os.path.join(get_assets_dir(), 'assets/ascii-art.ans'), 'r')
        logo = ''.join(f.readlines())
        if clear_screen:
            os.system('cls' if os.name == 'nt' else 'clear')
            
        print(logo)
        print(""" Cliente automático. Para terminar el programa primero usar CTRL + D para salir de la consola y posteriormente
        CTRL + C para finalizar la ejecución del cliente. Desarrollado por Juan Miguel Serrano Rodríguez
                                    Donasiones a https://www.paypal.me/juasmis""")
        time.sleep(delay)
    except FileNotFoundError:
        # Si falla por lo que sea simplemente no mostrar logo
        pass
    

def get_assets_dir():
    """Get assets directory path from environment variable ASSETS_DIR

    Returns:
        str: Path to assets directory
        
    Raises:
        ValueError: If ASSETS_DIR environment variable is not set
    """
    
    ASSETS_DIR = os.getenv("ASSETS_DIR", None)
    
    if not ASSETS_DIR:
        raise ValueError("ASSETS_DIR environment variable not set, set it with 'export ASSETS_DIR=<path>' to where the configuration files are located (e.g. 'export ASSETS_DIR=/assets/').")
    
    else:
        ASSETS_DIR = os.path.abspath(ASSETS_DIR)
        return ASSETS_DIR

# class api_logging_handler(logging.Handler):
#     """Custom log handler to send log messages and alerts to API

#     Args:
#         logging (_type_): _description_
#     """
    
#     def __init__(self, api_url):
#         # Initialize logger
#         import requests
#         self.requests = requests
#         logging.Handler.__init__(self)
        
#         # Set format
#         # self.setFormatter("%(asctime)s - [%(levelname)s] - %(message)s")
        
#         # Set API url
#         self.api_url = api_url
        
#     def emit(self, log_record):
#         # Every time the logger is called it will format the message and send it to the API
                
#         alert_data = {
#             "level": log_record.levelname,
#             "title": "hola",
#             "message": log_record.msg,
#             "source": log_record.name,
#         }
    
#         self.requests.post(self.api_url, json=alert_data)


def generate_groups(config, type='measurements'):
    """Generate groups of different structure depending on the type
        argument
    """
    
    if type == 'measurements':
        
        """
            Returns a list of dictionaries grouped by the group key from the measurements
            Group structure:
                {
                    name: group_name,
                    sensorId_list: [sensor_id1, sensor_id2, ...],
                    measurements: {var_id:{sensor_id, var_id, description, unit, unit_SCADA, unit_model, group}
                    varId_list: [var_id1, var_id2, ...]
                }
            
        """
        
        # Create groups
        groupValues = [config["measurements"][var]["group"] for var in config["measurements"].keys()]
        uniqueGroupValues = list(set(groupValues))
        uniqueGroupValues.sort()

        groups = [{"name": grpName, "sensorId_list": None, "measurements": dict(), "varId_list":None} for grpName in uniqueGroupValues]
                
        for var in config["measurements"].keys():
            grpIdx  = uniqueGroupValues.index(config["measurements"][var]["group"])
            groups[grpIdx]["measurements"][var] = config["measurements"][var]
            # print(group["measurements"].keys())

        # Create sensor_id and var_id list for each group
        for grpIdx in range(len(groups)):
            groups[grpIdx]["sensorId_list"] = [ groups[grpIdx]["measurements"][var]["sensor_id"] for var in groups[grpIdx]["measurements"].keys() ]
            groups[grpIdx]["varId_list"]    = [ groups[grpIdx]["measurements"][var]["var_id"] for var in groups[grpIdx]["measurements"].keys() ]

    elif type=='inputs':
        """
            Returns a list of dictionaries grouped by the group key from the inputs
            Group structure:
                {
                    name: group_name (subsystem),
                    inputId_list: [input_id1, input_id2, ...],
                    inputs: {var_id:{input_id, var_id, description, unit, unit_SCADA, unit_model, subsystem}
                    varId_list: [var_id1, var_id2, ...]
                }
            
        """
        # Create groups
        groupValues = [config["inputs"][var]["subsystem"] for var in config["inputs"].keys()]
        uniqueGroupValues = list(set(groupValues))
        uniqueGroupValues.sort()

        groups = [{"name": grpName, "id_list":None, "inputs": dict(), "varId_list":None} for grpName in uniqueGroupValues]
                
        for var in config["inputs"].keys():
            grpIdx  = uniqueGroupValues.index(config["inputs"][var]["subsystem"])
            groups[grpIdx]["inputs"][var] = config["inputs"][var]

        # Create sensor_id and var_id list for each group
        for grpIdx in range(len(groups)):
            groups[grpIdx]["inputId_list"]  = [ groups[grpIdx]["inputs"][var]["input_id"] for var in groups[grpIdx]["inputs"].keys() ]
            groups[grpIdx]["varId_list"]    = [ groups[grpIdx]["inputs"][var]["var_id"] for var in groups[grpIdx]["inputs"].keys() ]
            
    elif type=='grouped': # Used in data_export and data_visualization
        """
            Returns a dict with both measurements and inputs using sensor or input ids as keys.
            input_id and subsystem keys are substituted by sensor_id and group keys to be consistent with measurements
        """
        
        groups = {}
        for var_id in config['measurements']:
            var = config['measurements'][var_id]
            groups[var['sensor_id']] = var
            
        config_copy = copy.deepcopy(config['inputs']) # Copy since it's going to be modified
        for var_id in config_copy:
            var = config_copy[var_id]
            id_ = var['input_id']
            groups[id_] = var
            
            # Replace input_id key for sensor_id to be consistent with measurements
            groups[id_]['sensor_id'] = groups[id_].pop('input_id')
            # Replace subsystem key for group to be consistent with measurements
            groups[id_]['group'] = groups[id_].pop('subsystem')
            
        # Not really needed, but for consistency
        groupValues = [groups[var]["group"] for var in groups]
        uniqueGroupValues = list(set(groupValues))
        uniqueGroupValues.sort()
        
    elif type=='grouped_varIds': # Same as 'grouped' but with var_ids instead of sensor_ids as keys
        """
            Returns a dict with both measurements and inputs using var ids as keys.
            input_id and subsystem keys are substituted by sensor_id and group keys to be consistent with measurements
            
        """
        
        groups = {}
        for var_id in config['measurements']:
            var = config['measurements'][var_id]
            groups[var['var_id']] = var
            
        config_copy = copy.deepcopy(config['inputs']) # Copy since it's going to be modified
        for var_id in config_copy:
            var = config_copy[var_id]
            id_ = var['var_id']
            groups[id_] = var
            
            # Replace input_id key for sensor_id to be consistent with measurements
            groups[id_]['sensor_id'] = groups[id_].pop('input_id')
            # Replace subsystem key for group to be consistent with measurements
            groups[id_]['group'] = groups[id_].pop('subsystem')
            
        # Not really needed, but for consistency
        groupValues = [groups[var]["group"] for var in groups]
        uniqueGroupValues = list(set(groupValues))
        uniqueGroupValues.sort()
    
    else: raise ValueError('Type not recognized')
    
    return groups, uniqueGroupValues

def fix_path(path):
    fixed_path = os.path.abspath(os.path.expanduser(path))
    return fixed_path

def capfirst(s):
    """Function that given a string, capitalizes the first letter: from:
        https://stackoverflow.com/questions/31767622/capitalize-the-first-letter-of-a-string-without-touching-the-others

    Args:
        s (_type_): _description_

    Returns:
        _type_: _description_
    """
    return s[:1].upper() + s[1:]

if __name__ == '__main__':
    # Api logger
    logger_api = logging.getLogger(__name__)
    api_handler = api_logging_handler("http://127.0.0.1:8000/generate_alert")
    logger_api.addHandler(api_handler)
    logger_api.setLevel(logging.INFO)
    
    logger_api.info(f'Started logging of TEST')
