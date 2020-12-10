'''
Created by: Filipe Camelo
Date created: 20190925

Usage:
python jdbc_test.py /full/path/to/config_file.yml

Configuration file is a yaml file with with the required variable defined on root:

driver_path: /path/to/driver.jar
driver_class: cass.driver.to.use

driver_args:
  arg_1: arg_value
  ...

conn_str: jdbc:postgres:localhost

query: 

'''

import sys
import jaydebeapi
import yaml
from pprint import pprint
import logging
import os

## LOGGING CONFIG

# Create a custom logger
logger = logging.getLogger(__name__)
LOG_LEVEL = logging.DEBUG

# Create handlers
console_handler = logging.StreamHandler()
console_handler.setLevel(LOG_LEVEL)
console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_format)

# Add handlers to the logger
logger.addHandler(console_handler)

def main():
	# get YAML config file
	with open(CONFIG_FILE, "r") as fp:
		conf = yaml.safe_load(fp)

	# set JAVA_HOME
	os.environ['JAVA_HOME'] = conf['java_home']

	# connection
	conn = jaydebeapi.connect(conf["driver_class"],conf["conn_str"],conf["driver_args"],conf["driver_path"])
	logger.info("Connection successfull")

	# Execute queries
	curs = conn.cursor()
	logger.info("Executing queries...")

	curs.execute(conf['query_string'])

	# Fetch results and print
	logger.info("Fetching results...")
	res = curs.fetchall()
	pprint(res)

	# Close cursor and connection
	logger.info("Closing connection...")
	curs.close()
	conn.close()

if __name__ == "__main__":
	# get file path from argument
	CONFIG_FILE = sys.argv[1]
	# main function
	main()