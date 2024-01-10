import configparser

parser = configparser.ConfigParser()
parser.read('/config/config.conf')

#PATH
INPUT_PATH = parser.get('file_paths', 'input_path')
OUTPUT_PATH = parser.get('file_paths', 'output_path')

#CREDENTIAL
CREDENTIAL = parser.get('api_keys','credential')