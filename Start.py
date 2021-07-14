




_server = '195.57.30.196'
#_server = 'vidreo.ddns.net'
_database = 'portal_contratacion'
_username = 'portal_contrataciondba'
_password = 'Tm@s8+En-z..}'
_port='20043'

engine_string = 'mysql+mysqlconnector://'+_username+':'+_password+'@'+_server+':'+_port+'/'+_database
cnxn_string = {
  'user': _username,
  'password': _password,
  'host': _server,
  'database': _database,
  'port':_port,
  'raise_on_warnings': True
}

import PCSP_plataformacontratacionsectorpublico

