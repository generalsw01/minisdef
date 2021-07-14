




_server = '10.20.50.42'
#_server = 'vidreo.ddns.net'
_database = 'portal_contratacion'
_username = 'portal_contrataciondba'
_password = 'Tm@s8+En-z..}'
_port='3306'

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

