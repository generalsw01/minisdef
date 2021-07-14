




_server = '195.57.30.196'
#_server = 'vidreo.ddns.net'
_database = 'contratosmenores'
_username = 'pcsp_user'
_password = 'Fualsa123!'
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
PCSP_plataformacontratacionsectorpublico.start_plataformacontratacionsectorpublico(cnxn_string,engine_string)

import PCSP_contratosmenores
PCSP_contratosmenores.start_contratosmenores(cnxn_string,engine_string)