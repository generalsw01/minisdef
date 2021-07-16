#!/usr/bin/env python
# coding: utf-8

# In[1]:


import xmltodict
import requests
import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
import time
from sqlalchemy import create_engine
import sqlalchemy
from datetime import datetime
import mysql.connector
from mysql.connector import errorcode

_server = ' 10.20.50.42'
#_server = 'vidreo.ddns.net'
_database = 'portal_contratacion'
_username = 'portal_contrataciondba'
_password = 'Tmas8+En-z..}'
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

def fc_licitaciones (entry):
    
    ContractFolderSatus = entry['cac-place-ext:ContractFolderStatus']
    
    try: PliegoClausulasAdministrativasURI = ContractFolderSatus['cac:LegalDocumentReference']['cac:Attachment']['cac:ExternalReference']['cbc:URI']
    except: PliegoClausulasAdministrativasURI = ''
        
    try: PliegoClausulasAdministrativasID = ContractFolderSatus['cac:LegalDocumentReference']['cbc:ID']
    except: PliegoClausulasAdministrativasID = ''
        
    try: PliegoPrescripcionesTecnicasURI = ContractFolderSatus['cac:TechnicalDocumentReference']['cac:Attachment']['cac:ExternalReference']['cbc:URI']
    except: PliegoPrescripcionesTecnicasURI = ''
    
    try: PliegoPrescripcionesTecnicasID = ContractFolderSatus['cac:TechnicalDocumentReference']['cbc:ID']
    except: PliegoPrescripcionesTecnicasID = ''
        
    try: EstadoLicitacion =ContractFolderSatus['cbc-place-ext:ContractFolderStatusCode']['#text']
    except: EstadoLicitacion = ''

    try: IdiomaLicitacion = ContractFolderSatus['cbc-place-ext:ContractFolderStatusCode']['@languageID']
    except: IdiomaLicitacion = ''
    
    try: CodigoExpediente = ContractFolderSatus['cbc:ContractFolderID']
    except: CodigoExpediente = ''
        
    try: IDEntry = entry['id']
    except: IDEntry = ''
        
    try: LinkEntry = entry['link']['@href']
    except: LinkEntry = ''

    try: Sumario = entry['summary']['#text']
    except: Sumario = ''
    
    try: Titulo = entry['title']
    except: Titulo = ''

    try:
        UltimaActualizacion = entry['updated']
        UltimaActualizacion = datetime.strptime(UltimaActualizacion.replace('T',' ')[0:19], '%Y-%m-%d %H:%M:%S')
        if UltimaActualizacion.year<1000: 
            UltimaActualizacion = UltimaActualizacion.replace(year=a.year + 2000)
    except: UltimaActualizacion = ''
    
    
   
    
    
    dict_licitaciones = {'PliegoClausulasAdministrativasURI':PliegoClausulasAdministrativasURI,'PliegoClausulasAdministrativasID':PliegoClausulasAdministrativasID,
                                              'PliegoPrescripcionesTecnicasURI':PliegoPrescripcionesTecnicasURI,'PliegoPrescripcionesTecnicasID':PliegoPrescripcionesTecnicasID,
                                              'EstadoLicitacion':EstadoLicitacion,'IdiomaLicitacion':IdiomaLicitacion,'CodigoExpediente':CodigoExpediente,'IDEntry':IDEntry,'LinkEntry':LinkEntry,'Sumario':Sumario,'Titulo':Titulo,
                                              'UltimaActualizacion':UltimaActualizacion}
    
    Licitaciones = pd.DataFrame.from_dict(dict_licitaciones,orient='index').T
    return(Licitaciones)


# In[ ]:


def fc_DocumentosGenerales(ContractFolderStatus,IDEntry):
    
    IDEntry = IDEntry.replace('https://contrataciondelestado.es/sindicacion/','')
    try:
        df_DocumentosGenerales = pd.DataFrame()

        for DocumentoGeneral in ContractFolderStatus['cac-place-ext:GeneralDocument']:
            try: NombreFichero = DocumentoGeneral['cac-place-ext:GeneralDocumentDocumentReference']['cac:Attachment']['cac:ExternalReference']['cbc:FileName']
            except: NombreFichero = ''    
            try: URI = DocumentoGeneral['cac-place-ext:GeneralDocumentDocumentReference']['cac:Attachment']['cac:ExternalReference']['cbc:URI']
            except: URI = ''   
        
            dict_DocumentosGenerales = {'NombreFichero':NombreFichero,'URI':URI,'IDEntry':IDEntry}
            df_DocumentosGenerales = df_DocumentosGenerales.append(pd.DataFrame.from_dict(dict_DocumentosGenerales, orient='index').T)
    except:
        df_DocumentosGenerales = pd.DataFrame()
        

    return(df_DocumentosGenerales)

# In[ ]:

def fc_OrganoDeContratacion(ContractFolderSatus,Link_IDEntry,IDEntry): 
    
    IDEntry = IDEntry.replace('https://contrataciondelestado.es/sindicacion/','')
    
    LocatedContractingParty = ContractFolderSatus['cac-place-ext:LocatedContractingParty']
    
    try: Nombre = LocatedContractingParty['cac:Party']['cac:PartyName']['cbc:Name'].replace("'","")
    except: Nombre = ''
        
    try: TipoDeOrganoDeContratacion = LocatedContractingParty['cbc:ContractingPartyTypeCode']['#text']
    except: TipoDeOrganoDeContratacion = ''
        
    try:
        NIF = DIR3 =''
        Identificadores = LocatedContractingParty['cac:Party']['cac:PartyIdentification']
        for Identificador in Identificadores:
            tipo = Identificador['cbc:ID']['@schemeName']
            ID = Identificador['cbc:ID']['#text']
            if tipo == 'NIF':
                NIF = ID
            else:
                DIR3 = ID
    except: NIF = DIR3 =''
    
    if len(sql_select_all_where("oc_unicos","NombreEntry",Nombre,cnxn_string))==0:   
        OContratacion = get_data_OC(Link_IDEntry,Nombre)
        if len(OContratacion)>0:
            OContratacion['TipoDeOrganoDeContratacion']=TipoDeOrganoDeContratacion
            OContratacion['DIR3']=DIR3
            df_tmp = pd.DataFrame.from_dict(OContratacion,orient='index').T
            sql_append_df('oc_unicos',df_tmp,engine_string)
    

    OrganoDeContratacion = pd.DataFrame(data={'NombreOC': [Nombre], 'IDEntry': [IDEntry]})
    
    return(OrganoDeContratacion)



def get_data_OC(url_tmp,NombreEntry):
    
    page = urllib.request.urlopen(url_tmp)
    # parse the html using beautiful soup and store in variable 'soup'
    soup = BeautifulSoup(page, 'html.parser')
    OContratacion= {}
    try:
        url = soup.find('ul',{'class','altoDetalleLicitacion'}).find('a').get('href')
        page = urllib.request.urlopen(url)
        # parse the html using beautiful soup and store in variable 'soup'
        soup = BeautifulSoup(page, 'html.parser')


        lista_Jerarquia = soup.find('div',{'class','divDatosGeneralesIzq'}).find('li',{'id':'fila1_columna2'}).find('span').getText().replace('\n','').split('>')

        OContratacion['Nombre']  = soup.find('div',{'class','divDatosGeneralesIzq'}).find_all('ul')[1].find('li',{'id':'fila1_columna2'}).find('span').getText().replace("'","")

        try:
            OContratacion['NIF']  = soup.find('div',{'class','divDatosGeneralesIzq'}).find_all('span',{'title':'NIF:'})[1].getText()
        except:
            OContratacion['NIF'] = ""

        OContratacion['link'] = url
        #soup.find_all('a',{'title':'Este enlace se abrira en una ventana nueva'})[1].get('href')

        try:
            OContratacion['DirPaginaWeb'] = soup.find('div',{'class','divDatosGeneralesIzq'}).find_all('span',{'title':'Dirección del Site del Órgano:'})[1].getText()
        except:
            OContratacion['DirPaginaWeb']= ""

        Actividades = ""
        try:
            for actividad in soup.find('fieldset',{'id':'Actividades'}).find('div').find_all('span'):
                Actividades= Actividades+actividad.getText()+". "
        except:
            pass

        try:
            DirCalle = soup.find_all('fieldset')[4].find_all('span',{'title':'Vía: '})[1].getText()
            OContratacion['DirCalle'] = DirCalle
        except: 
            pass
        try:
            DirCodigoPostal = soup.find_all('fieldset')[4].find_all('span',{'title':'C.P.:'})[1].getText()
            OContratacion['DirCodigoPostal'] = DirCodigoPostal
        except:
            OContratacion['DirCodigoPostal'] = ""
            pass
        try:
            Provincia = soup.find_all('fieldset')[4].find_all('span',{'title':'Población: '})[1].getText()
            OContratacion['DirCiudad'] = Provincia
        except:
            OContratacion['DirCiudad'] = ""
            pass
        try:
            DirPais = soup.find_all('fieldset')[4].find_all('span',{'title':'Pais:'})[1].getText()
            OContratacion['DirPais'] = DirPais
        except:
            OContratacion['DirPais'] = ""
            pass
        try:
            ContactoEmail = soup.find_all('fieldset')[5].find_all('span',{'title':'Email:'})[1].getText()
            OContratacion['ContactoEmail'] = ContactoEmail
        except: 
            OContratacion['ContactoEmail']=""
            pass
        try:
            ContactoTelf = soup.find_all('fieldset')[5].find_all('span',{'title':'Teléfono:'})[2].getText()
            OContratacion['ContactoTelf'] = ContactoTelf
        except:
            OContratacion['ContactoTelf']=""

            pass
        
        OContratacion['NombreEntry']=NombreEntry
        try:
            OContratacion['Actividad'] = Actividades
            OContratacion['JeraquiaNivel0'] = 'Sector Público'
            OContratacion['JeraquiaNivel1'] = lista_Jerarquia[0]
            OContratacion['JeraquiaNivel2'] = lista_Jerarquia[1]
            OContratacion['JeraquiaNivel3'] = lista_Jerarquia[2]
            OContratacion['JeraquiaNivel4'] = lista_Jerarquia[3]
            OContratacion['JeraquiaNivel5'] = lista_Jerarquia[4]
            OContratacion['JeraquiaNivel6'] = lista_Jerarquia[5]
            OContratacion['JeraquiaNivel7'] = lista_Jerarquia[6]
        except:
            pass
    except:
        pass
        
    return(OContratacion)




# In[ ]:



# In[ ]:


def fc_AdditionalDocumentReference(ContractFolderSatus,IDEntry):
    df_PliegoDocumentosAdicionales=pd.DataFrame()
    try:
        PliegoDocumentosAdicionaless = ContractFolderSatus['cac:AdditionalDocumentReference']
        try:
            tmp = PliegoDocumentosAdicionaless['cbc:ID']
            PliegoDocumentosAdicionaless = [PliegoDocumentosAdicionaless]

        except:
            pass

        for PliegoDocumentosAdicionales in PliegoDocumentosAdicionaless:

            try: URI = PliegoDocumentosAdicionales['cac:Attachment']['cac:ExternalReference']['cbc:URI']
            except: URI = ''

            try: ID = PliegoDocumentosAdicionales['cbc:ID']
            except: ID = ''
            dict_PliegoDocumentosAdicionales = {'URI':URI,'ID':ID, 'IDEntry':IDEntry}
            df_PliegoDocumentosAdicionales = df_PliegoDocumentosAdicionales.append(dict_PliegoDocumentosAdicionales, ignore_index=True)
    except:
        pass
    
    return(df_PliegoDocumentosAdicionales)


# In[ ]:


def fc_ContractModifications(ContractFolderSatus,IDEntry):
    df_ModificacionesContrato = pd.DataFrame()
    try:
        
        ContractModifications = ContractFolderSatus['cac:ContractModification']
        try:
            tmp = ContractModifications['cbc:ContractID']
            ContractModifications = [ContractModifications]
        except: pass


        for ContractModification in ContractModifications:
            try: ImporteSinImpuestosDeLaModificacion = ContractModification['cac:ContractModificationLegalMonetaryTotal']['cbc:TaxExclusiveAmount']['#text']
            except: ImporteSinImpuestosDeLaModificacion=''
            try: ImporteSinImpuestosDeLaModificacionMoneda = ContractModification['cac:ContractModificationLegalMonetaryTotal']['cbc:TaxExclusiveAmount']['@currencyID']
            except: ImporteSinImpuestosDeLaModificacionMoneda=''
            try: ImporteSinImpuestosFinal = ContractModification['cac:FinalLegalMonetaryTotal']['cbc:TaxExclusiveAmount']['#text']
            except: ImporteSinImpuestosFinal=''
            try: ImporteSinImpuestosFinalMoneda = ContractModification['cac:FinalLegalMonetaryTotal']['cbc:TaxExclusiveAmount']['@currencyID']
            except: ImporteSinImpuestosFinalMoneda=''
            try: IDContrato = ContractModification['cbc:ContractID']
            except: IDContrato=''
            try: PlazoDeLaModificacion = ContractModification['cbc:ContractModificationDurationMeasure']['#text']
            except: PlazoDeLaModificacion=''
            try: PlazoDeLaModificacionMedida = ContractModification['cbc:ContractModificationDurationMeasure']['@unitCode']
            except: PlazoDeLaModificacionMedida=''
            try: DuracionTotalFinal = ContractModification['cbc:FinalDurationMeasure']['#text']
            except: DuracionTotalFinal=''
            try: DuracionTotalFinalMedida = ContractModification['cbc:FinalDurationMeasure']['@unitCode']
            except: DuracionTotalFinalMedida=''
            try: IDModificacion = ContractModification['cbc:ID']
            except: IDModificacion=''
            try: Nota = ContractModification['cbc:Note']
            except: Nota=''

            
            
            dict_ModificacionesContrato = {'ImporteSinImpuestosDeLaModificacion':ImporteSinImpuestosDeLaModificacion,'ImporteSinImpuestosDeLaModificacionMoneda':ImporteSinImpuestosDeLaModificacionMoneda,
                                          'ImporteSinImpuestosFinal':ImporteSinImpuestosFinal,'ImporteSinImpuestosFinalMoneda':ImporteSinImpuestosFinalMoneda,'IDContrato':IDContrato,'PlazoDeLaModificacion':PlazoDeLaModificacion,
                                          'PlazoDeLaModificacionMedida':PlazoDeLaModificacionMedida,'DuracionTotalFinal':DuracionTotalFinal,'DuracionTotalFinalMedida':DuracionTotalFinalMedida,
                                          'IDModificacion':IDModificacion,'Nota':Nota,'IDEntry':IDEntry}
            
            df_ModificacionesContrato =df_ModificacionesContrato.append(pd.DataFrame.from_dict(dict_ModificacionesContrato, orient='index').T)
    except:pass
    
    return(df_ModificacionesContrato)


# In[ ]:


def fc_ProcurementProject(ContractFolderSatus,IDEntry):
    
    df_ConsecucionProyectoCPV = pd.DataFrame()
    df_ConsecucionProyecto = pd.DataFrame()
    ProcurementProject = ContractFolderSatus['cac:ProcurementProject']


    try: ValorEstimado = ProcurementProject['cac:BudgetAmount']['cbc:EstimatedOverallContractAmount']['#text']
    except: ValorEstimado = ''
    try: ValorEstimadoMoneda = ProcurementProject['cac:BudgetAmount']['cbc:EstimatedOverallContractAmount']['@currencyID']
    except: ValorEstimadoMoneda = ''
    try: PresupuestoBaseLicitacionSinImpuestos = ProcurementProject['cac:BudgetAmount']['cbc:TaxExclusiveAmount']['#text']
    except: PresupuestoBaseLicitacionSinImpuestos = ''
    try: PresupuestoBaseLicitacionSinImpuestosMoneda = ProcurementProject['cac:BudgetAmount']['cbc:TaxExclusiveAmount']['@currencyID']
    except: PresupuestoBaseLicitacionSinImpuestosMoneda = ''
    try: PresupuestoBaseLicitacion = ProcurementProject['cac:BudgetAmount']['cbc:TotalAmount']['#text']
    except: PresupuestoBaseLicitacion = ''
    try: PresupuestoBaseLicitacionMoneda = ProcurementProject['cac:BudgetAmount']['cbc:TotalAmount']['@currencyID']
    except: PresupuestoBaseLicitacionMoneda = ''
    try: ProrrogaDescripcionOpciones = ProcurementProject['cac:ContractExtension']['cac:OptionValidityPeriod']['cbc:Description']
    except: ProrrogaDescripcionOpciones = ''
    try: ProrrogaDescripcion = ProcurementProject['cac:ContractExtension']['cbc:OptionsDescription']
    except: ProrrogaDescripcion = ''
    try: DuracionContrato = ProcurementProject['cac:PlannedPeriod']['cbc:DurationMeasure']['#text']
    except: DuracionContrato = ''
    try: DuracionContratoMedida = ProcurementProject['cac:PlannedPeriod']['cbc:DurationMeasure']['@unitCode']
    except: DuracionContratoMedida = ''
    try: FinContrato = ProcurementProject['cac:PlannedPeriod']['cbc:EndDate']
    except: FinContrato = ''
    try: InicioContrato = ProcurementProject['cac:PlannedPeriod']['cbc:StartDate']
    except: InicioContrato = ''
    try: LugarEjecucionPaisCodigo = ProcurementProject['cac:RealizedLocation']['cac:Address']['cac:Country']['cbc:IdentificationCode']['#text']
    except: LugarEjecucionPaisCodigo = ''
    try: LugarEjecucionPaisNombre = ProcurementProject['cac:RealizedLocation']['cac:Address']['cac:Country']['cbc:Name']
    except: LugarEjecucionPaisNombre = ''
    try: LugarEjecucionCiudad = ProcurementProject['cac:RealizedLocation']['cac:Address']['cbc:CityName']
    except: LugarEjecucionCiudad = ''
    try: LugarEjecucionCodigoPostal = ProcurementProject['cac:RealizedLocation']['cac:Address']['cbc:PostalZone']
    except: LugarEjecucionCodigoPostal = ''
    try: LugarEjecucionSubIdentidad = ProcurementProject['cac:RealizedLocation']['cbc:CountrySubentity']
    except: LugarEjecucionSubIdentidad = ''
    try: LugarEjecucionSubIdentidadCodigo = ProcurementProject['cac:RealizedLocation']['cbc:CountrySubentityCode']['#text']
    except: LugarEjecucionSubIdentidadCodigo = ''
    try: ObjetoDelContrato = ProcurementProject['cbc:Name']
    except: ObjetoDelContrato = ''
    try: ContratoSubtipo = ProcurementProject['cbc:SubTypeCode']['#text']
    except: ContratoSubtipo = ''
    try: ContratoTipo = ProcurementProject['cbc:TypeCode']['#text']
    except: ContratoTipo = ''

    dict_ConsecucionProyecto = {'ValorEstimado':ValorEstimado,'ValorEstimadoMoneda':ValorEstimadoMoneda,'PresupuestoBaseLicitacionSinImpuestos':PresupuestoBaseLicitacionSinImpuestos,'PresupuestoBaseLicitacionSinImpuestosMoneda':PresupuestoBaseLicitacionSinImpuestosMoneda,
                           'PresupuestoBaseLicitacion':PresupuestoBaseLicitacion,'PresupuestoBaseLicitacionMoneda':PresupuestoBaseLicitacionMoneda,'ProrrogaDescripcionOpciones':ProrrogaDescripcionOpciones,'ProrrogaDescripcion':ProrrogaDescripcion,
                           'DuracionContrato':DuracionContrato,'DuracionContratoMedida':DuracionContratoMedida,'FinContrato':FinContrato,'InicioContrato':InicioContrato,'LugarEjecucionPaisCodigo':LugarEjecucionPaisCodigo,
                           'LugarEjecucionPaisNombre':LugarEjecucionPaisNombre,'LugarEjecucionCiudad':LugarEjecucionCiudad,'LugarEjecucionCodigoPostal':LugarEjecucionCodigoPostal,'LugarEjecucionSubIdentidad':LugarEjecucionSubIdentidad,
                           'LugarEjecucionSubIdentidadCodigo':LugarEjecucionSubIdentidadCodigo,'ObjetoDelContrato':ObjetoDelContrato,'ContratoSubtipo':ContratoSubtipo,'ContratoTipo':ContratoTipo, 'IDEntry':IDEntry}

    df_ConsecucionProyecto = pd.DataFrame.from_dict(dict_ConsecucionProyecto, orient='index').T
    
    try:
        RequiredCommodityClassifications = ProcurementProject['cac:RequiredCommodityClassification']

        try:
            Codigo = RequiredCommodityClassifications['cbc:ItemClassificationCode']['#text']
            RequiredCommodityClassifications = [RequiredCommodityClassifications]

        except: pass

        for RequiredCommodityClassification in RequiredCommodityClassifications:
            try:
                Codigo = RequiredCommodityClassification['cbc:ItemClassificationCode']['#text']
                df_ConsecucionProyectoCPV = df_ConsecucionProyectoCPV.append({'Codigo':Codigo,'IDEntry':IDEntry}, ignore_index=True)
            except:pass

    except:
        pass
    
    return(df_ConsecucionProyecto,df_ConsecucionProyectoCPV)


# In[ ]:


def fc_ProcurementProjectLot(ContractFolderSatus,IDEntry):
    
    df_ConsecucionProyectoLotesCPV = pd.DataFrame()
    df_ConsecucionProyectoLotes = pd.DataFrame()
    try:

        ProcurementProjectLots = ContractFolderSatus['cac:ProcurementProjectLot']

        for ProcurementProjectLot in ProcurementProjectLots:
            try: PresupuestoBaseLicitacionSinImpuestos = ProcurementProjectLot['cac:ProcurementProject']['cac:BudgetAmount']['cbc:TaxExclusiveAmount']['#text']
            except: PresupuestoBaseLicitacionSinImpuestos = ''
            try: PresupuestoBaseLicitacionSinImpuestosMoneda = ProcurementProjectLot['cac:ProcurementProject']['cac:BudgetAmount']['cbc:TaxExclusiveAmount']['@currencyID']
            except: PresupuestoBaseLicitacionSinImpuestosMoneda = ''
            try: PresupuestoBaseLicitacion = ProcurementProjectLot['cac:ProcurementProject']['cac:BudgetAmount']['cbc:TotalAmount']['#text']
            except: PresupuestoBaseLicitacion = ''
            try: PresupuestoBaseLicitacionMoneda = ProcurementProjectLot['cac:ProcurementProject']['cac:BudgetAmount']['cbc:TotalAmount']['@currencyID']
            except: PresupuestoBaseLicitacionMoneda = ''

            try: LugarEjecucionPaisCodigo = ProcurementProjectLot['cac:ProcurementProject']['cac:RealizedLocation']['cac:Address']['cac:Country']['cbc:IdentificationCode']['#text']
            except: LugarEjecucionPaisCodigo = ''
            try: LugarEjecucionPaisNombre = ProcurementProjectLot['cac:ProcurementProject']['cac:RealizedLocation']['cac:Address']['cac:Country']['cbc:Name']
            except: LugarEjecucionPaisNombre = ''
            try: LugarEjecucionCiudad = ProcurementProjectLot['cac:ProcurementProject']['cac:RealizedLocation']['cac:Address']['cbc:CityName']
            except: LugarEjecucionCiudad = ''
            try: LugarEjecucionCodigoPostal = ProcurementProjectLot['cac:ProcurementProject']['cac:RealizedLocation']['cac:Address']['cbc:PostalZone']
            except: LugarEjecucionCodigoPostal = ''
            try: LugarEjecucionSubIdentidad = ProcurementProjectLot['cac:ProcurementProject']['cac:RealizedLocation']['cbc:CountrySubentity']
            except: LugarEjecucionSubIdentidad = ''
            try: LugarEjecucionSubIdentidadCodigo = ProcurementProjectLot['cac:ProcurementProject']['cac:RealizedLocation']['cbc:CountrySubentityCode']['#text']
            except: LugarEjecucionSubIdentidadCodigo = ''
            try: ObjetoDelContrato = ProcurementProjectLot['cac:ProcurementProject']['cbc:Name']
            except: ObjetoDelContrato = ''
            try: IDLote = ProcurementProjectLot['cbc:ID']['#text']
            except: IDLote = ''

            IDEntryIDLote = IDEntry+IDLote

            IDLoteCPV = IDEntry+IDLote

            dict_ConsecucionProyectoLotes = {'PresupuestoBaseLicitacionSinImpuestos':PresupuestoBaseLicitacionSinImpuestos,'PresupuestoBaseLicitacionSinImpuestosMoneda':PresupuestoBaseLicitacionSinImpuestosMoneda,'PresupuestoBaseLicitacion':PresupuestoBaseLicitacion,
                   'PresupuestoBaseLicitacionMoneda':PresupuestoBaseLicitacionMoneda,'LugarEjecucionPaisCodigo':LugarEjecucionPaisCodigo,'LugarEjecucionPaisNombre':LugarEjecucionPaisNombre,'LugarEjecucionCiudad':LugarEjecucionCiudad,
                   'LugarEjecucionCodigoPostal':LugarEjecucionCodigoPostal,'LugarEjecucionSubIdentidad':LugarEjecucionSubIdentidad,'LugarEjecucionSubIdentidadCodigo':LugarEjecucionSubIdentidadCodigo,'ObjetoDelContrato':ObjetoDelContrato,
                   'IDLote':IDLote,'IDEntry':IDEntry,'IDLoteCPV':IDLoteCPV,'IDEntryIDLote':IDEntryIDLote}

            df_ConsecucionProyectoLotes = df_ConsecucionProyectoLotes.append(dict_ConsecucionProyectoLotes, ignore_index=True)


            try:
                RequiredCommodityClassifications = ProcurementProjectLot['cac:ProcurementProject']['cac:RequiredCommodityClassification']

                try:
                    Codigo = RequiredCommodityClassifications['cbc:ItemClassificationCode']['#text']
                    RequiredCommodityClassifications = [RequiredCommodityClassifications]


                except: pass

                for RequiredCommodityClassification in RequiredCommodityClassifications:
                    try:
                        Codigo = RequiredCommodityClassification['cbc:ItemClassificationCode']['#text']
                        df_ConsecucionProyectoLotesCPV = df_ConsecucionProyectoLotesCPV.append({'Codigo':Codigo,'IDEntry':IDEntry,'IDLoteCPV':IDLoteCPV}, ignore_index=True)
                    except:pass

            except:
                pass

        if(len(df_ConsecucionProyectoLotes)!=len(df_ConsecucionProyectoLotes.IDEntryIDLote.unique())):print('Error')
    except:
        pass
    
    
    
    
    return(df_ConsecucionProyectoLotes,df_ConsecucionProyectoLotesCPV)


# In[ ]:


def fc_TenderResult(ContractFolderSatus,IDEntry):

    df_TenderResult = pd.DataFrame(columns={'ImporteAdjudicacion','ImporteAdjudicacionMoneda','ImporteAdjudicacionSinImpuestos','ImporteAdjudicacionSinImpuestosMoneda','IDLote', 'IDContrato',
                                            'FechaDeFormalizacion','AdjudicadoID','AdjudicadoIDSchemaName','AdjudicatarioNombre','ExclusionOfertasAnormalmenteBajas','FechaAdjudicacion','MotivoAdjudicacion','ImporteOfertaMasAlta',
                                            'ImporteOfertaMasAltaMoneda','ImporteOfertaMasBaja','ImporteOfertaMasBajaMoneda','OfertasRecibidas','TipoDeResultado','AdjudicatarioPYME','FechaEntradaEnVigor','IDEntry','IDEntryIDLote'})
    try:
        TenderResults = ContractFolderSatus['cac:TenderResult']
        
        try:
            ImporteAdjudicacion = TenderResults['cbc:ResultCode']['#text']       
            TenderResults = [TenderResults]

        except: pass


        for TenderResult in TenderResults:

            try: ImporteAdjudicacion = TenderResult['cac:AwardedTenderedProject']['cac:LegalMonetaryTotal']['cbc:PayableAmount']['#text']
            except: ImporteAdjudicacion = ''
            try: ImporteAdjudicacionMoneda = TenderResult['cac:AwardedTenderedProject']['cac:LegalMonetaryTotal']['cbc:PayableAmount']['@currencyID'] 
            except: ImporteAdjudicacionMoneda = ''
            try: ImporteAdjudicacionSinImpuestos = TenderResult['cac:AwardedTenderedProject']['cac:LegalMonetaryTotal']['cbc:TaxExclusiveAmount']['#text']
            except: ImporteAdjudicacionSinImpuestos = ''
            try: ImporteAdjudicacionSinImpuestosMoneda = TenderResult['cac:AwardedTenderedProject']['cac:LegalMonetaryTotal']['cbc:TaxExclusiveAmount']['@currencyID']
            except: ImporteAdjudicacionSinImpuestosMoneda = ''
            try: IDLote = TenderResult['cac:AwardedTenderedProject']['cbc:ProcurementProjectLotID']
            except: IDLote = ''
            try: IDContrato = TenderResult['cac:Contract']['cbc:ID']       
            except: IDContrato = ''
            try: 
                FechaDeFormalizacion = datetime.strptime( TenderResult['cac:Contract']['cbc:IssueDate'], '%Y-%m-%d')
                if FechaDeFormalizacion.year<1000: 
                    FechaDeFormalizacion = FechaDeFormalizacion.replace(year=a.year + 2000)
            except: FechaDeFormalizacion = ''
            try: AdjudicadoID = TenderResult['cac:WinningParty']['cac:PartyIdentification']['cbc:ID']['#text']      
            except: AdjudicadoID = ''
            try: AdjudicadoIDSchemaName = TenderResult['cac:WinningParty']['cac:PartyIdentification']['cbc:ID']['@schemeName']
            except: AdjudicadoIDSchemaName = ''
            try: AdjudicatarioNombre = TenderResult['cac:WinningParty']['cac:PartyName']['cbc:Name']       
            except: AdjudicatarioNombre = ''
            try: ExclusionOfertasAnormalmenteBajas = TenderResult['cbc:AbnormallyLowTendersIndicator']  
            except: ExclusionOfertasAnormalmenteBajas = ''
            try: 
                FechaAdjudicacion = datetime.strptime(TenderResult['cbc:AwardDate'], '%Y-%m-%d')
                if FechaAdjudicacion.year<1000: 
                    FechaAdjudicacion = FechaAdjudicacion.replace(year=a.year + 2000)
                
            except: FechaAdjudicacion = ''
            try: MotivoAdjudicacion = TenderResult['cbc:Description']         
            except: MotivoAdjudicacion = ''
            try: ImporteOfertaMasAlta = TenderResult['cbc:HigherTenderAmount']['#text']  
            except: ImporteOfertaMasAlta = ''
            try: ImporteOfertaMasAltaMoneda = TenderResult['cbc:HigherTenderAmount']['@currencyID']    
            except: ImporteOfertaMasAltaMoneda = ''
            try: ImporteOfertaMasBaja = TenderResult['cbc:LowerTenderAmount']['#text']  
            except: ImporteOfertaMasBaja = ''
            try: ImporteOfertaMasBajaMoneda = TenderResult['cbc:LowerTenderAmount']['@currencyID'] 
            except: ImporteOfertaMasBajaMoneda = ''
            try: OfertasRecibidas = TenderResult['cbc:ReceivedTenderQuantity']  
            except: OfertasRecibidas = ''
            try: TipoDeResultado = TenderResult['cbc:ResultCode']['#text']     
            except: 
                print('ERROR GRAVE')

                TipoDeResultado = ''
            try: AdjudicatarioPYME = TenderResult['cbc:SMEAwardedIndicator']         
            except: AdjudicatarioPYME = ''
            try: 
                FechaEntradaEnVigor = datetime.strptime(TenderResult['cbc:StartDate'], '%Y-%m-%d')
                if FechaEntradaEnVigor.year<1000: 
                    FechaEntradaEnVigor = FechaEntradaEnVigor.replace(year=a.year + 2000)
                    
            except: FechaEntradaEnVigor = ''
            
            if IDLote!='': IDEntryIDLote=IDEntry+IDLote
            else: IDEntryIDLote=''

            dict_TenderResults = {'ImporteAdjudicacion':ImporteAdjudicacion,'ImporteAdjudicacionMoneda':ImporteAdjudicacionMoneda,'ImporteAdjudicacionSinImpuestos':ImporteAdjudicacionSinImpuestos,'TipoDeResultado':TipoDeResultado,
                                  'IDLote':IDLote, 'IDContrato':IDContrato, 'FechaDeFormalizacion':FechaDeFormalizacion,'AdjudicadoID':AdjudicadoID,'AdjudicadoIDSchemaName':AdjudicadoIDSchemaName,'AdjudicatarioNombre':AdjudicatarioNombre,
                                  'ExclusionOfertasAnormalmenteBajas':ExclusionOfertasAnormalmenteBajas,'FechaAdjudicacion':FechaAdjudicacion,'MotivoAdjudicacion':MotivoAdjudicacion,'ImporteOfertaMasAlta':ImporteOfertaMasAlta,
                                  'ImporteOfertaMasAltaMoneda':ImporteOfertaMasAltaMoneda,'ImporteOfertaMasBaja':ImporteOfertaMasBaja,'ImporteOfertaMasBajaMoneda':ImporteOfertaMasBajaMoneda,'OfertasRecibidas':OfertasRecibidas,
                                  'AdjudicatarioPYME':AdjudicatarioPYME,'FechaEntradaEnVigor':FechaEntradaEnVigor,'ImporteAdjudicacionSinImpuestosMoneda':ImporteAdjudicacionSinImpuestosMoneda,'IDEntry':IDEntry,'IDEntryIDLote':IDEntryIDLote}
            df_TenderResult = df_TenderResult.append(dict_TenderResults, ignore_index=True)
    except: pass

    return(df_TenderResult)


# In[ ]:


def fc_TenderingProcess(ContractFolderSatus,IDEntry):
    df_ProcesoLicitacion = pd.DataFrame()
    
    try:
        TenderingProcess = ContractFolderSatus['cac:TenderingProcess']
        
        try: SubastaElectronica = TenderingProcess['cac:AuctionTerms']['cbc:AuctionConstraintIndicator']
        except: SubastaElectronica = ''
        try: 
            FechaLimiteObtencionPliegos = datetime.strptime(TenderingProcess['cac:DocumentAvailabilityPeriod']['cbc:EndDate'] , '%Y-%m-%d')
            if FechaLimiteObtencionPliegos.year<1000: 
                    FechaLimiteObtencionPliegos = FechaLimiteObtencionPliegos.replace(year=a.year + 2000)
            
        except: FechaLimiteObtencionPliegos = ''
        try: 
            FechaLimiteObtencionPliegosHora = datetime.strptime(TenderingProcess['cac:DocumentAvailabilityPeriod']['cbc:EndTime'], '%Y-%m-%d')
            if FechaLimiteObtencionPliegosHora.year<1000: 
                    FechaLimiteObtencionPliegosHora = FechaLimiteObtencionPliegosHora.replace(year=a.year + 2000)
            
        except: FechaLimiteObtencionPliegosHora = ''
        try: NumeroPrevistoOperadores = TenderingProcess['cac:EconomicOperatorShortList']['cbc:ExpectedQuantity']
        except: NumeroPrevistoOperadores = ''
        try: CriteriosLimitacionOperadores = TenderingProcess['cac:EconomicOperatorShortList']['cbc:LimitationDescription']
        except: CriteriosLimitacionOperadores = ''
        try: MaximoOperadores = TenderingProcess['cac:EconomicOperatorShortList']['cbc:MaximumQuantity']
        except: MaximoOperadores = ''
        try: MinimoOperadores = TenderingProcess['cac:EconomicOperatorShortList']['cbc:MinimumQuantity']
        except: MinimoOperadores = ''
        try: FechaLimiteOfertasDescripcion = TenderingProcess['cac:TenderSubmissionDeadlinePeriod']['cbc:Description']
        except: FechaLimiteOfertasDescripcion = ''
        try: 
            FechaLimiteOfertas = datetime.strptime(TenderingProcess['cac:TenderSubmissionDeadlinePeriod']['cbc:EndDate'], '%Y-%m-%d')
            if FechaLimiteOfertas.year<1000: 
                    FechaLimiteOfertas = FechaLimiteOfertas.replace(year=a.year + 2000)
                    
        except: FechaLimiteOfertas = ''
        try: FechaLimiteOfertasHora = TenderingProcess['cac:TenderSubmissionDeadlinePeriod']['cbc:EndTime']
        except: FechaLimiteOfertasHora = ''
        try: SistemaContratacion = TenderingProcess['cbc:ContractingSystemCode']['#text']
        except: SistemaContratacion = ''
        try: DerechoCombinacionLotes = TenderingProcess['cbc:LotsCombinationContractingAuthorityRights']
        except: DerechoCombinacionLotes = ''
        try: NumeroLotesMaximoPorOferta = TenderingProcess['cbc:MaximumLotPresentationQuantity']
        except: NumeroLotesMaximoPorOferta = ''
        try: NumeroLotesMaximoParaAdjudicar = TenderingProcess['cbc:MaximumTendererAwardedLotsQuantity']
        except: NumeroLotesMaximoParaAdjudicar = ''
        try: LotesPorOferta = TenderingProcess['cbc:PartPresentationCode']['#text']
        except: LotesPorOferta = ''
        try: LotesPorOfertaDescripcion = TenderingProcess['cbc:PartPresentationCode']['@name']
        except: LotesPorOfertaDescripcion = ''
        try: TipoProcedimiento = TenderingProcess['cbc:ProcedureCode']['#text']
        except: TipoProcedimiento = ''
        try: ModoPresentacionOferta = TenderingProcess['cbc:SubmissionMethodCode']['#text']
        except: ModoPresentacionOferta = ''
        try: TipoTramitacion = TenderingProcess['cbc:UrgencyCode']['#text']
        except: TipoTramitacion = ''
            
            
        dict_ProcesoLicitacion = {'SubastaElectronica':SubastaElectronica,'FechaLimiteObtencionPliegos':FechaLimiteObtencionPliegos,'FechaLimiteObtencionPliegosHora':FechaLimiteObtencionPliegosHora,
                 'NumeroPrevistoOperadores':NumeroPrevistoOperadores,'CriteriosLimitacionOperadores':CriteriosLimitacionOperadores,'MaximoOperadores':MaximoOperadores,
                 'MinimoOperadores':MinimoOperadores,'FechaLimiteOfertasDescripcion':FechaLimiteOfertasDescripcion,'FechaLimiteOfertas':FechaLimiteOfertas,'IDEntry':IDEntry,
                 'FechaLimiteOfertasHora':FechaLimiteOfertasHora,'SistemaContratacion':SistemaContratacion,'DerechoCombinacionLotes':DerechoCombinacionLotes,
                 'NumeroLotesMaximoPorOferta':NumeroLotesMaximoPorOferta,'NumeroLotesMaximoParaAdjudicar':NumeroLotesMaximoParaAdjudicar,'LotesPorOferta':LotesPorOferta,
                 'LotesPorOfertaDescripcion':LotesPorOfertaDescripcion,'TipoProcedimiento':TipoProcedimiento,'ModoPresentacionOferta':ModoPresentacionOferta,'TipoTramitacion':TipoTramitacion}    

        df_ProcesoLicitacion = pd.DataFrame.from_dict(dict_ProcesoLicitacion, orient='index').T
    except: pass
    
    return(df_ProcesoLicitacion)


# In[ ]:


def fc_TenderingTerms(ContractFolderSatus,IDEntry):
    
    df_CondicionesLicitacion = pd.DataFrame(columns={'CondicionLicitacionDescription','CondicionLicitacionRate','LenguajeID','SujetoRegulacionArmonizada','ProgramaDeFinanciacion','ProgramaDeFinanciacionDescripcion',
                            'FormulaDeRevisionDePrecios','AdmisionDeVariantes','RequierePresentacionCV','SolvenciaRequerida','TituloAvilitante','TituloAvilitante','SolvenciaRequerida','IDEntry'})

    df_CondicionesLicitacionCriterios = pd.DataFrame(columns={'Criterios','Ponderacion','IDEntry'})

    df_CondicionesLicitacion_GarantiasRequeridas =  pd.DataFrame(columns={'PorcentajeGarantia','TipoGarantia','Importe','ImporteMoneda','IDEntry'})

    df_CondicionesLicitacion_CriteriosFinancieros = pd.DataFrame(columns={'CriterioFinancieroDescripcion','TipoCriterioSolvencia','IDEntry'} )


    df_CondicionesLicitacion_CriteriosTecnicos = pd.DataFrame(columns={'CriterioTecnicoDescripcion','CodigoEvaluacionTecnica','IDEntry'} )

    df_CondicionesLicitacion_ClasificacionEmpresa = pd.DataFrame(columns={'ClasificacionEmpresarialSolicitadaCodigo','ClasificacionEmpresarialSolicitadaID','IDEntry'} )

    df_CondicionesLicitacion_CondicionesAdmision = pd.DataFrame(columns={'CondicionesAdmision','IDEntry'} )
    
    try:
        TenderingTerms = ContractFolderSatus['cac:TenderingTerms']

        try: CondicionLicitacionDescription = TenderingTerms['cac:AllowedSubcontractTerms']['cbc:Description']
        except: CondicionLicitacionDescription = ''
        try: CondicionLicitacionRate = TenderingTerms['cac:AllowedSubcontractTerms']['cbc:Rate']
        except: CondicionLicitacionRate = ''
        try: LenguajeID = TenderingTerms['cac:Language']['cbc:ID']
        except: LenguajeID = ''
        try: SujetoRegulacionArmonizada = TenderingTerms['cac:ProcurementLegislationDocumentReference']['cbc:ID']
        except: SujetoRegulacionArmonizada = ''
        try: ProgramaDeFinanciacion = TenderingTerms['cbc:FundingProgram']
        except: ProgramaDeFinanciacion = ''
        try: ProgramaDeFinanciacionDescripcion = TenderingTerms['cbc:FundingProgramCode']['#text']
        except: ProgramaDeFinanciacionDescripcion = ''
        try: FormulaDeRevisionDePrecios = TenderingTerms['cbc:PriceRevisionFormulaDescription']
        except: FormulaDeRevisionDePrecios = ''
        try: RequierePresentacionCV = TenderingTerms['cbc:RequiredCurriculaIndicator']
        except: RequierePresentacionCV = ''
        try: AdmisionDeVariantes = TenderingTerms['cbc:VariantConstraintIndicator']
        except: AdmisionDeVariantes = ''
        
        TendererQualificationRequest = TenderingTerms['cac:TendererQualificationRequest']

        try: SolvenciaRequerida = TendererQualificationRequest['cbc:Description']
        except: SolvenciaRequerida = ''
        try: TituloAvilitante = TendererQualificationRequest['cbc:PersonalSituation']
        except: TituloAvilitante = ''
    
        
    
        dict_CondicionesLicitacion ={'CondicionLicitacionDescription':CondicionLicitacionDescription,'CondicionLicitacionRate':CondicionLicitacionRate,'LenguajeID':LenguajeID,
                                     'SujetoRegulacionArmonizada':SujetoRegulacionArmonizada,'ProgramaDeFinanciacion':ProgramaDeFinanciacion,
                                     'ProgramaDeFinanciacionDescripcion':ProgramaDeFinanciacionDescripcion,'RequierePresentacionCV':RequierePresentacionCV,
                                     'FormulaDeRevisionDePrecios':FormulaDeRevisionDePrecios,'AdmisionDeVariantes':AdmisionDeVariantes,
                                     'TituloAvilitante':TituloAvilitante,'SolvenciaRequerida':SolvenciaRequerida,'IDEntry':IDEntry}
        
        df_CondicionesLicitacion = df_CondicionesLicitacion.append(dict_CondicionesLicitacion, ignore_index=True) 
        
        AwardingTermss = TenderingTerms['cac:AwardingTerms']['cac:AwardingCriteria']
        
        try: 
            tmp = AwardingTermss['cbc:Description']
            AwardingTermss = [AwardingTermss]
        except: pass
        
        for AwardingTerms in AwardingTermss:
            try: Criterios = AwardingTerms['cbc:Description']
            except:Criterios = ''
            try: Ponderacion = AwardingTerms['cbc:WeightNumeric']
            except: Ponderacion = ''   
        
            if(Criterios != '')|(Ponderacion != ''):
                dict_CondicionesLicitacionCriterios ={'Criterios':Criterios,'Ponderacion':Ponderacion,'IDEntry':IDEntry}
                df_CondicionesLicitacionCriterios = df_CondicionesLicitacionCriterios.append(dict_CondicionesLicitacionCriterios,ignore_index=True)

        RequiredFinancialGuarantees = TenderingTerms['cac:RequiredFinancialGuarantee']
        
        try: 
            tmp =  RequiredFinancialGuarantee['cbc:AmountRate']
            RequiredFinancialGuarantees = [RequiredFinancialGuarantees]
        except: pass
        
        for RequiredFinancialGuarantee in RequiredFinancialGuarantees:
 
            try: PorcentajeGarantia = RequiredFinancialGuarantee['cbc:AmountRate']
            except: PorcentajeGarantia = ''
            try: TipoGarantia = RequiredFinancialGuarantee['cbc:GuaranteeTypeCode']['#text']
            except: TipoGarantia = ''
            try: Importe = RequiredFinancialGuarantee['cbc:LiabilityAmount']['#text']
            except: Importe = ''
            try: ImporteMoneda = RequiredFinancialGuarantee['cbc:LiabilityAmount']['@currencyID']
            except: ImporteMoneda = ''
            
            if(PorcentajeGarantia!='')|(TipoGarantia!='')|(Importe!='')|(ImporteMoneda!=''):
                dict_CondicionesLicitacion_GarantiasRequeridas ={'PorcentajeGarantia':PorcentajeGarantia,'TipoGarantia':TipoGarantia,'Importe':Importe,'ImporteMoneda':ImporteMoneda,'IDEntry':IDEntry}
                df_CondicionesLicitacion_GarantiasRequeridas = df_CondicionesLicitacion_GarantiasRequeridas.append(dict_CondicionesLicitacion_GarantiasRequeridas,ignore_index=True)

        
        FinancialEvaluationCriterias = TendererQualificationRequest['cac:FinancialEvaluationCriteria']
        try: 
            tmp =  FinancialEvaluationCriterias['cbc:Description']
            FinancialEvaluationCriterias = [FinancialEvaluationCriterias]
        except: pass
        
        for FinancialEvaluationCriteria in FinancialEvaluationCriterias:
            try: CriterioFinancieroDescripcion = FinancialEvaluationCriteria['cbc:Description']
            except: CriterioFinancieroDescripcion = ''
            try: TipoCriterioSolvencia = FinancialEvaluationCriteria['cbc:EvaluationCriteriaTypeCode']['#text']
            except: TipoCriterioSolvencia = ''

        
            if(CriterioFinancieroDescripcion!='')|(TipoCriterioSolvencia!=''):
                dict_CondicionesLicitacion_CriteriosFinancieros ={'CriterioFinancieroDescripcion':CriterioFinancieroDescripcion,'TipoCriterioSolvencia':TipoCriterioSolvencia,'IDEntry':IDEntry}
                df_CondicionesLicitacion_CriteriosFinancieros = df_CondicionesLicitacion_CriteriosFinancieros.append(dict_CondicionesLicitacion_CriteriosFinancieros, ignore_index=True)

        TechnicalEvaluationCriterias = TendererQualificationRequest['cac:TechnicalEvaluationCriteria']
        try: 
            tmp =  TechnicalEvaluationCriterias['cbc:Description']
            TechnicalEvaluationCriterias = [TechnicalEvaluationCriterias]
        except: pass
        
        for TechnicalEvaluationCriteria in TechnicalEvaluationCriterias:
            try: CriterioTecnicoDescripcion = TechnicalEvaluationCriteria['cbc:Description']
            except: CriterioTecnicoDescripcion = ''
            try: CodigoEvaluacionTecnica = TechnicalEvaluationCriteria['cbc:EvaluationCriteriaTypeCode']['#text']
            except: CodigoEvaluacionTecnica = ''

        
            if(CriterioTecnicoDescripcion!='')|(CodigoEvaluacionTecnica!=''):
                dict_CondicionesLicitacion_CriteriosTecnicos ={'CriterioTecnicoDescripcion':CriterioTecnicoDescripcion,'CodigoEvaluacionTecnica':CodigoEvaluacionTecnica,'IDEntry':IDEntry}
                df_CondicionesLicitacion_CriteriosTecnicos = df_CondicionesLicitacion_CriteriosTecnicos.append(dict_CondicionesLicitacion_CriteriosTecnicos, ignore_index=True)      
                
        
        RequiredBusinessClassificationScheme = TendererQualificationRequest['cac:RequiredBusinessClassificationScheme']
        try: ClasificacionEmpresarialSolicitadaID = RequiredBusinessClassificationScheme['cbc:ID']
        except: ClasificacionEmpresarialSolicitadaID = ''
                
        ClassificationCategorys = RequiredBusinessClassificationScheme['cac:ClassificationCategory']        
        try: 
            tmp =  ClassificationCategorys['cbc:CodeValue']
            ClassificationCategorys = [ClassificationCategorys]
        except: pass
        
        for ClassificationCategory in ClassificationCategorys:
            try: ClasificacionEmpresarialSolicitadaCodigo = ClassificationCategory['cbc:CodeValue']
            except: ClasificacionEmpresarialSolicitadaCodigo = ''
            
        
        
            if(ClasificacionEmpresarialSolicitadaCodigo!='')|(ClasificacionEmpresarialSolicitadaID!=''):
                dict_CondicionesLicitacion_ClasificacionEmpresa ={'ClasificacionEmpresarialSolicitadaCodigo':ClasificacionEmpresarialSolicitadaCodigo,'ClasificacionEmpresarialSolicitadaID':ClasificacionEmpresarialSolicitadaID,'IDEntry':IDEntry}
                df_CondicionesLicitacion_ClasificacionEmpresa = df_CondicionesLicitacion_ClasificacionEmpresa.append(dict_CondicionesLicitacion_ClasificacionEmpresa, ignore_index=True)      
                
        
        SpecificTendererRequirements = TendererQualificationRequest['cac:SpecificTendererRequirement']
        
        
        try: 
            tmp =  SpecificTendererRequirements['cbc:RequirementTypeCode']['#text']
            SpecificTendererRequirements = [SpecificTendererRequirements]
        except: pass
        
        for SpecificTendererRequirement in SpecificTendererRequirements:
            try: CondicionesAdmision = SpecificTendererRequirement['cbc:RequirementTypeCode']['#text']
            except: CondicionesAdmision = ''
        
            if(CondicionesAdmision!=''):
                dict_CondicionesLicitacion_CondicionesAdmision ={'CondicionesAdmision':CondicionesAdmision,'IDEntry':IDEntry}
                df_CondicionesLicitacion_CondicionesAdmision = df_CondicionesLicitacion_CondicionesAdmision.append(dict_CondicionesLicitacion_CondicionesAdmision, ignore_index=True)      
                
    except:
        pass
    

    
    return(df_CondicionesLicitacion,df_CondicionesLicitacionCriterios,df_CondicionesLicitacion_GarantiasRequeridas,df_CondicionesLicitacion_CriteriosFinancieros,df_CondicionesLicitacion_CriteriosTecnicos,df_CondicionesLicitacion_ClasificacionEmpresa,df_CondicionesLicitacion_CondicionesAdmision)


# In[11]:


def sql_append_df(_df,df,engine_string):
    _df = _df.lower()
    sqlEngine       = create_engine(engine_string)
    dbConnection    = sqlEngine.connect()
    #print(list(df.columns))
    frame           = df.to_sql(name = _df, con= dbConnection, if_exists='append', index=False)
    #print(_df, 'Tabla Guardada')
    dbConnection.close()
    return()


# In[ ]:


def sql_exist_entry(Valor,cnxn_string):

    mydb = mysql.connector.connect(**cnxn_string)
    mycursor = mydb.cursor()
    
    
    
    sql = "SELECT UltimaActualizacion FROM  Licitaciones WHERE IDEntry ='"+Valor+"';"
    mycursor.execute(sql)
    try:
        row = mycursor.fetchone()[0]
        
    except:
        row=0
    mycursor.close()
    #if row == 0 : print("En la tabla ",Tabla,"para el ",Campo," el valor ",Valor," -> NO EXISTE")
    #else:
        #print("En la tabla ",Tabla,"para el ",Campo," el valor ",Valor," -> SI EXISTE")
    return(row)
    # 1=Existe | 0=No Existe

    mydb.close()
# In[ ]:


def sql_exist_delete(Valor,cnxn_string):

    mydb = mysql.connector.connect(**cnxn_string)
    mycursor = mydb.cursor()
    
    sql = "SELECT * FROM  Deleted WHERE IDdeleted ='"+Valor+"';"
    mycursor.execute(sql)
    try:
        row = mycursor.fetchone()[0]
        
    except:
        row=0
    mycursor.close()
    
    return(row)
    # 1=Existe | 0=No Existe


# In[ ]:


def sql_delete(IDEntry,cnxn_string):
    Tablas = ['licitaciones']
    
    
    
    mydb = mysql.connector.connect(**cnxn_string)
    mycursor = mydb.cursor()

    for Table in Tablas:
        sql   = "DELETE FROM  "+Table+" where IDEntry ='"+IDEntry+"'"
        #print(sql)
        mycursor.execute(sql)
        mydb.commit() 
    mycursor.close()
    return()


# In[ ]:


def sql_update(IDEntry,newIDentry,cnxn_string):
    Tablas = ['licitaciones', 'documentosgenerales', 'organodecontratacion', 
              'pliegodocumentosadicionales', 'modificacionescontrato', 'consecucionproyecto', 'consecucionproyectocpv',
               'procesolicitacion', 'condicioneslicitacion', 'condicioneslicitacion_criterios', 'condicioneslicitacion_garantiasrequeridas', 'condicioneslicitacion_criteriosfinancieros',
              'condicioneslicitacion_criteriostecnicos', 'condicioneslicitacion_clasificacionempresa', 'condicioneslicitacion_condicionesadmision', 'consecucionproyectolotes', 'consecucionproyectolotescpv','resultadolicitacion']
    TablasLotes = ['consecucionproyectolotes','resultadolicitacion']

    mydb = mysql.connector.connect(**cnxn_string)
    mycursor = mydb.cursor()
    for Table in Tablas:
        sql  = "UPDATE  "+Table+" SET IDEntry = '"+newIDentry+"' where IDEntry ='"+IDEntry+"'"
        #print(sql)
        mycursor.execute(sql)
        mydb.commit() 
    
    for Table2 in TablasLotes:
        sql2   = "UPDATE  "+Table2+" SET IDEntryIDLote = REPLACE(IDEntryIDLote, '"+IDEntry+"', '"+newIDentry+"') where IDEntry ='"+newIDentry+"'"
        #print(sql)
        mycursor.execute(sql2)
        mydb.commit()   
    
    
    sql3   = "UPDATE  ConsecucionProyectoLotesCPV SET IDLoteCPV = REPLACE(IDLoteCPV, '"+IDEntry+"', '"+newIDentry+"') where IDEntry ='"+newIDentry+"'"
    #print(sql)
    mycursor.execute(sql3)
    mydb.commit()   
    
    
    
    mycursor.close()
    return()


# In[ ]:


def sql_select_all_where(Tabla,Campo,Valor,cnxn_string):

    mydb = mysql.connector.connect(**cnxn_string)
    
    sql = "SELECT * FROM  "+Tabla+" WHERE "+Campo+" ='"+Valor+"';"
    data = pd.read_sql(sql,mydb)
    return(data)


# In[ ]:


def sql_delete_ContratanteCompleto3(UrlFin,cnxn_string):
    
    
    mydb = mysql.connector.connect(**cnxn_string)
    mycursor = mydb.cursor()
    
    Tables = ['licitaciones', 'documentosgenerales', 'organodecontratacion', 
              'pliegodocumentosadicionales', 'modificacionescontrato', 'consecucionproyecto', 'consecucionproyectocpv',
               'procesolicitacion', 'condicioneslicitacion', 'condicioneslicitacion_criterios', 'condicioneslicitacion_garantiasrequeridas', 'CondicionesLicitacion_CriteriosFinancieros',
              'condicioneslicitacion_criteriostecnicos', 'condicioneslicitacion_clasificacionempresa', 'condicioneslicitacion_condicionesadmision', 'consecucionproyectolotes', 'consecucionproyectolotescpv','resultadolicitacion']
    
    
    to_delete_org = list(sql_select_all_where('Licitaciones','Link_Atom',UrlFin,cnxn_string)['IDEntry'])
    for Table in Tables: 
        #print(Table)
        to_delete = str([(lambda x: ' or IDEntry = '+x)(x) for x in to_delete_org]).replace(", ' or IDEntry = "," or IDEntry = '").replace("[' or IDEntry = ","IDEntry = '").replace("']","';")

    
    sql   = "DELETE FROM  "+Table+" where "+ to_delete
   
    try:
        mycursor.execute(sql)
        mycursor.close()
    except:
        pass
    sql2= "DELETE from  Atoms where self = '"+UrlFin+"';"
    mycursor2 = mydb.cursor()
    mycursor2.execute(sql2)
    mycursor2.close()
    mydb.commit() 
    
    print('Eliminados')
    


# In[14]:


def last_URL(UrlFin,cnxn_string):
    UrlFin = UrlFin.replace('.atom','')
    mydb = mysql.connector.connect(**cnxn_string)
    mycursor = mydb.cursor()
    sql_recent = "Select link_Atom FROM Licitaciones WHERE UltimaActualizacion = (Select MAX(UltimaActualizacion) FROM Licitaciones WHERE Link_Atom LIKE '%"+UrlFin+"%')"
    data = pd.read_sql(sql_recent,mydb)
    link_Atom = data.link_Atom[0]

    return(link_Atom)


# In[ ]:
    
    



# In[6]:


def fc_entries(df_licitaciones,entries_to_normalize,link_self):    
    
    # ORGANO DE CONTRATACION
    OrganosDeContratacion_list = [fc_OrganoDeContratacion(x['cac-place-ext:ContractFolderStatus'],x['link']['@href'],x['id']) for x in entries_to_normalize]
    df_OrganoDeContratacion = pd.concat(OrganosDeContratacion_list)

    # DOCUMENTOS GENERALES
    DocumentosGenerales_list = [fc_DocumentosGenerales(x['cac-place-ext:ContractFolderStatus'],x['id']) for x in entries_to_normalize]
    df_DocumentosGenerales = pd.concat(DocumentosGenerales_list)

    # PLIEGOS ADICIONALES
    PliegoDocumentosAdicionales_list = [fc_AdditionalDocumentReference(x['cac-place-ext:ContractFolderStatus'],x['id']) for x in entries_to_normalize]
    df_PliegoDocumentosAdicionales = pd.concat(PliegoDocumentosAdicionales_list)

    #MODIFICACIONES DE CONTRATO
    ContractModifications_list = [fc_ContractModifications(x['cac-place-ext:ContractFolderStatus'],x['id']) for x in entries_to_normalize]
    df_ModificacionesContrato = pd.concat(ContractModifications_list)

    # CONSECUCION DEL PROYECTO CON LOTES
    ConsecucionProyectoLotes_list_tuple = [fc_ProcurementProjectLot(x['cac-place-ext:ContractFolderStatus'],x['id']) for x in entries_to_normalize]
    ConsecucionProyectoLotes_list = [x[0] for x in ConsecucionProyectoLotes_list_tuple]
    ConsecucionProyectoLotesCPV_list = [x[1] for x in ConsecucionProyectoLotes_list_tuple]
    df_ConsecucionProyectoLotes=pd.concat(ConsecucionProyectoLotes_list)
    df_ConsecucionProyectoLotesCPV=pd.concat(ConsecucionProyectoLotesCPV_list)

    # CONSECUCION DEL PROYECTO 
    ConsecucionProyecto_list_tuple = [fc_ProcurementProject(x['cac-place-ext:ContractFolderStatus'],x['id']) for x in entries_to_normalize]
    ConsecucionProyecto_list = [x[0] for x in ConsecucionProyecto_list_tuple]
    ConsecucionProyectoCPV_list = [x[1] for x in ConsecucionProyecto_list_tuple]
    df_ConsecucionProyecto=pd.concat(ConsecucionProyecto_list)
    df_ConsecucionProyectoCPV=pd.concat(ConsecucionProyectoCPV_list)

    #RESULTADO LICITACION
    ResultadoLicitacion_list = [fc_TenderResult(x['cac-place-ext:ContractFolderStatus'],x['id']) for x in entries_to_normalize]
    df_ResultadoLicitacion = pd.concat(ResultadoLicitacion_list)

    #PROCESO DE LICITACION
    ProcesoLicitacion_list = [fc_TenderingProcess(x['cac-place-ext:ContractFolderStatus'],x['id']) for x in entries_to_normalize]
    df_ProcesoLicitacion = pd.concat(ProcesoLicitacion_list)

    #CONDICIONES DE LICITACION
    CondicionesLicitacion_list_Multiple = [fc_TenderingTerms(x['cac-place-ext:ContractFolderStatus'],x['id']) for x in entries_to_normalize]
    CondicionesLicitacion_list = [x[0] for x in CondicionesLicitacion_list_Multiple]
    CondicionesLicitacion_Criterios_list = [x[1] for x in CondicionesLicitacion_list_Multiple]
    CondicionesLicitacion_GarantiasRequeridas_list = [x[2] for x in CondicionesLicitacion_list_Multiple]
    CondicionesLicitacion_CriteriosFinancieros_list = [x[3] for x in CondicionesLicitacion_list_Multiple]
    CondicionesLicitacion_CriteriosTecnicos_list = [x[4] for x in CondicionesLicitacion_list_Multiple]
    CondicionesLicitacion_ClasificacionEmpresa_list = [x[5] for x in CondicionesLicitacion_list_Multiple]
    CondicionesLicitacion_CondicionesAdmision_list = [x[6] for x in CondicionesLicitacion_list_Multiple]

    df_CondicionesLicitacion = pd.concat(CondicionesLicitacion_list)
    df_CondicionesLicitacion_Criterios = pd.concat(CondicionesLicitacion_Criterios_list)
    df_CondicionesLicitacion_GarantiasRequeridas = pd.concat(CondicionesLicitacion_GarantiasRequeridas_list)
    df_CondicionesLicitacion_CriteriosFinancieros = pd.concat(CondicionesLicitacion_CriteriosFinancieros_list)
    df_CondicionesLicitacion_CriteriosTecnicos = pd.concat(CondicionesLicitacion_CriteriosTecnicos_list)
    df_CondicionesLicitacion_ClasificacionEmpresa = pd.concat(CondicionesLicitacion_ClasificacionEmpresa_list)
    df_CondicionesLicitacion_CondicionesAdmision = pd.concat(CondicionesLicitacion_CondicionesAdmision_list)

    dict_dfs = {}

    df_atom_content = df_licitaciones[['IDEntry','UltimaActualizacion']]
    df_atom_content['atom'] = link_self
    
    df_licitaciones = df_licitaciones.merge(df_OrganoDeContratacion,on='IDEntry',how='left')
    
    dict_dfs = {
        'ContenidoAtom':df_atom_content,
        'Licitaciones': df_licitaciones,
        'DocumentosGenerales':df_DocumentosGenerales,
        #'OrganoDeContratacion':df_OrganoDeContratacion,
        'PliegoDocumentosAdicionales':df_PliegoDocumentosAdicionales,
        'ModificacionesContrato':df_ModificacionesContrato,
        'ConsecucionProyecto':df_ConsecucionProyecto,
        'ConsecucionProyectoCPV':df_ConsecucionProyectoCPV,
        'ConsecucionProyectoLotes':df_ConsecucionProyectoLotes,
        'ConsecucionProyectoLotesCPV':df_ConsecucionProyectoLotesCPV,
        'ResultadoLicitacion':df_ResultadoLicitacion,
        'ProcesoLicitacion':df_ProcesoLicitacion,
        'CondicionesLicitacion':df_CondicionesLicitacion,
        'CondicionesLicitacion_Criterios':df_CondicionesLicitacion_Criterios,
        'CondicionesLicitacion_GarantiasRequeridas':df_CondicionesLicitacion_GarantiasRequeridas,
        'CondicionesLicitacion_CriteriosFinancieros':df_CondicionesLicitacion_CriteriosFinancieros,
        'CondicionesLicitacion_CriteriosTecnicos':df_CondicionesLicitacion_CriteriosTecnicos,
        'CondicionesLicitacion_ClasificacionEmpresa':df_CondicionesLicitacion_ClasificacionEmpresa,
        'CondicionesLicitacion_CondicionesAdmision':df_CondicionesLicitacion_CondicionesAdmision

    }

    
    elementos = dict_dfs.items()
    for clave, valor in elementos:
        if len(valor)>0:
            valor['IDEntry'] = valor['IDEntry'].apply(lambda x: x.replace('https://contrataciondelestado.es/sindicacion/',''))
            dict_dfs[clave]=valor
    
    
    return(dict_dfs)


# In[7]:




# In[8]:


def Normalize_atom(url,engine_string):
    
    
    start_time = time.time()                                                                     # Marcamos la hora de inicio
    
    
    response = requests.get(url)                                                                 # Hacemos la solicitud de la pagina
    infile = response.content                                                                    # Recibimos el contenido de la respuesta 
    mydict = xmltodict.parse(infile)                                                             # Transformamos la respuesta en una estructura de diccionario.
    print("Peticion url: --- %s seconds ---" % (time.time() - start_time))                       # Imprimimos lo que se ha tardado en descargar y tranformar

    dict_atoms = {}                                                                              # Declaramos un diccionario para almacenar la información
    
    links = mydict['feed']['link']                                                               # Extraemos la infromacion sobre los links de enlace del atom
    for link in links:
        href = link['@href']
        idlink = link['@rel']
        dict_atoms[idlink]=href
        
    dict_atoms['titulo']= mydict['feed']['title']                                                # Extraemos el titulo del Atom
    dict_atoms['Actualizado']= mydict['feed']['updated'].replace('T',' ').rsplit(':',2)[0]       # Extraemos la fecha de actualizacion
    dict_atoms['Actualizado'] = datetime.strptime(dict_atoms['Actualizado'], '%Y-%m-%d %H:%M')   # y le damos formato
    
    if dict_atoms['Actualizado'].year<1000: 
        dict_atoms['Actualizado'] = dict_atoms['Actualizado'].replace(year=a.year + 2000)
    
    df_atoms = pd.DataFrame.from_dict(dict_atoms,orient='index').T                               # Transformamos el diccionario en un dataframe
    sql_append_df('atoms',df_atoms,engine_string)                                                # Alamacenamos los datos del atom en la bbdd
    
    url_next = df_atoms['prev'][0]
    link_self = df_atoms['self'][0]
    
    
    df_delete = pd.DataFrame()                                                                   # Desclaramos el dataframe donde almacenaremos los registros "eliminados"
    
    try:                                                                                         # Utilizamos un control de excepciones por si no hay sección de eliminados
        for deleted in mydict['feed']['at:deleted-entry']:                                       
            ref = deleted['@ref']
            when = deleted['@when']
            IDdeleted = ref+when
            exist = sql_exist_delete(IDdeleted,cnxn)
            if exist == 0:                                                                       # Si el registro no existe 
                sql_update(ref,IDdeleted,cnxn)                                                   # Actualizamos los IDEntry de todas las tablas
                df_delete = df_delete.append({'IDEntry' : ref , 'Date' : when, 'IDdeleted':IDdeleted} , ignore_index=True) # Incluimos en el dataframe de registros deleted
    except:
        pass

    entries = mydict['feed']['entry']                                                             # Extraemos los entries del atom
    entires_list = [fc_licitaciones(x) for x in entries]                                          # Transformamos cada entry en un dataframe y los apilamos en una lista
    entries_df = pd.concat(entires_list)                                                          # Concatenamos cada df de la lista
    entries_df['Link_Atom']=link_self                                                             # Añadimos una columna de referenci del atom de origen
    entries_df['Id']=entries_df['IDEntry']                                                        # Igualamos las columnas IDEntry e ID
    
    entries_df['IDEntry'] = entries_df['IDEntry'].apply(lambda x: x.replace('https://contrataciondelestado.es/sindicacion/','')) # Remplazamos parte del texto de IDEntry
    entries_df = entries_df.reset_index().drop(['index'],axis=1).drop_duplicates(subset=['IDEntry'], keep='last')               #Eliminamos los registros (entries) que esten duplicados y nos quedamos con el mas reciente
    
    #Filtamos la lista original de entries, quitando asi los duplicados
    entries_filtered = entries
    #entries_filtered = [entries[x] for x in entries_df.index.to_list()]



    before_sql = time.time()
    
    list_IDEntry_ToSql = str(entries_df['IDEntry'].to_list()).replace('[','(').replace(']',')')            # Extraemos un df con los entries que coinciden con el atom en la bbdd
    sql = 'SELECT IDEntry,UltimaActualizacion FROM Licitaciones WHERE IDEntry IN '+ list_IDEntry_ToSql     # con su fecha de ultima actualizacion 
    mydb = mysql.connector.connect(**cnxn_string)
    data = pd.read_sql(sql,mydb)

    print("Consulta sql: --- %s seconds ---" % (time.time() - before_sql))                                 # Imprimimos en pantalla cuanto ha tardado la operación de consulta

    before_Normalize = time.time()

    IDEntry_list_in_bbdd = data['IDEntry'].to_list()                                                       # Extraemos unicamenta la lista de identrys con la que vamos a comparar
    entries_df_toupdate = entries_df.loc[entries_df['IDEntry'].isin(IDEntry_list_in_bbdd)]                 # el cruce de las dos listas da como resultado los identry para actualizar
    entries_df_toinsert = entries_df.loc[~entries_df['IDEntry'].isin(IDEntry_list_in_bbdd)]                # lo contrario al paso anterior nos da los nuevos registros para insertar
    
    
    entries_filtered_toupdate = [entries_filtered[x] for x in entries_df_toupdate.index.to_list()]         # Filtramos el listado de entries para actualizar
    entries_filtered_toinsert = [entries_filtered[x] for x in entries_df_toinsert.index.to_list()]         # Filtramos el listado de entries para insertar

    if len(entries_df_toinsert)>0 : dict_toinsert = fc_entries(entries_df_toinsert,entries_filtered_toinsert,link_self) # Si Existen entries para insertar nuevos los mandamos a la funcion de entries
    else:                                                                                                     # Si no hay se queda vacio
        dict_toinsert={}
    if len(entries_df_toupdate)>0 : dict_toupdate = fc_entries(entries_df_toupdate,entries_filtered_toupdate,link_self) # Si Existen entries para actualizar los mandamos a la funcion de entries
    else: 
        dict_toupdate={}                                                                                      # Si no hay se queda vacio 

    print("Normalizacion: --- %s seconds ---" % (time.time() - before_Normalize))                             # Imprimimos por pantalla el tiempo de normalizacion


    # Nuevos
    before_SaveNews = time.time()                                                                             # Guardamos los registros nuevos
    if len(dict_toinsert)>0:
        elementos = dict_toinsert.items()
        for clave, valor in elementos:
            if len(valor)>0:
                clave = clave.lower()
                valor = valor.drop_duplicates()
                sql_append_df(clave,valor,engine_string)

    print("Guardar Registros Nuevos: --- %s seconds ---" % (time.time() - before_SaveNews)) 

    # Actualizacion
    before_Delete = time.time()                                                                               # Actualizamos los registros existentes. 
    if len(dict_toupdate)>0:
        subdf_IDEntry_toupdate = dict_toupdate['Licitaciones'][['IDEntry','UltimaActualizacion']]
        lista_new_insert=[]
        for index, row in subdf_IDEntry_toupdate.iterrows():
            exist = data.loc[data.IDEntry == row['IDEntry'],['UltimaActualizacion']]
            exist = exist.iloc[0]['UltimaActualizacion']
            if str(exist) != '0':
                if row['UltimaActualizacion'] > exist:
                    lista_new_insert.append(row['IDEntry'])
                    sql_delete(row['IDEntry'],cnxn_string)

    print("Eliminar Registros: --- %s seconds ---" % (time.time() - before_Delete))
    if len(dict_toupdate)>0:
        elementos = dict_toupdate.items()
        for clave, valor in elementos:
            if len(valor)>0:
                df_tmp = valor.loc[valor['IDEntry'].isin(lista_new_insert)]
                df_tmp = df_tmp.drop_duplicates()
                sql_append_df(clave,df_tmp,engine_string)

    print("Actualizar Registros: --- %s seconds ---" % (time.time() - before_SaveNews))


    print("Total: --- %s seconds ---" % (time.time() - start_time)) 

    print("Numero de entradas: ",len(entries)," | Nuevas: ",len(entries_df_toinsert)," | Update: ",len(entries_df_toupdate)," | Duplicadas: ",len(entries)-len(entries_df))
    
    return(url_next)


# In[ ]:


UrlFin = 'https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom'
try:
    url = last_URL(UrlFin,cnxn_string)
except:
    url = "https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_20200607_131257_3.atom"
while url != UrlFin:
    url_next = Normalize_atom(url,engine_string)
    url = url_next


# In[ ]:




