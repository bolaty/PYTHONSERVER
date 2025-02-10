from flask import Blueprint, request, jsonify
import pyodbc
import datetime
from datetime import datetime
from typing import List
import json
import requests
import socket
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import uuid
#import pyodbc
import sys
sys.path.append("../")
from config import MYSQL_REPONSE,LIENAPISMS
import threading


def insert_RemboursementCreditEnContentieuxAvecDepotGarantie(connection, RemboursementCreditEnContentieux_info):
    ip_address = get_ip_address()
    public_ip_address = get_public_ip_address()
    mac_address = get_mac_address()
    teminal = ip_address + "@" + public_ip_address + "@" + mac_address
    params = {
        'AG_CODEAGENCE': RemboursementCreditEnContentieux_info['AG_CODEAGENCE'],
        'PV_CODEPOINTVENTE': RemboursementCreditEnContentieux_info['PV_CODEPOINTVENTE'],
        'CR_CODECREDIT': RemboursementCreditEnContentieux_info['CR_CODECREDIT'],
        'CR_MONTANTGARANTIE': RemboursementCreditEnContentieux_info['CR_MONTANTGARANTIE'],
        'DATEJOURNEE': RemboursementCreditEnContentieux_info['DATEJOURNEE'],
        'OP_CODEOPERATEUR': RemboursementCreditEnContentieux_info['OP_CODEOPERATEUR'],
        'CODECRYPTAGE': RemboursementCreditEnContentieux_info['CODECRYPTAGE'],
        'MC_TERMINAL': teminal,#RemboursementCreditEnContentieux_info['MC_TERMINAL'],
        'MC_AUTRE1': RemboursementCreditEnContentieux_info['MC_AUTRE1'],
        'MC_AUTRE2': RemboursementCreditEnContentieux_info['MC_AUTRE2'],
        'MC_AUTRE3': RemboursementCreditEnContentieux_info['MC_AUTRE3']
    }
    

    try:
        cursor = connection
        cursor.execute("EXEC dbo.PS_REMBOURSEMENTCREDITENCONTENTIEUXAVECDEPOTGARANTIE ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?", list(params.values()))
        connection.commit()
        get_commit(connection,RemboursementCreditEnContentieux_info)
        #cursor.close()
    except Exception as e:
        connection.rollback()
        raise Exception(f"Erreur lors de la modification: {str(e.args[1])}")

def insert_pvgMicreditReglementDepotGarantieApresDeblocage(connection, ReglementDepotGarantieApresDeblocage_info):
    
    params = {
        'AG_CODEAGENCE': ReglementDepotGarantieApresDeblocage_info['AG_CODEAGENCE'],
        'PV_CODEPOINTVENTE': ReglementDepotGarantieApresDeblocage_info['PV_CODEPOINTVENTE'],
        'CR_CODECREDIT': ReglementDepotGarantieApresDeblocage_info['CR_CODECREDIT'],
        'CR_MONTANTGARANTIE': ReglementDepotGarantieApresDeblocage_info['CR_MONTANTGARANTIE'],
        'DATEJOURNEE': ReglementDepotGarantieApresDeblocage_info['DATEJOURNEE'],
        'OP_CODEOPERATEUR': ReglementDepotGarantieApresDeblocage_info['OP_CODEOPERATEUR'],
        'TS_CODETYPESCHEMACOMPTABLE': ReglementDepotGarantieApresDeblocage_info['TS_CODETYPESCHEMACOMPTABLE'],
        'CODECRYPTAGE': ReglementDepotGarantieApresDeblocage_info['CODECRYPTAGE'],
        'TYPEOPERATION': ReglementDepotGarantieApresDeblocage_info['TYPEOPERATION']
    }
    

    try:
        cursor = connection
        cursor.execute("EXEC dbo.PS_MICCREDITDEPOTGARANTIEAPRESDEBLOCAGENEW ?, ?, ?, ?, ?, ?, ?, ?, ?", list(params.values()))
        connection.commit()
        get_commit(connection,ReglementDepotGarantieApresDeblocage_info)
        #cursor.close()
    except Exception as e:
        connection.rollback()
        raise Exception(f"Erreur lors de la modification: {str(e.args[1])}")



def insert_pvgUpdateOP_AGENTCREDIT(connection, AGENTCREDIT_info):
    
    params = {
        'AG_CODEAGENCE': AGENTCREDIT_info['AG_CODEAGENCE'],
        'PV_CODEPOINTVENTE': '',
        'CR_CODECREDIT': AGENTCREDIT_info['CR_CODECREDIT'],
        'CR_NUMERODOSSIER': None,
        'TI_IDTIERS': '',
        'TI_IDTIERSINSTITUTION': '',
        'CO_CODECOMPTE': '',
        'OF_CODEOBJETFINANCEMENT':'',
        'PS_CODESOUSPRODUIT':'',
        'AT_CODEACTIVITE':'',
        'AC_CODEACTIVITE':'',
        'TO_CODETOMBEE'	:'', 
        'CL_IDCLIENT':'',
        'CR_DESCRIPTIONACTIVITE':'', 
        'CO_CODECOMMUNE':'',
        'CR_ADRESSEGEOGRAPHIQUEACTIVITE':'', 
        'CR_MONTANTCREDIT':'0',
        'CR_MONTCREDITACCORDE':'0',
        'CR_MONTANTGARANTIE':'0',
        'CR_MONTANTEPARGNE':'0',
        'CR_MONTANTASSURANCE':'0',
        'CR_DATEMISEENPLACE':parse_datetime('01/01/1900'),
        'CR_DATEPECHEANCE':parse_datetime('01/01/1900'),
        'CR_DATERETARD':parse_datetime('01/01/1900'),
        'CR_DATECONSOLIDATION':parse_datetime('01/01/1900'),
        'CR_CODECREDITAVANTCONSOLIDATION':'',
        'CR_DATEPROLONGATION':parse_datetime('01/01/1900'),
        'CR_CODECREDITAVANTPROLONGATION':'',
        'CR_DATESOLDE':parse_datetime('01/01/1900'),
        'CR_DATEREMBOURSEMENT':parse_datetime('01/01/1900'),
        'CR_TAUX':'0',
        'CR_DUREE':'0',
        'CR_DIFFERE':'0',
        'PE_CODEPERIODICITE':'',
        'TP_CODETYPEPAIEMENT':'',
        'TI_CODETYPEAMORTISSEMENT':'',
        'TD_CODETYPEDIFFERRE':'',
        'OP_AGENTCREDIT': AGENTCREDIT_info['OP_AGENTCREDIT'],
        'OP_CODEOPERATEUR': AGENTCREDIT_info['OP_CODEOPERATEUR'],
        'CODECRYPTAGE': AGENTCREDIT_info['CODECRYPTAGE'],
        'TYPEOPERATION': '5',
        'CR_CODECREDITRETOUR': ''
    }
    

    try:
        cursor = connection
        cursor.execute("EXEC dbo.PC_MICCREDIT ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? ", list(params.values()))
        connection.commit()
        get_commit(connection,AGENTCREDIT_info)
        #cursor.close()
    except Exception as e:
        connection.rollback()
        raise Exception(f"Erreur lors de la modification: {str(e.args[1])}")

def pvgChargerDansDataSetPourComboOP_AGENTCREDIT(connection, *vppCritere):
    cursor = connection.cursor()
    
    
    if len(vppCritere) > 1:
        # Critères et paramètres de la requête
        vapCritere = " WHERE AG_CODEAGENCE=? AND OP_AGENTCREDIT=? "
        vapNomParametre = ["CODECRYPTAGE", "AG_CODEAGENCE", "OP_AGENTCREDIT"]
        vapValeurParametre = [vppCritere[0], vppCritere[1], 'O'] # Note le tuple avec la virgule pour une seule valeur
    else:
        vapCritere = ""
        vapNomParametre = ()
        vapValeurParametre = ()
    
    
    vapRequete = f"""
    SELECT OP_CODEOPERATEUR AS OP_AGENTCREDIT, OP_NOMPRENOM 
    FROM dbo.FT_OPERATEUR(?) 
    """ + vapCritere + " AND OP_ACTIF='O' AND NOT (OP_LOGIN LIKE '%ADMIN%' OR OP_LOGIN LIKE '%BILAN%') ORDER BY OP_NOMPRENOM"
    
    try:
        cursor.execute(vapRequete, vapValeurParametre)
    except Exception as e:
        cursor.close()
        cursor.execute("ROLLBACK")
        MYSQL_REPONSE = f'Impossible d\'exécuter la requête : {str(e)}'
        raise Exception(MYSQL_REPONSE)
    
    try:
        rows = cursor.fetchall()
       
        results = []
        for row in rows:
            result = {}
            result['OP_CODEOPERATEUR'] = row[0]
            result['OP_NOMPRENOM'] = row[1]
            
            # Ajouter le dictionnaire à la liste des résultats
            results.append(result)
        # Retourne l'objet
        return results
    except Exception as e:
        cursor.close()
        cursor.execute("ROLLBACK")
        MYSQL_REPONSE = f'Impossible de récupérer les données : {str(e)}'
        raise Exception(MYSQL_REPONSE)
    

def pvgChargerDansDataSetPourComboParametretableaimporterouexporter(connection, *vppCritere):
    cursor = connection.cursor()
    
    if len(vppCritere) == 0:
        # Critères et paramètres de la requête
        vapCritere = " WHERE  ACTIF='O' "
        vapNomParametre = ()
        vapValeurParametre = () # Note le tuple avec la virgule pour une seule valeur
    elif len(vppCritere) == 1:
        # Critères et paramètres de la requête
        vapCritere = " WHERE CODEPARAMETRE=? AND ACTIF='O' "
        vapNomParametre = ["CODEPARAMETRE"]
        vapValeurParametre = [vppCritere[0]] # Note le tuple avec la virgule pour une seule valeur
    else:
        vapCritere = ""
        vapNomParametre = ()
        vapValeurParametre = ()
    
    
    vapRequete = f"""
    SELECT CODEPARAMETRE , LIBELLE FROM dbo.PARAMETRETABLEAIMPORTEROUEXPORTER 
    """ + vapCritere 
    
    try:
        cursor.execute(vapRequete, vapValeurParametre)
    except Exception as e:
        cursor.close()
        cursor.execute("ROLLBACK")
        MYSQL_REPONSE = f'Impossible d\'exécuter la requête : {str(e)}'
        raise Exception(MYSQL_REPONSE)
    
    try:
        rows = cursor.fetchall()
       
        results = []
        for row in rows:
            result = {}
            result['CODEPARAMETRE'] = row[0]
            result['LIBELLE'] = row[1]
            
            # Ajouter le dictionnaire à la liste des résultats
            results.append(result)
        # Retourne l'objet
        return results
    except Exception as e:
        cursor.close()
        cursor.execute("ROLLBACK")
        MYSQL_REPONSE = f'Impossible de récupérer les données : {str(e)}'
        raise Exception(MYSQL_REPONSE)



def parse_datetime(date_str):
    """Convertit une chaîne de caractères en datetime. Renvoie None si la conversion échoue."""
    if not date_str:
        return None
    
    # Liste des formats possibles
    date_formats = ["%d/%m/%Y","%d-%m-%Y", "%Y-%m-%d %H:%M:%S"]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    # Si aucun format ne correspond, lever une exception
    raise ValueError(f"Format de date invalide: {date_str}")

    
def get_commit(connection,clsBilletages):
    try:
       for row in clsBilletages: 
        cursor = connection
        params = {
            'AG_CODEAGENCE3': '1000',
            'MC_DATEPIECE3': '01/01/1900'
        }
        try:
            connection.commit()
            cursor.execute("EXECUTE [PC_COMMIT]  ?, ?", list(params.values()))
            #instruction pour valider la commande de mise à jour
            connection.commit()
        except Exception as e:
            # En cas d'erreur, annuler la transaction
            cursor.execute("ROLLBACK")
            cursor.close()
            MYSQL_REPONSE = e.args[1]
            if "varchar" in MYSQL_REPONSE:
               MYSQL_REPONSE = MYSQL_REPONSE.split("varchar", 1)[1].split("en type de donn", 1)[0]
               
            raise Exception(MYSQL_REPONSE)
    except Exception as e:
        MYSQL_REPONSE = f'Erreur lors du commit des opérations: {str(e)}'
        raise Exception(MYSQL_REPONSE)        
    
    
    
def get_ip_address():
    try:
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        print("Adresse IP locale : " + ip_address)
        return ip_address
    except Exception as e:
        #print("Erreur lors de la récupération de l'adresse IP : " + str(e))
        MYSQL_REPONSE = f'Erreur lors de la récupération de l adresse IP : {str(e)}'
        raise Exception(MYSQL_REPONSE) 

def get_public_ip_address():
    try:
        response = requests.get('http://icanhazip.com/')
        ip_address = response.text.strip()
        print("Adresse IP publique : " + ip_address)
        return ip_address
    except Exception as e:
        #print("Erreur lors de la récupération de l'adresse IP publique : " + str(e))
        MYSQL_REPONSE = f'Erreur lors de la récupération de l adresse IP publique : : {str(e)}'
        raise Exception(MYSQL_REPONSE) 

def get_mac_address():
    try:
        mac_address = ':'.join(['{:02x}'.format((uuid.getnode() >> i) & 0xff)
                                for i in range(0,8*6,8)][::-1])
        print("Adresse MAC : " + mac_address)
        return mac_address
    except Exception as e:
        #print("Erreur lors de la récupération de l'adresse MAC : " + str(e))
        MYSQL_REPONSE = f'Erreur lors de la récupération de l adresse MAC : {str(e)}'
        raise Exception(MYSQL_REPONSE)    