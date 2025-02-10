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


def pvgChargerDansDataSetPourComboParametretableaimporterouexporters(connection, *vppCritere):
    cursor = connection.cursor()
    
    
    if len(vppCritere) > 0:
        # Critères et paramètres de la requête
        vapCritere = " WHERE CODEPARAMETRE=? "
        vapNomParametre = ["CODEPARAMETRE"]
        vapValeurParametre = [vppCritere[0]] # Note le tuple avec la virgule pour une seule valeur
    else:
        vapCritere = ""
        vapNomParametre = ()
        vapValeurParametre = ()
    
    
    vapRequete = f"""
    SELECT CODEPARAMETRE , LIBELLE 
    FROM dbo.PARAMETRETABLEAIMPORTEROUEXPORTER
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
    

def pvgChargerDansDataSetPourComboOP_AGENTCOLLECTEETCREDIT(connection, *vppCritere):
    cursor = connection.cursor()
    
    # Critères et paramètres de la requête
    vapCritere = " WHERE AG_CODEAGENCE=? AND  OP_AGENTDECOLLECTEETDECREDIT=? "
    vapNomParametre = ["CODECRYPTAGE", "AG_CODEAGENCE", "OP_AGENTDECOLLECTEETDECREDIT"]
    vapValeurParametre = [vppCritere[0],vppCritere[1],'O'] # Note le tuple avec la virgule pour une seule valeur
    
    
    vapRequete = f"""
    SELECT OP_CODEOPERATEUR AS OP_AGENTDECOLLECTEETDECREDIT , OP_NOMPRENOM FROM dbo.FT_OPERATEUR(?)
    """ + vapCritere + "  AND OP_ACTIF='O' AND NOT (OP_LOGIN like '%ADMIN%' OR OP_LOGIN like'%BILAN%') ORDER BY OP_NOMPRENOM"
    
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
            result['OP_AGENTDECOLLECTEETDECREDIT'] = row[0]
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

def pvgChargerDansDataSetPourComboEcranComptabilisationVirement(connection, *vppCritere):
    cursor = connection.cursor()
    
    
    if len(vppCritere) > 0:
        # Critères et paramètres de la requête
        vapCritere = " WHERE TS_AFFICHERECRANCOMPTABILISATION=? "
        vapNomParametre = ["TS_AFFICHERECRANCOMPTABILISATION"]
        vapValeurParametre = [vppCritere[0]] # Note le tuple avec la virgule pour une seule valeur
    else:
        vapCritere = ""
        vapNomParametre = ()
        vapValeurParametre = ()
    
    
    vapRequete = f"""
    SELECT TS_CODETYPESCHEMACOMPTABLE,TS_LIBELLE,TS_AFFICHERECRANCOMPTABILISATION FROM dbo.TYPESCHEMACOMPTABLE
    """ + vapCritere + " ORDER BY TS_LIBELLE" 
    
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
            result['TS_CODETYPESCHEMACOMPTABLE'] = row[0]
            result['TS_LIBELLE'] = row[1]
            result['TS_AFFICHERECRANCOMPTABILISATION'] = row[2]
            
            # Ajouter le dictionnaire à la liste des résultats
            results.append(result)
        # Retourne l'objet
        return results
    except Exception as e:
        cursor.close()
        cursor.execute("ROLLBACK")
        MYSQL_REPONSE = f'Impossible de récupérer les données : {str(e)}'
        raise Exception(MYSQL_REPONSE)


#creation de ...

def pvgInsertValidation(connection, credit_info):
    # Préparation des paramètres
    

    try:
        cursor = connection

        # Exécution de la procédure stockée
        cursor.execute("""
            EXECUTE PC_MICSALAIREIMPORTECREDITVALIDER 
                @SL_IDINDEX=?, 
                @AG_CODEAGENCE=?, 
                @OP_CODEOPERATEUR=?, 
                @CL_NUMEROCOMPTE=?, 
                @CO_CODECOMPTE=?, 
                @CL_IDCLIENT=?, 
                @PL_CODENUMCOMPTE=?, 
                @PS_CODESOUSPRODUIT=?, 
                @PV_CODEPOINTVENTE=?, 
                @PV_RAISONSOCIAL=?, 
                @TS_CODETYPESCHEMACOMPTABLE=?, 
                @CODEPARAMETRE=?, 
                @AT_CODEACTIVITE=?, 
                @TI_CODETYPEAMORTISSEMENT=?, 
                @PE_CODEPERIODICITE=?, 
                @CR_NUMERODOSSIER=?, 
                @CR_DATEDEBLOCAGE=?, 
                @CR_DATEPECHEANCE=?, 
                @CR_NOMBREECHEANCE=?, 
                @CR_TAUX=?, 
                @CR_DUREE=?, 
                @CR_MONTCREDITACCORDE=?, 
                @CR_MONTANTINTERETATTENDU=?, 
                @CR_LIBELLEOPERATION=?, 
                @CR_NOMCLIENT=?, 
                @CR_PRENOMCLIENT=?, 
                @CR_TELEPHONE=?, 
                @SL_TYPEOPERATION=?, 
                @CR_DATEOPERATION=?, 
                @CODECRYPTAGE=?, 
                @TYPEOPERATION=?, 
                @CR_FRAISDOSSIER=?, 
                @CR_FONDASSURANCE=?, 
                @CR_FRAISETUDE=?, 
                @CR_COMMISSIONENGAGEMENT=?, 
                @CR_ASSURENCEDECESINVALIDITE=?, 
                @CR_ASSURENCEFACULTATIVE=?, 
                @CR_MONTANTGARENTIE=?, 
                @CR_EPARGNE=?, 
                @TO_CODETOMBEE=?, 
                @CR_FRAISASSISTANCEETFORMATION=?
        """, (
            credit_info['SL_IDINDEX'], 
            credit_info['AG_CODEAGENCE'], 
            credit_info['OP_CODEOPERATEUR'], 
            credit_info['CL_NUMEROCOMPTE'], 
            credit_info['CO_CODECOMPTE'], 
            credit_info['CL_IDCLIENT'], 
            None, 
            credit_info['PS_CODESOUSPRODUIT'], 
            credit_info['PV_CODEPOINTVENTE'], 
            credit_info['PV_RAISONSOCIAL'], 
            credit_info['TS_CODETYPESCHEMACOMPTABLE'], 
            credit_info['CODEPARAMETRE'], 
            credit_info['AT_CODEACTIVITE'], 
            credit_info['TI_CODETYPEAMORTISSEMENT'], 
            credit_info['PE_CODEPERIODICITE'], 
            credit_info['CR_NUMERODOSSIER'], 
            credit_info['CR_DATEDEBLOCAGE'], 
            credit_info['CR_DATEPECHEANCE'], 
            credit_info['CR_NOMBREECHEANCE'], 
            credit_info['CR_TAUX'], 
            credit_info['CR_DUREE'], 
            credit_info['CR_MONTCREDITACCORDE'], 
            credit_info['CR_MONTANTINTERETATTENDU'], 
            credit_info['CR_LIBELLEOPERATION'], 
            credit_info['CR_NOMCLIENT'], 
            credit_info['CR_PRENOMCLIENT'], 
            credit_info['CR_TELEPHONE'], 
            'Z', 
            credit_info['CR_DATEOPERATION'], 
            credit_info['CODECRYPTAGE'], 
            0, 
            credit_info['CR_FRAISDOSSIER'], 
            credit_info['CR_FONDASSURANCE'], 
            credit_info['CR_FRAISETUDE'], 
            credit_info['CR_COMMISSIONENGAGEMENT'], 
            credit_info['CR_ASSURENCEDECESINVALIDITE'], 
            credit_info['CR_ASSURENCEFACULTATIVE'], 
            credit_info['CR_MONTANTGARENTIE'], 
            credit_info['CR_EPARGNE'], 
            None, 
            credit_info['CR_FRAISASSISTANCEETFORMATION']
        ))

        
        # Si la procédure s'exécute correctement
        #connection.commit()

    except Exception as e:
        Retour = {}
        Retour['SL_MESSAGE'] = str(e.args[1])
        if "[SQL Server]" in Retour['SL_MESSAGE']:
            Retour['SL_MESSAGE'] = Retour['SL_MESSAGE'].split("[SQL Server]", 1)[1].split(".", 1)[0]
        Retour['SL_RESULTAT'] = "FALSE"
        return Retour
    #finally:
    #    cursor.close()


def pvgDelete1(connection, *vppCritere):
    cursor = connection
    op_codeoperateur = int(vppCritere[1])
    
    # Critères et paramètres de la requête
    vapCritere = " WHERE OP_CODEOPERATEUR=? "
    vapNomParametre = ["OP_CODEOPERATEUR"]
    vapValeurParametre = [op_codeoperateur] # Note le tuple avec la virgule pour une seule valeur
    
    
    vapRequete = f"""
    DELETE FROM  MICSALAIREIMPORTE
    """ + vapCritere 
    
    try:
        cursor.execute(vapRequete, vapValeurParametre)
    except Exception as e:
        
        Retour = {}
        Retour['SL_MESSAGE'] = str(e.args[1])
        if "[SQL Server]" in Retour['SL_MESSAGE']:
            Retour['SL_MESSAGE'] = Retour['SL_MESSAGE'].split("[SQL Server]", 1)[1].split(".", 1)[0]
        Retour['SL_RESULTAT'] = "FALSE"
        
        return Retour
        
    
    try:
        '''
        results = []
        result = {}
        result['AG_CODEAGENCE3'] = '1000'
        result['MC_DATEPIECE3'] = '01/01/1900'
            
            # Ajouter le dictionnaire à la liste des résultats
        results.append(result)
        connection.commit()
        get_commit(connection,results)
        Retour = {}
        Retour['SL_MESSAGE'] = "Operations successful"
        Retour['SL_RESULTAT'] = "TRUE"
        return Retour
        '''
    except Exception as e:
        cursor.close()
        cursor.execute("ROLLBACK")
        Retour = {}
        Retour['SL_MESSAGE'] = str(e.args[1])
        if "[SQL Server]" in Retour['SL_MESSAGE']:
            Retour['SL_MESSAGE'] = Retour['SL_MESSAGE'].split("[SQL Server]", 1)[1].split(".", 1)[0]
        Retour['SL_RESULTAT'] = "FALSE"
        return Retour
        
        #MYSQL_REPONSE = f'Impossible de récupérer les données : {str(e)}'
        #raise Exception(MYSQL_REPONSE)

def pvgDeleteMicsalaireimportezedtachesautomatique(connection, *vppCritere):
    cursor = connection
    
    
    # Critères et paramètres de la requête
    vapCritere = " WHERE AG_CODEAGENCE=? "
    vapNomParametre = ["AG_CODEAGENCE"]
    vapValeurParametre = [vppCritere[0]] # Note le tuple avec la virgule pour une seule valeur
    
    
    vapRequete = f"""
    DELETE FROM  MICSALAIREIMPORTEZEDTACHESAUTOMATIQUE
    """ + vapCritere 
    
    try:
        cursor.execute(vapRequete, vapValeurParametre)
    except Exception as e:
        
        Retour = {}
        Retour['SL_MESSAGE'] = str(e.args[1])
        if "[SQL Server]" in Retour['SL_MESSAGE']:
            Retour['SL_MESSAGE'] = Retour['SL_MESSAGE'].split("[SQL Server]", 1)[1].split(".", 1)[0]
        Retour['SL_RESULTAT'] = "FALSE"
        
        return Retour
        
    
    try:
        results = []
        result = {}
        result['AG_CODEAGENCE3'] = '1000'
        result['MC_DATEPIECE3'] = '01/01/1900'
            
            # Ajouter le dictionnaire à la liste des résultats
        results.append(result)
        connection.commit()
        get_commit(connection,results)
        Retour = {}
        Retour['SL_MESSAGE'] = "Operations successful"
        Retour['SL_RESULTAT'] = "TRUE"
        return Retour
    except Exception as e:
        cursor.close()
        cursor.execute("ROLLBACK")
        Retour = {}
        Retour['SL_MESSAGE'] = str(e.args[1])
        if "[SQL Server]" in Retour['SL_MESSAGE']:
            Retour['SL_MESSAGE'] = Retour['SL_MESSAGE'].split("[SQL Server]", 1)[1].split(".", 1)[0]
        Retour['SL_RESULTAT'] = "FALSE"
        return Retour
        
        #MYSQL_REPONSE = f'Impossible de récupérer les données : {str(e)}'
        #raise Exception(MYSQL_REPONSE)

def pvgInsert(connection, credit_infos):
    # Préparation des paramètres
    

    try:
        cursor = connection.cursor()
        for credit_info in credit_infos:
            # Exécution de la procédure stockée EXECUTE PC_MICSALAIREIMPORTECREDITVALIDER
            cursor.execute("""
            EXECUTE PC_MICSALAIREIMPORTECREDITWEB
                @SL_IDINDEX=?, 
                @AG_CODEAGENCE=?, 
                @OP_CODEOPERATEUR=?, 
                @CL_NUMEROCOMPTE=?, 
                @CO_CODECOMPTE=?, 
                @CL_IDCLIENT=?, 
                @PL_CODENUMCOMPTE=?, 
                @PS_CODESOUSPRODUIT=?, 
                @PV_CODEPOINTVENTE=?, 
                @PV_RAISONSOCIAL=?, 
                @TS_CODETYPESCHEMACOMPTABLE=?, 
                @CODEPARAMETRE=?, 
                @AT_CODEACTIVITE=?, 
                @TI_CODETYPEAMORTISSEMENT=?, 
                @PE_CODEPERIODICITE=?, 
                @CR_NUMERODOSSIER=?, 
                @CR_DATEDEBLOCAGE=?, 
                @CR_DATEPECHEANCE=?, 
                @CR_NOMBREECHEANCE=?, 
                @CR_TAUX=?, 
                @CR_DUREE=?, 
                @CR_MONTCREDITACCORDE=?, 
                @CR_MONTANTINTERETATTENDU=?, 
                @CR_LIBELLEOPERATION=?, 
                @CR_NOMCLIENT=?, 
                @CR_PRENOMCLIENT=?, 
                @CR_TELEPHONE=?, 
                @SL_TYPEOPERATION=?, 
                @CR_DATEOPERATION=?, 
                @CODECRYPTAGE=?, 
                @TYPEOPERATION=?, 
                @CR_FRAISDOSSIER=?, 
                @CR_FONDASSURANCE=?, 
                @CR_FRAISETUDE=?, 
                @CR_COMMISSIONENGAGEMENT=?, 
                @CR_ASSURENCEDECESINVALIDITE=?, 
                @CR_ASSURENCEFACULTATIVE=?, 
                @CR_MONTANTGARENTIE=?, 
                @CR_EPARGNE=?, 
                @TO_CODETOMBEE=?, 
                @FRAISASSITANCEDEFORMATION=?
            """, (
                credit_info['SL_IDINDEX'], 
                credit_info['AG_CODEAGENCE'], 
                credit_info['OP_CODEOPERATEUR'], 
                credit_info['CL_NUMEROCOMPTE'], 
                credit_info['CO_CODECOMPTE'], 
                credit_info['CL_IDCLIENT'], 
                None, 
                credit_info['PS_CODESOUSPRODUIT'], 
                credit_info['PV_CODEPOINTVENTE'], 
                credit_info['PV_RAISONSOCIAL'], 
                credit_info['TS_CODETYPESCHEMACOMPTABLE'], 
                credit_info['CODEPARAMETRE'], 
                credit_info['AT_CODEACTIVITE'], 
                credit_info['TI_CODETYPEAMORTISSEMENT'], 
                credit_info['PE_CODEPERIODICITE'], 
                credit_info['CR_NUMERODOSSIER'], 
                credit_info['CR_DATEDEBLOCAGE'], 
                credit_info['CR_DATEPECHEANCE'], 
                credit_info['CR_NOMBREECHEANCE'], 
                credit_info['CR_TAUX'], 
                credit_info['CR_DUREE'], 
                credit_info['CR_MONTCREDITACCORDE'], 
                credit_info['CR_MONTANTINTERETATTENDU'], 
                credit_info['CR_LIBELLEOPERATION'], 
                credit_info['CR_NOMCLIENT'], 
                credit_info['CR_PRENOMCLIENT'], 
                credit_info['CR_TELEPHONE'], 
                "Z",
                credit_info['CR_DATEOPERATION'], 
                credit_info['CODECRYPTAGE'], 
                0,
                credit_info['CR_FRAISDOSSIER'], 
                credit_info['CR_FONDASSURANCE'], 
                credit_info['CR_FRAISETUDE'], 
                credit_info['CR_COMMISSIONENGAGEMENT'], 
                credit_info['CR_ASSURENCEDECESINVALIDITE'], 
                credit_info['CR_ASSURENCEFACULTATIVE'], 
                credit_info['CR_MONTANTGARENTIE'], 
                credit_info['CR_EPARGNE'], 
                None, 
                credit_info['CR_FRAISASSISTANCEETFORMATION']
            ))
            # Validation de la transaction
            #connection.commit()
        get_commit(cursor,credit_infos)
        
    except Exception as e:
        connection.rollback()
        Retour = {}
        Retour['SL_MESSAGE'] = str(e.args[1])
        if "[SQL Server]" in Retour['SL_MESSAGE']:
            Retour['SL_MESSAGE'] = Retour['SL_MESSAGE'].split("[SQL Server]", 1)[1].split(".", 1)[0]
        Retour['SL_RESULTAT'] = "FALSE"
        return Retour# Annule les changements en cas d'erreur
        #raise Exception(f"Erreur lors de l'exécution de la procédure : {str(e)}")
    
    #finally:
    #    cursor.close()  # Assure la fermeture du curseur



def pvgChargerDansDataSetMICSALAIREIMPORTECREDIT(connection, CODECRYPTAGE,AG_CODEAGENCE,OP_CODEOPERATEUR,CODEPARAMETRE):
    cursor = connection.cursor()
    
    
    cursor.execute("SELECT *  FROM dbo.FT_MICSALAIREIMPORTECREDIT(?,?,?,?)",(AG_CODEAGENCE,OP_CODEOPERATEUR,CODEPARAMETRE,CODECRYPTAGE))
    
    try:
        rows = cursor.fetchall()
       
        
        results = []
        for row in rows:
            result = {}
            result['SL_IDINDEX'] = row.SL_IDINDEX
            result['OP_CODEOPERATEUR'] = row.OP_CODEOPERATEUR
            result['CL_NUMEROCOMPTE'] = row.CL_NUMEROCOMPTE
            result['CO_CODECOMPTE'] = row.CO_CODECOMPTE
            result['CL_IDCLIENT'] = row.CL_IDCLIENT
            result['PL_CODENUMCOMPTE'] = row.PL_CODENUMCOMPTE
            result['PS_CODESOUSPRODUIT'] = row.PS_CODESOUSPRODUIT
            result['PV_CODEPOINTVENTE'] = row.PV_CODEPOINTVENTE
            result['PV_RAISONSOCIAL'] = row.PV_RAISONSOCIAL
            result['TS_CODETYPESCHEMACOMPTABLE'] = row.TS_CODETYPESCHEMACOMPTABLE
            result['CODEPARAMETRE'] = row.CODEPARAMETRE
            result['AT_CODEACTIVITE'] = row.AT_CODEACTIVITE
            result['TYPECREDIT'] = row.TYPECREDIT
            result['TI_CODETYPEAMORTISSEMENT'] = row.TI_CODETYPEAMORTISSEMENT
            result['TI_LIBELLE'] = row.TI_LIBELLE
            result['PE_CODEPERIODICITE'] = row.PE_CODEPERIODICITE
            result['PE_LIBELLE'] = row.PE_LIBELLE
            result['CR_NUMERODOSSIER'] = row.CR_NUMERODOSSIER
            result['CR_DATEDEBLOCAGE'] = row.CR_DATEDEBLOCAGE.strftime("%d/%m/%Y") if row.CR_DATEDEBLOCAGE else None
            result['CR_DATEPECHEANCE'] = row.CR_DATEPECHEANCE.strftime("%d/%m/%Y") if row.CR_DATEPECHEANCE else None
            result['CR_NOMBREECHEANCE'] = row.CR_NOMBREECHEANCE
            result['CR_TAUX'] = row.CR_TAUX
            result['CR_DUREE'] = row.CR_DUREE
            result['CR_MONTCREDITACCORDE'] = row.CR_MONTCREDITACCORDE
            result['CR_MONTANTINTERETATTENDU'] = row.CR_MONTANTINTERETATTENDU
            result['CR_FONDASSURANCE'] = row.CR_FONDASSURANCE
            result['CR_FRAISDOSSIER'] = row.CR_FRAISDOSSIER
            result['CR_LIBELLEOPERATION'] = row.CR_LIBELLEOPERATION
            result['CR_NOMCLIENT'] = row.CR_NOMCLIENT
            result['CR_PRENOMCLIENT'] = row.CR_PRENOMCLIENT
            result['NOMETPRENOM'] = row.NOMETPRENOM
            result['CR_TELEPHONE'] = row.CR_TELEPHONE
            result['SL_TYPEOPERATION'] = row.SL_TYPEOPERATION
            result['CR_DATEOPERATION'] = row.CR_DATEOPERATION.strftime("%d/%m/%Y") if row.CR_DATEOPERATION else None
            result['CR_FRAISETUDE'] = row.CR_FRAISETUDE
            result['CR_COMMISSIONENGAGEMENT'] = row.CR_COMMISSIONENGAGEMENT
            result['CR_ASSURENCEDECESINVALIDITE'] = row.CR_ASSURENCEDECESINVALIDITE
            result['CR_ASSURENCEFACULTATIVE'] = row.CR_ASSURENCEFACULTATIVE
            result['CR_MONTANTGARENTIE'] = row.CR_MONTANTGARENTIE
            result['CR_EPARGNE'] = row.CR_EPARGNE
            result['TO_CODETOMBEE'] = row.TO_CODETOMBEE
            # Ajouter le dictionnaire à la liste des résultats
            results.append(result)
        
        
        return results
    except Exception as e:
        cursor.close()
        cursor.execute("ROLLBACK")
        MYSQL_REPONSE = f'Impossible de récupérer les données : {str(e)}'
        raise Exception(MYSQL_REPONSE)




def pvgChargerDansDataSet4(connection, CODECRYPTAGE, AT_LIBELLE):
    """
    Récupère les données de la procédure SQL [PS_LISTEPRODUIT].
    
    :param connection: Connexion à la base de données SQL Server
    :param CODECRYPTAGE: Nature du compte (varchar)
    :param AT_LIBELLE: Indicateur actif (varchar)
    :return: Liste de dictionnaires représentant les enregistrements de la table
    """
    try:
        cursor = connection.cursor()
        
        
        
        # Exécuter la procédure stockée avec le bon schéma (assure-toi que 'dbo' est le bon schéma)
        cursor.execute("EXEC dbo.PS_MICCREDITPARAMETREACTIVITE1 ?, ?", AT_LIBELLE, CODECRYPTAGE)
        
        # Passer aux résultats (au cas où la procédure exécute plusieurs commandes)
        cursor.nextset()
        # Récupérer les résultats
        rows = cursor.fetchall()
        results = []
        
        # Parcourir les lignes et les convertir en dictionnaires
        for row in rows:
            result = {
                'AT_CODEACTIVITE': row.AT_CODEACTIVITE,
                'AT_LIBELLE': row.AT_LIBELLE,
                'TA_CODETYPEACTIVITE': row.TA_CODETYPEACTIVITE,
                'TI_CODETYPEAMORTISSEMENT': row.TI_CODETYPEAMORTISSEMENT,
                'AT_TAUXMINIMUM': row.AT_TAUXMINIMUM,
                'AT_TAUXPARDEFAUT': row.AT_TAUXPARDEFAUT,
                'AT_TAUXMAXIMUM': row.AT_TAUXMAXIMUM,
                'AT_MONTANTMINIMUM': row.AT_MONTANTMINIMUM,
                'AT_MONTANTMAXIMUM': row.AT_MONTANTMAXIMUM,
                'AT_MONTANTEPARGNEMINIMUM': row.AT_MONTANTEPARGNEMINIMUM,
                'AT_MONTANTEPARGNEOBLIGATOIRE': row.AT_MONTANTEPARGNEOBLIGATOIRE,
                'AT_MONTANTEPARGNEMAXIMUM': row.AT_MONTANTEPARGNEMAXIMUM,
                'AT_TAUXASSURANCE': row.AT_TAUXASSURANCE,
                'AT_MONTANTASSURANCEMINIMUM': row.AT_MONTANTASSURANCEMINIMUM,
                'AT_MONTANTASSURANCEMAXIMUM': row.AT_MONTANTASSURANCEMAXIMUM,
                'AT_DUREEMINIMUM': row.AT_DUREEMINIMUM,
                'AT_DUREEMAXIMUM': row.AT_DUREEMAXIMUM,
                'AT_DIFFEREMINIMUM': row.AT_DIFFEREMINIMUM,
                'AT_DIFFEREMAXIMUM': row.AT_DIFFEREMAXIMUM,
                'AT_MONTANTMAXIMUMCREDITLIESALAIRENET': row.AT_MONTANTMAXIMUMCREDITLIESALAIRENET,
                'AT_MONTANTMAXIMUMCREDITLIEALEPARGNE': row.AT_MONTANTMAXIMUMCREDITLIEALEPARGNE,
                'AT_DUREEMINIMUMANCIENNETE': row.AT_DUREEMINIMUMANCIENNETE,
                'AT_NOMBREMINIMUMMEMBRE': row.AT_NOMBREMINIMUMMEMBRE,
                'AT_NOMBREMAXIMUMMEMBRE': row.AT_NOMBREMAXIMUMMEMBRE,
                'AT_TAUXPENALITEMINIMUM': row.AT_TAUXPENALITEMINIMUM,
                'AT_TAUXPENALITEMAXIMUM': row.AT_TAUXPENALITEMAXIMUM,
                'AT_TABLEAUAMORTISSEMENTAUTOMATIQUE': row.AT_TABLEAUAMORTISSEMENTAUTOMATIQUE,
                'AT_NUMEROORDRE': row.AT_NUMEROORDRE,
                'TA_LIBELLE': row.TA_LIBELLE,
                'TI_LIBELLE': row.TI_LIBELLE,
                'AT_TAUXDEPOTGARANTIEMINIMUM': row.AT_TAUXDEPOTGARANTIEMINIMUM,
                'AT_TAUXDEPOTGARANTIEPARDEFAUT': row.AT_TAUXDEPOTGARANTIEPARDEFAUT,
                'AT_TAUXDEPOTGARANTIEMAXIMUM': row.AT_TAUXDEPOTGARANTIEMAXIMUM,
                'AT_AGEMINIMUM': row.AT_AGEMINIMUM,
                'AT_AGEMAXIMUM': row.AT_AGEMAXIMUM,
                'AT_ACTIF': row.AT_ACTIF,
                'AT_FRAISOBLIGATOIREAVANTDEBLOCAGE': row.AT_FRAISOBLIGATOIREAVANTDEBLOCAGE,
                'AT_REGLERENTIEREMENTFRAISAVANTDEBLOCAGE': row.AT_REGLERENTIEREMENTFRAISAVANTDEBLOCAGE,
                'AT_AUTORISATIONMODIFICATIONFRAISCREDITORDINAIRE': row.AT_AUTORISATIONMODIFICATIONFRAISCREDITORDINAIRE,
                'AT_AUTORISATIONMODIFICATIONFRAISCREDITIMMOBILISE': row.AT_AUTORISATIONMODIFICATIONFRAISCREDITIMMOBILISE,
                'AT_CODESOUSPRODUITEPARGNESURCREDIT': row.AT_CODESOUSPRODUITEPARGNESURCREDIT,
                'AT_CODESOUSPRODUITFRAISFIXESURCREDIT': row.AT_CODESOUSPRODUITFRAISFIXESURCREDIT
            }
            results.append(result)
        
        return results
    
    except Exception as e:
        # Gérer les exceptions et retourner un message d'erreur approprié
        raise Exception(f"Erreur lors de la récupération des données: {str(e)}")






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

def pvgDeleteValide(connection, *vppCritere):
    cursor = connection
    
    # Critères et paramètres de la requête
    vapCritere = ""
    vapNomParametre = ()
    vapValeurParametre = ()
    
    
    vapRequete = f"""
    DELETE FROM  MICSALAIREIMPORTECREDITFOSAT
    """ 
    
    try:
        cursor.execute(vapRequete, vapValeurParametre)
    except Exception as e:
        cursor.close()
        cursor.execute("ROLLBACK")
        MYSQL_REPONSE = f'Impossible d\'exécuter la requête : {str(e)}'
        raise Exception(MYSQL_REPONSE)
    
    #try:
    #    results = []
    #    result = {}
    #    result['AG_CODEAGENCE3'] = '1000'
    #    result['MC_DATEPIECE3'] = '01/01/1900'
            
    #        # Ajouter le dictionnaire à la liste des résultats
    #    results.append(result)
    #    connection.commit()
    #    get_commit(connection,results)
        
    except Exception as e:
        cursor.close()
        cursor.execute("ROLLBACK")
        MYSQL_REPONSE = f'Impossible de récupérer les données : {str(e)}'
        raise Exception(MYSQL_REPONSE)

def pvgDelete(connection, *vppCritere):
    cursor = connection.cursor()
    
    # Critères et paramètres de la requête
    vapCritere = ""
    vapNomParametre = ()
    vapValeurParametre = ()
    
    
    vapRequete = f"""
    DELETE FROM  MICSALAIREIMPORTECREDITFOSAT
    """ 
    
    try:
        cursor.execute(vapRequete, vapValeurParametre)
    except Exception as e:
        cursor.close()
        cursor.execute("ROLLBACK")
        MYSQL_REPONSE = f'Impossible d\'exécuter la requête : {str(e)}'
        raise Exception(MYSQL_REPONSE)
    
    #try:
    #    results = []
    #    result = {}
    #    result['AG_CODEAGENCE3'] = '1000'
    #    result['MC_DATEPIECE3'] = '01/01/1900'
            
    #        # Ajouter le dictionnaire à la liste des résultats
    #    results.append(result)
    #    connection.commit()
    #    get_commit(connection,results)
        
    except Exception as e:
        cursor.close()
        cursor.execute("ROLLBACK")
        MYSQL_REPONSE = f'Impossible de récupérer les données : {str(e)}'
        raise Exception(MYSQL_REPONSE)
   
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