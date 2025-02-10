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


def insert_MICCOMPTEPRODUITSOUSPRODUITCOMPTEAUTORISEENOD(connection, produitcompteautoriseenod):
    # Préparation des paramètres
    params = {
        'PS_CODESOUSPRODUIT': produitcompteautoriseenod['PS_CODESOUSPRODUIT'],
        'PL_CODENUMCOMPTE': produitcompteautoriseenod['PL_CODENUMCOMPTE'],
        'CODECRYPTAGE': produitcompteautoriseenod['CODECRYPTAGE'],
        'TYPEOPERATION': 0  # 0 pour insertion
    }

    try:
        cursor = connection
        cursor.execute("EXEC dbo.PC_MICCOMPTEPRODUITSOUSPRODUITCOMPTEAUTORISEENOD ?, ?, ?, ?", list(params.values()))
        #connection.commit()
        
    except Exception as e:
        connection.rollback()
        raise Exception(f"Erreur lors de l'insertion de l'échéancier: {str(e)}")
    #finally:
    #    cursor.close()
    
#suppression
def delete_MICCOMPTEPRODUITSOUSPRODUITCOMPTEAUTORISEENOD(connection, produitcompteautoriseenod):
    # Préparation des paramètres
    params = {
        'PS_CODESOUSPRODUIT': produitcompteautoriseenod['PS_CODESOUSPRODUIT'],
        'PL_CODENUMCOMPTE':  produitcompteautoriseenod['PL_CODENUMCOMPTE'],
        'CODECRYPTAGE': produitcompteautoriseenod['CODECRYPTAGE'],
        'TYPEOPERATION': 3  # 2 pour suppression
    }

    try:
        cursor = connection
        cursor.execute("EXEC dbo.PC_MICCOMPTEPRODUITSOUSPRODUITCOMPTEAUTORISEENOD ?, ?, ?, ?", list(params.values()))
        connection.commit()
        get_commit(connection,produitcompteautoriseenod)
    except Exception as e:
        connection.rollback()
        raise Exception(f"Erreur lors de la suppression de l'échéancier: {str(e)}")
    #finally:
    #    cursor.close()
    
    
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