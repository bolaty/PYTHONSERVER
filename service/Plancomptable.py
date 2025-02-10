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



def List_pvgChargerDansDataSetCompteAutoriseEnODAvecProduit(connection, SO_CODESOCIETE, PS_CODESOUSPRODUIT, PL_TYPECOMPTE):
    """
    Récupère les données de la procédure SQL [PS_LISTEPRODUIT].
    
    :param connection: Connexion à la base de données SQL Server
    :param SO_CODESOCIETE: Nature du compte (varchar)
    :param PS_CODESOUSPRODUIT: Indicateur actif (varchar)
    :param PL_TYPECOMPTE: Type d'écran (varchar)
    :return: Liste de dictionnaires représentant les enregistrements de la table
    """
    try:
        cursor = connection.cursor()
        
        
        # Exécuter la procédure stockée avec le bon schéma (assure-toi que 'dbo' est le bon schéma)
        cursor.execute("EXEC dbo.PS_MICCOMPTEPRODUITSOUSPRODUITCOMPTEAUTORISEENODLISTENEW ?, ?, ?", SO_CODESOCIETE, PS_CODESOUSPRODUIT, PL_TYPECOMPTE)
        
        # Passer aux résultats (au cas où la procédure exécute plusieurs commandes)
        #cursor.nextset()
        # Récupérer les résultats
        rows = cursor.fetchall()
        results = []
        
        # Parcourir les lignes et les convertir en dictionnaires
        for row in rows:
            result = {
                'PS_CODESOUSPRODUIT': row.PS_CODESOUSPRODUIT,
                'PL_CODENUMCOMPTE': row.PL_CODENUMCOMPTE,
                'SO_CODESOCIETE': row.SO_CODESOCIETE,
                'PL_NUMCOMPTE': row.PL_NUMCOMPTE,
                'PL_LIBELLE': row.PL_LIBELLE,
                'PL_COMPTECOLLECTIF': row.PL_COMPTECOLLECTIF,
                'PL_REPORTDEBIT': row.PL_REPORTDEBIT,
                'PL_REPORTCREDIT': row.PL_REPORTCREDIT,
                'PL_MONTANTPERIODEPRECEDENTDEBIT': row.PL_MONTANTPERIODEPRECEDENTDEBIT,
                'PL_MONTANTPERIODEPRECEDENTCREDIT': row.PL_MONTANTPERIODEPRECEDENTCREDIT,
                'PL_MONTANTPERIODEDEBITENCOURS': row.PL_MONTANTPERIODEDEBITENCOURS,
                'PL_MONTANTPERIODECREDITENCOURS': row.PL_MONTANTPERIODECREDITENCOURS,
                'PL_MONTANTSOLDEFINALDEBIT': row.PL_MONTANTSOLDEFINALDEBIT,
                'PL_MONTANTSOLDEFINALCREDIT': row.PL_MONTANTSOLDEFINALCREDIT,
                'PL_SENS': row.PL_SENS,
                'PL_TYPECOMPTE': row.PL_TYPECOMPTE,
                'PL_COMPTERESULTATINSTANCE': row.PL_COMPTERESULTATINSTANCE,
                'PL_EXCEDENTEXERCICE': row.PL_EXCEDENTEXERCICE,
                'PL_DEFICITEXERCICE': row.PL_DEFICITEXERCICE,
                'PL_ACTIF': row.PL_ACTIF,
                'PL_AUTORISEINVERSION': row.PL_AUTORISEINVERSION,
                'PL_SAISIE_ANALYTIQUE': row.PL_SAISIE_ANALYTIQUE,
                'PL_COMPTEREFERENTIELCOMPTABLE': row.PL_COMPTEREFERENTIELCOMPTABLE,
                'COCHER': row.COCHER
            }
            results.append(result)
        
        return results
    
    except Exception as e:
        # Gérer les exceptions et retourner un message d'erreur approprié
        raise Exception(f"Erreur lors de la récupération des données: {str(e)}")
