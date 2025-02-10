from flask import Blueprint,request, jsonify,current_app
from service.comptabilisation import pvgComptabilisationVersement
from service.Signature import insertion_signaturepad,mise_a_jour_signaturepad,suppression_signaturepad,List_signaturepad
from service.Miccompteproduitsousproduit import List_compte_produit
from service.Plancomptable import List_pvgChargerDansDataSetCompteAutoriseEnODAvecProduit
from service.Miccompteproduitsousproduitcompteautorisenod import insert_MICCOMPTEPRODUITSOUSPRODUITCOMPTEAUTORISEENOD,delete_MICCOMPTEPRODUITSOUSPRODUITCOMPTEAUTORISEENOD
from service.Credit import pvgChargerDansDataSetPourComboParametretableaimporterouexporter, pvgChargerDansDataSetPourComboOP_AGENTCREDIT,insert_pvgUpdateOP_AGENTCREDIT,insert_RemboursementCreditEnContentieuxAvecDepotGarantie,insert_pvgMicreditReglementDepotGarantieApresDeblocage
from service.Comptabilite import pvgDeleteMicsalaireimportezedtachesautomatique,pvgChargerDansDataSet4,pvgDelete,pvgDeleteValide,pvgInsert,pvgChargerDansDataSetPourComboParametretableaimporterouexporters,pvgChargerDansDataSetPourComboOP_AGENTCOLLECTEETCREDIT,pvgChargerDansDataSetPourComboEcranComptabilisationVirement,pvgInsertValidation,pvgDelete1,pvgChargerDansDataSetMICSALAIREIMPORTECREDIT

from models.models import clsObjetEnvoi
from utils import connect_database
from datetime import datetime
from config import MYSQL_REPONSE
import random
import string
api_bp = Blueprint('api', __name__)

@api_bp.route('/pvgAjouterComptabilisation', methods=['POST'])
def OperationVersementRetrait():
    # Récupérer les données du corps de la requête
    
    request_data = request.json
    # Extraire les données nécessaires pour l'appel à la fonction
    Objet = request_data['Objet']
   # billetages = request_data['Objet']['clsBilletages']
   
    clsEtatmouvementacomptabiliserss = []
    clsBilletagess = []
    
   # clsObjetEnvoi.OE_A = request_data['Objet'].clsObjetEnvoi.OE_A
   ## clsObjetEnvoi.OE_Y = request_data['Objet'].clsObjetEnvoi.OE_Y
    
    for row in request_data['Objet']:
        clsEtatmouvementacomptabilisers = {}
        clsObjetEnvoi.OE_A = row['clsObjetEnvoi']['OE_A']
        clsObjetEnvoi.OE_Y = row['clsObjetEnvoi']['OE_Y']
        clsObjetEnvoi.OE_J = row['clsObjetEnvoi']['OE_J']
        clsObjetEnvoi.OE_U = row['clsObjetEnvoi']['OE_U']
        clsObjetEnvoi.OE_G = row['clsObjetEnvoi']['OE_G']
        clsObjetEnvoi.OE_T = row['clsObjetEnvoi']['OE_T']
        clsEtatmouvementacomptabilisers['AG_CODEAGENCE'] = row['AG_CODEAGENCE']
        clsEtatmouvementacomptabilisers['CO_CODECOMPTE'] = row['CO_CODECOMPTE1']
        clsEtatmouvementacomptabilisers['MC_DATEPIECE'] = row['MC_DATEPIECE']
        clsEtatmouvementacomptabilisers['MC_LIBELLEOPERATION'] = row['MC_LIBELLEOPERATION']
        clsEtatmouvementacomptabilisers['MC_MONTANTDEBIT'] = row['MC_MONTANTDEBIT']
        clsEtatmouvementacomptabilisers['MC_NOMTIERS'] = row['MC_NOMTIERS']
       # clsEtatmouvementacomptabilisers['EM_NUMEROSEQUENCE'] = row['EM_NUMEROSEQUENCE']
        clsEtatmouvementacomptabilisers['MC_NUMPIECETIERS'] = row['MC_NUMPIECETIERS']#row['MC_NUMPIECE']
        clsEtatmouvementacomptabilisers['MC_REFERENCEPIECE'] = ""#row['MC_REFERENCEPIECE']
       # clsEtatmouvementacomptabilisers['EM_SCHEMACOMPTABLECODE'] = row['EM_SCHEMACOMPTABLECODE']
        clsEtatmouvementacomptabilisers['MC_SENSBILLETAGE'] = row['MC_SENSBILLETAGE']
        clsEtatmouvementacomptabilisers['TI_IDTIERS'] = row['TI_IDTIERS']
        clsEtatmouvementacomptabilisers['OP_CODEOPERATEUR'] = row['OP_CODEOPERATEUR']
        clsEtatmouvementacomptabilisers['PI_CODEPIECE'] = row['PI_CODEPIECE']
        clsEtatmouvementacomptabilisers['PL_CODENUMCOMPTE'] = row['PL_CODENUMCOMPTE']
        clsEtatmouvementacomptabilisers['PV_CODEPOINTVENTE'] = row['PV_CODEPOINTVENTE']
        clsEtatmouvementacomptabilisers['MC_TERMINAL'] = ""
        clsEtatmouvementacomptabilisers['MC_AUTRE1'] = row['MC_REFERENCEPIECE']
        clsEtatmouvementacomptabilisers['TS_CODETYPESCHEMACOMPTABLE'] = row['TS_CODETYPESCHEMACOMPTABLE']
        clsEtatmouvementacomptabiliserss.append(clsEtatmouvementacomptabilisers)
    
    # Parcourir les éléments de la liste Objet
    for objet in request_data['Objet']:
        # Vérifier si l'élément contient la clé 'clsBilletages'
        if 'clsBilletages' in objet:
            # Récupérer la liste des billetages
            billetages = objet['clsBilletages']

            # Parcourir les billetages en utilisant une boucle for
            if billetages is not None:
                for row in billetages:
                    clsBilletages = {}
                    clsBilletages['AG_CODEAGENCE'] = row['AG_CODEAGENCE']
                    clsBilletages['BI_NUMPIECE'] = row['BI_NUMPIECE']
                    clsBilletages['BI_NUMSEQUENCE'] = row['BI_NUMSEQUENCE']
                    clsBilletages['BI_NUMSEQUENCE'] = row['BI_NUMSEQUENCE']
                    clsBilletages['BI_QUANTITEENTREE'] = row['BI_QUANTITEENTREE']
                    clsBilletages['BI_QUANTITESORTIE'] = row['BI_QUANTITESORTIE']
                    clsBilletages['CB_CODECOUPURE'] = row['CB_CODECOUPURE']
                    clsBilletages['MC_DATEPIECE'] = row['MC_DATEPIECE']
                    clsBilletages['MC_NUMPIECE'] = row['MC_NUMPIECE']
                    clsBilletages['MC_NUMSEQUENCE'] = row['MC_NUMSEQUENCE']
                    clsBilletages['PL_CODENUMCOMPTE'] = row['PL_CODENUMCOMPTE']
                    clsBilletages['TYPEOPERATION'] = row['TYPEOPERATION']
                    clsBilletagess.append(clsBilletages)
    
   
    # Récupérer la connexion à la base de données depuis current_app
   # db_connection = current_app.db_connection
    
   # db_connection.begin()
    #try:
        db_connection = connect_database()
        db_connection = db_connection.cursor()
        db_connection.execute("BEGIN TRANSACTION")
        #db_connection.begin()
         # Appeler la fonction avec les données récupérées
        response = pvgComptabilisationVersement(db_connection, clsEtatmouvementacomptabiliserss, clsBilletagess, clsObjetEnvoi)
        
        # Retourner la réponse au client
        if response['SL_RESULTAT'] == "TRUE":
            #db_connection.close()
            return jsonify({"NUMEROBORDEREAUREGLEMENT":str(response['NUMEROBORDEREAU']),"SL_MESSAGE":"Comptabilisation éffectuée avec success !!! / " + response['MESSAGEAPI'] ,"SL_RESULTAT": 'TRUE'}) 
        else:
            #db_connection.close()
            return jsonify({"SL_MESSAGE":response['SL_MESSAGE'] ,"SL_RESULTAT": 'FALSE'}) 
    #except Exception as e:
       # En cas d'erreur, annuler la transaction
        #db_connection.execute("ROLLBACK")
        #db_connection.close()
        #return jsonify({"SL_MESSAGE":str(e),"SL_RESULTAT": 'FALSE'})
        #return jsonify({"SL_MESSAGE":str(e),"SL_RESULTAT": 'FALSE'})    


@api_bp.route('/pvgAjouterComptabilisationtest', methods=['POST'])
def OperationVersementRetraittest():
    # Récupérer les données du corps de la requête
    
    return jsonify({"NUMEROBORDEREAUREGLEMENT":"verstest000215256","SL_MESSAGE":"Comptabilisation éffectuée avec success !!! / "  ,"SL_RESULTAT": 'TRUE'})   


@api_bp.route('/insertion_signaturepad', methods=['POST'])
def pvginsertion_signaturepad():
    request_data = request.json
    SG_TOKENSIGNATURE = generer_code_aleatoire()
    # Vérification que l'objet est bien présent dans la requête
    if 'Objet' not in request_data:
        return jsonify({"SL_MESSAGE": "Données manquantes. Code erreur (300) : voir le noeud Objet", "SL_RESULTAT": 'FALSE'})
    
    for row in request_data['Objet']:
        signature_info = {}

        # Validation et récupération des données pour l'insertion ou la modification
        if signature_info['TYPEOPERATION'] == '0' or signature_info['TYPEOPERATION'] == '1':
            signature_info['AG_CODEAGENCE'] = str(row.get('AG_CODEAGENCE', ''))
            signature_info['CODECRYPTAGE'] = str(row.get('CODECRYPTAGE', ''))
            signature_info['CL_EMAIL'] = str(row.get('CL_EMAIL', ''))
            signature_info['SG_CODESIGNATURE'] = str(row.get('SG_CODESIGNATURE', ''))
            signature_info['OP_CODEOPERATEUR'] = str(row.get('OP_CODEOPERATEUR', ''))
            signature_info['CL_IDCLIENT'] = str(row.get('CL_IDCLIENT', ''))
            signature_info['EJ_IDEPARGNANTJOURNALIER'] = str(row.get('EJ_IDEPARGNANTJOURNALIER', ''))
            signature_info['SG_DATESIGNATURE'] = str(row.get('SG_DATESIGNATURE', ''))
            signature_info['SG_TOKENSIGNATURE'] = SG_TOKENSIGNATURE
            signature_info['SG_NOMSIGNATURE'] = str(row.get('SG_NOMSIGNATURE', ''))
            signature_info['SG_STATUTSIGNATURE'] = str(row.get('SG_STATUTSIGNATURE', ''))
            signature_info['NT_CODENATURESIGNATUREPAD'] = str(row.get('NT_CODENATURESIGNATUREPAD', ''))
            signature_info['TYPEOPERATION'] = str(row.get('TYPEOPERATION', ''))
            if signature_info['TYPEOPERATION'] == '1' :
               signature_info['SIGNATURE'] = str(row.get('SIGNATURE', ''))
               signature_info['SG_CODESIGNATURE'] = str(row.get('SG_CODESIGNATURE', ''))
               
        if signature_info['TYPEOPERATION'] == '2' :
            signature_info['SG_CODESIGNATURE'] = str(row.get('SG_CODESIGNATURE', ''))
            signature_info['CODECRYPTAGE'] = str(row.get('CODECRYPTAGE', ''))
        # Vérification que toutes les données obligatoires sont présentes pour l'insertion ou la modification
        
        if signature_info['TYPEOPERATION'] == '0':  # Insertion
            if not all([signature_info['AG_CODEAGENCE'], signature_info['SG_DATESIGNATURE'],signature_info['SG_DATESIGNATURE'],signature_info['OP_CODEOPERATEUR'],  signature_info['NT_CODENATURESIGNATUREPAD'], signature_info['SG_TOKENSIGNATURE'], signature_info['SG_TOKENSIGNATURE']]):
               return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes. Code erreur (301)", "SL_RESULTAT": 'FALSE'})
        elif signature_info['TYPEOPERATION'] == '1':  # Mise à jour
            if not all([signature_info['SG_CODESIGNATURE']]):
               return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes. Code erreur (301)", "SL_RESULTAT": 'FALSE'})
        elif signature_info['TYPEOPERATION'] == '2':  # Suppression
            if not all([signature_info['SG_CODESIGNATURE']]):
               return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes. Code erreur (301)", "SL_RESULTAT": 'FALSE'})
        
        
        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                # Insertion, modification ou suppression en fonction du type d'opération
                if signature_info['TYPEOPERATION'] == '0':  # Insertion
                    insertion_signaturepad(db_connection, signature_info)
                elif signature_info['TYPEOPERATION'] == '1':  # Mise à jour
                    mise_a_jour_signaturepad(db_connection, signature_info)
                elif signature_info['TYPEOPERATION'] == '2':  # Suppression
                    suppression_signaturepad(db_connection, signature_info)
                
                # Valider la transaction
                db_connection.commit()
                
            if signature_info['TYPEOPERATION'] == '1':
              return jsonify({"SL_MESSAGE": "Signature reçue et enregistrée!", "SL_RESULTAT": 'TRUE'})
            if signature_info['TYPEOPERATION'] == '0':
              return jsonify({"SL_MESSAGE": "Opération réussie!", "SL_RESULTAT": 'TRUE'})
        
        except Exception as e:
            db_connection.rollback()
            return jsonify({"SL_MESSAGE": f"Erreur lors de l'opération : {str(e)}", "SL_RESULTAT": 'FALSE'})
        
        finally:
            db_connection.close()

@api_bp.route('/List_signaturepad', methods=['POST'])
def pvgList_signaturepad():
    request_data = request.json
    
    if 'Objet' not in request_data:
        return jsonify({"SL_MESSAGE": "Données manquantes.code erreur (300) voir le noeud Objet", "SL_RESULTAT": 'FALSE'})

    for row in request_data['Objet']:
        signature_info = {}
        
        # Validation et récupération des données pour la suppression
        signature_info['SG_CODESIGNATURE'] = row.get('SG_CODESIGNATURE') if 'SG_CODESIGNATURE' in signature_info and signature_info['SG_CODESIGNATURE'] else None
        signature_info['NT_CODENATURESIGNATUREPAD'] = row.get('NT_CODENATURESIGNATUREPAD') if 'NT_CODENATURESIGNATUREPAD' in signature_info and signature_info['NT_CODENATURESIGNATUREPAD'] else None #if 'CU_CODECOMPTEUTULISATEUR' in documentcontrat_info and documentcontrat_info['CU_CODECOMPTEUTULISATEUR'] else None
        signature_info['SG_NOMSIGNATURE'] = row.get('SG_NOMSIGNATURE') 
        signature_info['SG_TOKENSIGNATURE'] = row.get('SG_TOKENSIGNATURE')
        signature_info['TYPEOPERATION'] = row.get('TYPEOPERATION')
        signature_info['CODECRYPTAGE'] = row.get('CODECRYPTAGE')
        # Vérification que toutes les données obligatoires sont présentes
        
        if not all([signature_info['SG_CODESIGNATURE']]):
           return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes.code erreur (301) SG_CODESIGNATURE", "SL_RESULTAT": 'FALSE'}), 200
       
        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                # Appeler la fonction de liste selon les criteres ou récupération
                response = List_signaturepad(db_connection,str(row.get('SG_CODESIGNATURE', '')),str(row.get('NT_CODENATURESIGNATUREPAD', '')),str(row.get('SG_NOMSIGNATURE', '')),str(row.get('SG_TOKENSIGNATURE', '')),str(row.get('CODECRYPTAGE', '')), str(row.get('TYPEOPERATION', '')))
                
                if response and response[0].get('SG_CODESIGNATURE'):
                    cursor.execute("COMMIT")
                    return jsonify({"SL_MESSAGE": "Opération effectuée avec succès !!!", "SL_RESULTAT": 'TRUE'}, response)
                else:
                    cursor.execute("ROLLBACK")
                    return jsonify({"SL_MESSAGE": "Contrat non trouvé ou autre erreur.", "SL_RESULTAT": 'FALSE'})
        
        except Exception as e:
            db_connection.rollback()
            return jsonify({"SL_MESSAGE": "Erreur lors du chargement : " + str(e), "SL_RESULTAT": 'FALSE'})
        
        finally:
            db_connection.close() 




################################################################
#                  GESTION COMPTE PRODUIT                      #
################################################################

@api_bp.route('/pvgChargerDansDataSetListeProduit', methods=['POST'])
def ChargerDansDataSetListeProduit():
    request_data = request.json
    
    if 'Objet' not in request_data:
        return jsonify({"SL_MESSAGE": "Données manquantes.code erreur (300) voir le noeud Objet", "SL_RESULTAT": 'FALSE'})

    for row in request_data['Objet']:
        user_info = {}

        # Validation et récupération des données pour la suppression
        user_info['NC_CODENATURECOMPTE'] = row.get('NC_CODENATURECOMPTE')
        user_info['PS_ACTIF'] = row.get('PS_ACTIF')
        user_info['TYPEECRAN'] = row.get('TYPEECRAN')

        # Vérification que toutes les données obligatoires sont présentes
        if not all([user_info['NC_CODENATURECOMPTE'], user_info['PS_ACTIF'], user_info['TYPEECRAN']]):
            return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes.code erreur (301)", "SL_RESULTAT": 'FALSE'})

        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                # Appeler la fonction de suppression ou récupération
                response = List_compte_produit(db_connection,str(row.get('NC_CODENATURECOMPTE', '')),str(row.get('PS_ACTIF', '')),str(row.get('TYPEECRAN', '')))
                
                if response and response[0].get('PS_CODESOUSPRODUIT'):
                    return jsonify({"SL_MESSAGE": "Opération effectuée avec succès !!!", "SL_RESULTAT": 'TRUE'}, response)
                else:
                    cursor.execute("ROLLBACK")
                    return jsonify({"SL_MESSAGE": "Utilisateur non trouvé ou autre erreur.", "SL_RESULTAT": 'FALSE'})
        
        except Exception as e:
            db_connection.rollback()
            return jsonify({"SL_MESSAGE": "Erreur lors de la suppression : " + str(e), "SL_RESULTAT": 'FALSE'})
        
        finally:
            db_connection.close()



################################################################
#                  FIN GESTION COMPTE PRODUIT                  #
################################################################

################################################################
#                  GESTION PLANCOMPTABLE                       #
################################################################

@api_bp.route('/pvgChargerDansDataSetCompteAutoriseEnODAvecProduit', methods=['POST'])
def ChargerDansDataSetCompteAutoriseEnODAvecProduit():
    request_data = request.json
    
    if 'Objet' not in request_data:
        return jsonify({"SL_MESSAGE": "Données manquantes.code erreur (300) voir le noeud Objet", "SL_RESULTAT": 'FALSE'})

    for row in request_data['Objet']:
        user_info = {}

        # Validation et récupération des données pour la suppression
        user_info['SO_CODESOCIETE'] = row.get('SO_CODESOCIETE')
        user_info['PS_CODESOUSPRODUIT'] = row.get('PS_CODESOUSPRODUIT')
        user_info['PL_TYPECOMPTE'] = row.get('PL_TYPECOMPTE')

        # Vérification que toutes les données obligatoires sont présentes
        if not all([user_info['SO_CODESOCIETE'], user_info['PS_CODESOUSPRODUIT'], user_info['PL_TYPECOMPTE']]):
            return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes.code erreur (301)", "SL_RESULTAT": 'FALSE'})

        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                # Appeler la fonction de suppression ou récupération
                response = List_pvgChargerDansDataSetCompteAutoriseEnODAvecProduit(db_connection,str(row.get('SO_CODESOCIETE', '')),str(row.get('PS_CODESOUSPRODUIT', '')),str(row.get('PL_TYPECOMPTE', '')))
                
                if response and response[0].get('PS_CODESOUSPRODUIT'):
                    return jsonify({"SL_MESSAGE": "Opération effectuée avec succès !!!", "SL_RESULTAT": 'TRUE'}, response)
                else:
                    cursor.execute("ROLLBACK")
                    return jsonify({"SL_MESSAGE": "Utilisateur non trouvé ou autre erreur.", "SL_RESULTAT": 'FALSE'})
        
        except Exception as e:
            db_connection.rollback()
            return jsonify({"SL_MESSAGE": "Erreur lors de la suppression : " + str(e), "SL_RESULTAT": 'FALSE'})
        
        finally:
            db_connection.close()



################################################################
#                  FIN GESTION PLANCOMPTABLE                  #
################################################################

################################################################
#  GESTION DES MICCOMPTEPRODUITSOUSPRODUITCOMPTEAUTORISEENOD   #
################################################################
@api_bp.route('/pvginsert_produitcompteautoriseenod', methods=['POST'])
def insert_produitcompteautoriseenod():
        # Récupérer les données du corps de la requête
        request_data = request.json
        produitcompteautoriseenods = []
        if 'Objet' not in request_data:
            return jsonify({"SL_MESSAGE": "Données manquantes.code erreur (300) voir le noeud Objet", "SL_RESULTAT": 'FALSE'})
        
        for row in request_data['Objet']:
            produitcompteautoriseenod = {}

            try:
                # Validation des données
                if str(row.get('COCHER', '')) == 'O':
                    produitcompteautoriseenod['PS_CODESOUSPRODUIT'] = str(row.get('PS_CODESOUSPRODUIT', ''))
                    produitcompteautoriseenod['PL_CODENUMCOMPTE'] = str(row.get('PL_CODENUMCOMPTE', ''))
                    produitcompteautoriseenod['CODECRYPTAGE'] = str(row.get('CODECRYPTAGE', ''))
                    produitcompteautoriseenods.append(produitcompteautoriseenod)
            except ValueError as e:
                # Retourner un message d'erreur en cas de problème de type de données
                return jsonify({"SL_MESSAGE": f"Erreur de type de données : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
            
            except Exception as e:
                # Retourner un message d'erreur en cas d'exception générale
                return jsonify({"SL_MESSAGE": f"Erreur inattendue : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
            
            # Vérification que toutes les données obligatoires sont présentes
            if not all([produitcompteautoriseenod['PS_CODESOUSPRODUIT'], produitcompteautoriseenod['PL_CODENUMCOMPTE'], 
                        produitcompteautoriseenod['CODECRYPTAGE']]):
                return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes.code erreur (301)", "SL_RESULTAT": 'FALSE'}), 200
            
        # Connexion à la base de données
        db_connection = connect_database()
        db_connection = db_connection.cursor()
        db_connection.execute("BEGIN TRANSACTION")
        
        #try:
            
        #    with db_connection.cursor() as cursor:
        #        cursor.execute("BEGIN TRANSACTION")
        #delete_MICCOMPTEPRODUITSOUSPRODUITCOMPTEAUTORISEENOD(db_connection, produitcompteautoriseenod)
        for produitcompteautoriseenod in produitcompteautoriseenods:
                # Appeler la fonction d'insertion dans la base de données
                delete_MICCOMPTEPRODUITSOUSPRODUITCOMPTEAUTORISEENOD(db_connection, produitcompteautoriseenod)
        for produitcompteautoriseenod in produitcompteautoriseenods:
                # Appeler la fonction d'insertion dans la base de données
                insert_MICCOMPTEPRODUITSOUSPRODUITCOMPTEAUTORISEENOD(db_connection, produitcompteautoriseenod)
                
        
        # Valider la transaction
        get_commit(db_connection,produitcompteautoriseenods)
                
        return jsonify({"SL_MESSAGE": "Insertion réussie!", "SL_RESULTAT": 'TRUE'})
        
        #except Exception as e:
        #    db_connection.rollback()
        #    return jsonify({"SL_MESSAGE": f"Erreur lors de l'insertion : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
        
        #finally:
        #    db_connection.close()
################################################################
#  GESTION DES MICCOMPTEPRODUITSOUSPRODUITCOMPTEAUTORISEENOD   #
################################################################


################################################################
#                          GESTION DES CREDIT                  #
################################################################

@api_bp.route('/pvgRemboursementCreditEnContentieuxAvecDepotGarantie', methods=['POST'])
def RemboursementCreditEnContentieuxAvecDepotGarantie():
    # Récupérer les données du corps de la requête
    request_data = request.json
    
    for row in request_data['Objet']:
        RemboursementCreditEnContentieux_info = {}

        try:
            # Validation des chaînes de caractères
            RemboursementCreditEnContentieux_info['AG_CODEAGENCE'] = str(row.get('AG_CODEAGENCE', ''))
            RemboursementCreditEnContentieux_info['PV_CODEPOINTVENTE'] = str(row.get('PV_CODEPOINTVENTE', ''))
            RemboursementCreditEnContentieux_info['CR_CODECREDIT'] = str(row.get('CR_CODECREDIT', ''))
            
            RemboursementCreditEnContentieux_info['CR_MONTANTGARANTIE'] = float(row.get('CR_MONTANTGARANTIE', 0.0))
            
            RemboursementCreditEnContentieux_info['DATEJOURNEE'] = parse_datetime(row.get('DATEJOURNEE'))
            
            RemboursementCreditEnContentieux_info['OP_CODEOPERATEUR'] = str(row.get('OP_CODEOPERATEUR', ''))
            RemboursementCreditEnContentieux_info['CODECRYPTAGE'] = str(row.get('CODECRYPTAGE', ''))
            RemboursementCreditEnContentieux_info['MC_TERMINAL'] = '',#str(row.get('MC_TERMINAL', ''))
            RemboursementCreditEnContentieux_info['MC_AUTRE1'] = str(row.get('MC_AUTRE1', ''))
            RemboursementCreditEnContentieux_info['MC_AUTRE2'] = str(row.get('MC_AUTRE2', ''))
            RemboursementCreditEnContentieux_info['MC_AUTRE3'] = str(row.get('MC_AUTRE3', ''))
            
        
        except ValueError as e:
            # Retourner un message d'erreur en cas de problème de type de données
            return jsonify({"SL_MESSAGE": f"Erreur de type de données : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
        
        except Exception as e:
            # Retourner un message d'erreur en cas d'exception générale
            return jsonify({"SL_MESSAGE": f"Erreur inattendue : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
        
        # Vérification que toutes les données obligatoires sont présentes
        if not all([RemboursementCreditEnContentieux_info['CR_CODECREDIT'], RemboursementCreditEnContentieux_info['CR_MONTANTGARANTIE'] is not None, 
                    RemboursementCreditEnContentieux_info['DATEJOURNEE']]):
            return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes.code erreur (301)", "SL_RESULTAT": 'FALSE'}), 200    
            
        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection:
                #cursor.execute("BEGIN TRANSACTION")
                db_connection = db_connection.cursor()
                db_connection.execute("BEGIN TRANSACTION")
                # Appeler la fonction d'insertion dans la base de données
                insert_RemboursementCreditEnContentieuxAvecDepotGarantie(db_connection, RemboursementCreditEnContentieux_info)
                
                # Valider la transaction
                db_connection.commit()
                
            return jsonify({"SL_MESSAGE": "Opération éffectuée avec succes !!!", "SL_RESULTAT": 'TRUE'})
        
        except Exception as e:
            db_connection.rollback()
            MYSQL_REPONSE = e.args[0]
            if "varchar" in MYSQL_REPONSE:
              MYSQL_REPONSE = MYSQL_REPONSE.split("varchar", 1)[1].split("en type de donn", 1)[0]
            return jsonify({"SL_MESSAGE": f" {str(MYSQL_REPONSE)}", "SL_RESULTAT": 'FALSE'}), 200
        
        
        finally:
            db_connection.close()

#insert_pvgMicreditReglementDepotGarantieApresDeblocage

@api_bp.route('/pvgMicreditReglementDepotGarantieApresDeblocage', methods=['POST'])
def MicreditReglementDepotGarantieApresDeblocage():
    # Récupérer les données du corps de la requête
    request_data = request.json
    
    for row in request_data['Objet']:
        RemboursementCreditEnContentieux_info = {}

        try:
            # Validation des chaînes de caractères
            RemboursementCreditEnContentieux_info['AG_CODEAGENCE'] = str(row.get('AG_CODEAGENCE', ''))
            RemboursementCreditEnContentieux_info['PV_CODEPOINTVENTE'] = str(row.get('PV_CODEPOINTVENTE', ''))
            RemboursementCreditEnContentieux_info['CR_CODECREDIT'] = str(row.get('CR_CODECREDIT', ''))
            
            RemboursementCreditEnContentieux_info['CR_MONTANTGARANTIE'] = float(row.get('CR_MONTANTGARANTIE', 0.0))
            
            RemboursementCreditEnContentieux_info['DATEJOURNEE'] = parse_datetime(row.get('DATEJOURNEE'))
            
            RemboursementCreditEnContentieux_info['OP_CODEOPERATEUR'] = str(row.get('OP_CODEOPERATEUR', ''))
            RemboursementCreditEnContentieux_info['TS_CODETYPESCHEMACOMPTABLE'] = str(row.get('TS_CODETYPESCHEMACOMPTABLE', ''))
            RemboursementCreditEnContentieux_info['CODECRYPTAGE'] = str(row.get('CODECRYPTAGE', ''))
            RemboursementCreditEnContentieux_info['TYPEOPERATION'] = str(row.get('TYPEOPERATION', ''))
            
            
            
        except ValueError as e:
            # Retourner un message d'erreur en cas de problème de type de données
            return jsonify({"SL_MESSAGE": f"Erreur de type de données : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
        
        except Exception as e:
            # Retourner un message d'erreur en cas d'exception générale
            return jsonify({"SL_MESSAGE": f"Erreur inattendue : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
        
        # Vérification que toutes les données obligatoires sont présentes
        if not all([RemboursementCreditEnContentieux_info['CR_CODECREDIT'], RemboursementCreditEnContentieux_info['CR_MONTANTGARANTIE'], 
                    RemboursementCreditEnContentieux_info['TS_CODETYPESCHEMACOMPTABLE']]):
            return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes.code erreur (301)", "SL_RESULTAT": 'FALSE'}), 200    
            
        
        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection:
                #cursor.execute("BEGIN TRANSACTION")
                db_connection = db_connection.cursor()
                db_connection.execute("BEGIN TRANSACTION")
                # Appeler la fonction d'insertion dans la base de données
                insert_pvgMicreditReglementDepotGarantieApresDeblocage(db_connection, RemboursementCreditEnContentieux_info)
                
                # Valider la transaction
                db_connection.commit()
                
            return jsonify({"SL_MESSAGE": "Opération éffectuée avec succes !!!", "SL_RESULTAT": 'TRUE'})
        
        except Exception as e:
            db_connection.rollback()
            MYSQL_REPONSE = e.args[0]
            if "varchar" in MYSQL_REPONSE:
              MYSQL_REPONSE = MYSQL_REPONSE.split("varchar", 1)[1].split("en type de donn", 1)[0]
            return jsonify({"SL_MESSAGE": f"Erreur lors de l'insertion : {str(MYSQL_REPONSE)}", "SL_RESULTAT": 'FALSE'}), 200
        
        finally:
            db_connection.close()


@api_bp.route('/pvgUpdateOP_AGENTCREDIT', methods=['POST'])
def UpdateOP_AGENTCREDIT():
    # Récupérer les données du corps de la requête
    request_data = request.json
    
    for row in request_data['Objet']:
        AGENTCREDIT_info = {}

        try:
            # Validation des chaînes de caractères
            AGENTCREDIT_info['AG_CODEAGENCE'] = str(row.get('AG_CODEAGENCE', ''))
            AGENTCREDIT_info['CR_CODECREDIT'] = str(row.get('CR_CODECREDIT', ''))
            AGENTCREDIT_info['OP_AGENTCREDIT'] = str(row.get('OP_AGENTCREDIT', ''))
            AGENTCREDIT_info['OP_CODEOPERATEUR'] = str(row.get('OP_CODEOPERATEUR', ''))
            AGENTCREDIT_info['CODECRYPTAGE'] = str(row.get('CODECRYPTAGE', ''))
            
            
        except ValueError as e:
            # Retourner un message d'erreur en cas de problème de type de données
            return jsonify({"SL_MESSAGE": f"Erreur de type de données : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
        
        except Exception as e:
            # Retourner un message d'erreur en cas d'exception générale
            return jsonify({"SL_MESSAGE": f"Erreur inattendue : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
        
        # Vérification que toutes les données obligatoires sont présentes
        if not all([AGENTCREDIT_info['CR_CODECREDIT'], AGENTCREDIT_info['OP_AGENTCREDIT'], 
                    AGENTCREDIT_info['OP_CODEOPERATEUR']]):
            return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes.code erreur (301)", "SL_RESULTAT": 'FALSE'}), 200    
            
        
        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection:
                #cursor.execute("BEGIN TRANSACTION")
                db_connection = db_connection.cursor()
                db_connection.execute("BEGIN TRANSACTION")
                # Appeler la fonction d'insertion dans la base de données
                insert_pvgUpdateOP_AGENTCREDIT(db_connection, AGENTCREDIT_info)
                
                # Valider la transaction
                db_connection.commit()
                
            return jsonify({"SL_MESSAGE": "Opération éffectuée avec succes !!!", "SL_RESULTAT": 'TRUE'})
        
        except Exception as e:
            db_connection.rollback()
            return jsonify({"SL_MESSAGE": f"Erreur lors de l'insertion : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
        
        finally:
            db_connection.close()


@api_bp.route('/pvgChargerDansDataSetPourComboOP_AGENTCREDIT', methods=['POST'])
def ChargerDansDataSetPourComboOP_AGENTCREDIT():
    request_data = request.json
    
    for row in request_data['Objet']:
        useragent_info = {}

        # Validation et récupération des données pour la suppression
        useragent_info['CODECRYPTAGE'] = str(row.get('CODECRYPTAGE', '')) 
        useragent_info['AG_CODEAGENCE'] = str(row.get('AG_CODEAGENCE', '')) 
        useragent_info['OP_AGENTCREDIT'] = str(row.get('OP_AGENTCREDIT', '')) 
        
        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                # Appeler la fonction de suppression
                if useragent_info['OP_AGENTCREDIT']:  # Si une valeur est fournie
                    response = pvgChargerDansDataSetPourComboOP_AGENTCREDIT(db_connection, str(row.get('CODECRYPTAGE', '')),str(row.get('AG_CODEAGENCE', '')),str(row.get('OP_AGENTCREDIT', '')))
                else:  # Sinon, on appelle sans paramètre
                    response = pvgChargerDansDataSetPourComboOP_AGENTCREDIT(db_connection)
                
                
            if response[0]['OP_NOMPRENOM'] is not None:
                return jsonify({"SL_MESSAGE": "Opération éffectuée avec succès !!!", "SL_RESULTAT": 'TRUE'},response)
            else:
                return jsonify({"SL_MESSAGE": response['SL_MESSAGE'], "SL_RESULTAT": 'FALSE'})
        
        except Exception as e:
            db_connection.rollback()
            return jsonify({"SL_MESSAGE": "Erreur lors de la suppression : " + str(e), "SL_RESULTAT": 'FALSE'})
        
        finally:
            db_connection.close()

#pvgChargerDansDataSetPourComboParametretableaimporterouexporter
@api_bp.route('/pvgChargerParametretableaimporterouexportercombo', methods=['POST'])
def ChargerValeur():
    request_data = request.json
    
    for row in request_data['Objet']:
        Valeur_info = {}

        # Validation et récupération des données pour la suppression
        Valeur_info['CODEPARAMETRE'] = str(row.get('CODEPARAMETRE', '')) 
        
        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                # Appeler la fonction de suppression
                if Valeur_info['CODEPARAMETRE']:  # Si une valeur est fournie
                    response = pvgChargerDansDataSetPourComboParametretableaimporterouexporter(db_connection, str(row.get('CODEPARAMETRE', '')))
                else:  # Sinon, on appelle sans paramètre
                    response = pvgChargerDansDataSetPourComboParametretableaimporterouexporter(db_connection)
                
                
            if len(response) > 0 :
                return jsonify({"SL_MESSAGE": "Opération éffectuée avec succès !!!", "SL_RESULTAT": 'TRUE'},response)
            else:
                return jsonify({"SL_MESSAGE": response['SL_MESSAGE'], "SL_RESULTAT": 'FALSE'})
        
        except Exception as e:
            db_connection.rollback()
            return jsonify({"SL_MESSAGE": "Erreur lors de la suppression : " + str(e), "SL_RESULTAT": 'FALSE'})
        
        finally:
            db_connection.close()


@api_bp.route('/pvgInsert', methods=['POST'])
def Insert():
    request_data = request.json
    recuperation_infos = []
    SUPPRIMERDONNEES = ""
    if 'Objet' not in request_data:
            return jsonify({"SL_MESSAGE": "Données manquantes.code erreur (300) voir le noeud Objet", "SL_RESULTAT": 'FALSE'})
        
    for row in request_data['Objet']:
            recuperation_info = {}

            try:
                # Validation des données
                # Récupération des valeurs en les convertissant correctement
                SUPPRIMERDONNEES = str(row.get('SUPPRIMERDONNEES', ''))
                recuperation_info['SL_IDINDEX'] = str(row.get('SL_IDINDEX', ''))
                recuperation_info['AG_CODEAGENCE'] = str(row.get('AG_CODEAGENCE', ''))
                recuperation_info['OP_CODEOPERATEUR'] = str(row.get('OP_CODEOPERATEUR', ''))
                recuperation_info['CL_NUMEROCOMPTE'] = str(row.get('CL_NUMEROCOMPTE', ''))
                recuperation_info['CO_CODECOMPTE'] = str(row.get('CO_CODECOMPTE', ''))
                recuperation_info['CL_IDCLIENT'] = str(row.get('CL_IDCLIENT', ''))
                recuperation_info['PL_CODENUMCOMPTE'] = str(row.get('PL_CODENUMCOMPTE', ''))
                recuperation_info['PS_CODESOUSPRODUIT'] = str(row.get('PS_CODESOUSPRODUIT', ''))
                recuperation_info['PV_CODEPOINTVENTE'] = str(row.get('PV_CODEPOINTVENTE', ''))
                recuperation_info['PV_RAISONSOCIAL'] = str(row.get('PV_RAISONSOCIAL', ''))
                recuperation_info['TS_CODETYPESCHEMACOMPTABLE'] = str(row.get('TS_CODETYPESCHEMACOMPTABLE', ''))
                recuperation_info['CODEPARAMETRE'] = str(row.get('CODEPARAMETRE', ''))
                recuperation_info['AT_CODEACTIVITE'] = str(row.get('AT_CODEACTIVITE', ''))
                recuperation_info['TI_CODETYPEAMORTISSEMENT'] = str(row.get('TI_CODETYPEAMORTISSEMENT', ''))
                recuperation_info['PE_CODEPERIODICITE'] = str(row.get('PE_CODEPERIODICITE', ''))
                recuperation_info['CR_NUMERODOSSIER'] = str(row.get('CR_NUMERODOSSIER', ''))
                recuperation_info['CR_DATEDEBLOCAGE'] = parse_datetime(row.get('CR_DATEDEBLOCAGE')) if row.get('CR_DATEDEBLOCAGE') else None
                recuperation_info['CR_DATEPECHEANCE'] = parse_datetime(row.get('CR_DATEPECHEANCE')) if row.get('CR_DATEPECHEANCE') else None
                recuperation_info['CR_NOMBREECHEANCE'] = int(row.get('CR_NOMBREECHEANCE', 0))
                recuperation_info['CR_TAUX'] = float(row.get('CR_TAUX', 0.0))
                recuperation_info['CR_DUREE'] = int(row.get('CR_DUREE', 0))
                recuperation_info['CR_MONTCREDITACCORDE'] = float(row.get('CR_MONTCREDITACCORDE', 0.0))
                recuperation_info['CR_MONTANTINTERETATTENDU'] = float(row.get('CR_MONTANTINTERETATTENDU', 0.0))
                recuperation_info['CR_LIBELLEOPERATION'] = str(row.get('CR_LIBELLEOPERATION', ''))
                recuperation_info['CR_NOMCLIENT'] = str(row.get('CR_NOMCLIENT', ''))
                recuperation_info['CR_PRENOMCLIENT'] = str(row.get('CR_PRENOMCLIENT', ''))
                recuperation_info['CR_TELEPHONE'] = str(row.get('CR_TELEPHONE', ''))
                recuperation_info['SL_TYPEOPERATION'] = str(row.get('SL_TYPEOPERATION', ''))
                recuperation_info['CR_DATEOPERATION'] = parse_datetime(row.get('CR_DATEOPERATION')) if row.get('CR_DATEOPERATION') else None
                recuperation_info['CODECRYPTAGE'] = str(row.get('CODECRYPTAGE', ''))
                recuperation_info['CR_FRAISDOSSIER'] = float(row.get('CR_FRAISDOSSIER', 0.0))
                recuperation_info['CR_FONDASSURANCE'] = float(row.get('CR_FONDASSURANCE', 0.0))
                recuperation_info['CR_COMMISSIONAVANCECHEQUE'] = float(row.get('CR_COMMISSIONAVANCECHEQUE', 0.0))
                recuperation_info['CR_COMMISSIONENGAGEMENT'] = float(row.get('CR_COMMISSIONENGAGEMENT', 0.0))
                recuperation_info['CR_ASSURENCEDECESINVALIDITE'] = float(row.get('CR_ASSURENCEDECESINVALIDITE', 0.0))
                recuperation_info['CR_COMMISSIONAVANCESALAIRE'] = float(row.get('CR_COMMISSIONAVANCESALAIRE', 0.0))
                recuperation_info['CR_MONTANTGARENTIE'] = float(row.get('CR_MONTANTGARENTIE', 0.0))
                recuperation_info['CR_ASSURENCEFACULTATIVE'] = float(row.get('CR_ASSURENCEFACULTATIVE', 0.0))
                
                recuperation_info['CR_EPARGNE'] = float(row.get('CR_EPARGNE', 0.0))
                recuperation_info['TO_CODETOMBEE'] = str(row.get('TO_CODETOMBEE', ''))
                recuperation_info['CR_FRAISASSISTANCEETFORMATION'] = float(row.get('CR_FRAISASSISTANCEETFORMATION', 0.0))
                recuperation_info['CR_FRAISETUDE'] = float(row.get('CR_FRAISETUDE', 0.0))
                recuperation_info['FRAISASSITANCEDEFORMATION'] = 0.0
                
                recuperation_infos.append(recuperation_info)
            except ValueError as e:
                # Retourner un message d'erreur en cas de problème de type de données
                return jsonify({"SL_MESSAGE": f"Erreur de type de données : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
            
            except Exception as e:
                # Retourner un message d'erreur en cas d'exception générale
                return jsonify({"SL_MESSAGE": f"Erreur inattendue : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
            
            # Vérification que toutes les données obligatoires sont présentes
            if not all([recuperation_info['AG_CODEAGENCE'], 
                        recuperation_info['OP_CODEOPERATEUR'],
                        recuperation_info['CODECRYPTAGE']]):
                return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes.code erreur (301)", "SL_RESULTAT": 'FALSE'}), 200 
        
        # Connexion à la base de données
    db_connection = connect_database()

    try:
            with db_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                if(SUPPRIMERDONNEES == "O"):
                    pvgDelete(db_connection, "")
                # Appeler la fonction d'insertion dans la base de données
                reponse = pvgInsert(db_connection, recuperation_infos)
                
                if reponse is None:
                    # Valider la transaction
                    # get_commit(db_connection,recuperation_infos)
                            
                    return jsonify({"SL_MESSAGE": "Insertion réussie!", "SL_RESULTAT": 'TRUE'})
                else:
                    return jsonify({"SL_MESSAGE": f" {str(reponse['SL_MESSAGE'])}", "SL_RESULTAT": 'FALSE'}), 200
            
    except Exception as e:
            db_connection.rollback()
            return jsonify({"SL_MESSAGE": "Erreur lors de la suppression : " + str(e), "SL_RESULTAT": 'FALSE'})
        
    finally:
            db_connection.close()

@api_bp.route('/pvgInsert2', methods=['POST'])
def Insert2():
        # Récupérer les données du corps de la requête
        request_data = request.json
        recuperation_infos = []
        SUPPRIMERDONNEES = ""
        if 'Objet' not in request_data:
            return jsonify({"SL_MESSAGE": "Données manquantes.code erreur (300) voir le noeud Objet", "SL_RESULTAT": 'FALSE'})
        
        for row in request_data['Objet']:
            recuperation_info = {}

            try:
                # Validation des données
                # Récupération des valeurs en les convertissant correctement
                SUPPRIMERDONNEES = str(row.get('SUPPRIMERDONNEES', ''))
                recuperation_info['SL_IDINDEX'] = str(row.get('SL_IDINDEX', ''))
                recuperation_info['AG_CODEAGENCE'] = str(row.get('AG_CODEAGENCE', ''))
                recuperation_info['OP_CODEOPERATEUR'] = str(row.get('OP_CODEOPERATEUR', ''))
                recuperation_info['CL_NUMEROCOMPTE'] = str(row.get('CL_NUMEROCOMPTE', ''))
                recuperation_info['CO_CODECOMPTE'] = str(row.get('CO_CODECOMPTE', ''))
                recuperation_info['CL_IDCLIENT'] = str(row.get('CL_IDCLIENT', ''))
                recuperation_info['PL_CODENUMCOMPTE'] = str(row.get('PL_CODENUMCOMPTE', ''))
                recuperation_info['PS_CODESOUSPRODUIT'] = str(row.get('PS_CODESOUSPRODUIT', ''))
                recuperation_info['PV_CODEPOINTVENTE'] = str(row.get('PV_CODEPOINTVENTE', ''))
                recuperation_info['PV_RAISONSOCIAL'] = str(row.get('PV_RAISONSOCIAL', ''))
                recuperation_info['TS_CODETYPESCHEMACOMPTABLE'] = str(row.get('TS_CODETYPESCHEMACOMPTABLE', ''))
                recuperation_info['CODEPARAMETRE'] = str(row.get('CODEPARAMETRE', ''))
                recuperation_info['AT_CODEACTIVITE'] = str(row.get('AT_CODEACTIVITE', ''))
                recuperation_info['TI_CODETYPEAMORTISSEMENT'] = str(row.get('TI_CODETYPEAMORTISSEMENT', ''))
                recuperation_info['PE_CODEPERIODICITE'] = str(row.get('PE_CODEPERIODICITE', ''))
                recuperation_info['CR_NUMERODOSSIER'] = str(row.get('CR_NUMERODOSSIER', ''))
                recuperation_info['CR_DATEDEBLOCAGE'] = parse_datetime(row.get('CR_DATEDEBLOCAGE')) if row.get('CR_DATEDEBLOCAGE') else None
                recuperation_info['CR_DATEPECHEANCE'] = parse_datetime(row.get('CR_DATEPECHEANCE')) if row.get('CR_DATEPECHEANCE') else None
                recuperation_info['CR_NOMBREECHEANCE'] = int(row.get('CR_NOMBREECHEANCE', 0))
                recuperation_info['CR_TAUX'] = float(row.get('CR_TAUX', 0.0))
                recuperation_info['CR_DUREE'] = int(row.get('CR_DUREE', 0))
                recuperation_info['CR_MONTCREDITACCORDE'] = float(row.get('CR_MONTCREDITACCORDE', 0.0))
                recuperation_info['CR_MONTANTINTERETATTENDU'] = float(row.get('CR_MONTANTINTERETATTENDU', 0.0))
                recuperation_info['CR_LIBELLEOPERATION'] = str(row.get('CR_LIBELLEOPERATION', ''))
                recuperation_info['CR_NOMCLIENT'] = str(row.get('CR_NOMCLIENT', ''))
                recuperation_info['CR_PRENOMCLIENT'] = str(row.get('CR_PRENOMCLIENT', ''))
                recuperation_info['CR_TELEPHONE'] = str(row.get('CR_TELEPHONE', ''))
                recuperation_info['SL_TYPEOPERATION'] = str(row.get('SL_TYPEOPERATION', ''))
                recuperation_info['CR_DATEOPERATION'] = parse_datetime(row.get('CR_DATEOPERATION')) if row.get('CR_DATEOPERATION') else None
                recuperation_info['CODECRYPTAGE'] = str(row.get('CODECRYPTAGE', ''))
                recuperation_info['CR_FRAISDOSSIER'] = float(row.get('CR_FRAISDOSSIER', 0.0))
                recuperation_info['CR_FONDASSURANCE'] = float(row.get('CR_FONDASSURANCE', 0.0))
                recuperation_info['CR_COMMISSIONAVANCECHEQUE'] = float(row.get('CR_COMMISSIONAVANCECHEQUE', 0.0))
                recuperation_info['CR_COMMISSIONENGAGEMENT'] = float(row.get('CR_COMMISSIONENGAGEMENT', 0.0))
                recuperation_info['CR_ASSURENCEDECESINVALIDITE'] = float(row.get('CR_ASSURENCEDECESINVALIDITE', 0.0))
                recuperation_info['CR_COMMISSIONAVANCESALAIRE'] = float(row.get('CR_COMMISSIONAVANCESALAIRE', 0.0))
                recuperation_info['CR_MONTANTGARENTIE'] = float(row.get('CR_MONTANTGARENTIE', 0.0))
                recuperation_info['CR_ASSURENCEFACULTATIVE'] = float(row.get('CR_ASSURENCEFACULTATIVE', 0.0))
                
                recuperation_info['CR_EPARGNE'] = float(row.get('CR_EPARGNE', 0.0))
                recuperation_info['TO_CODETOMBEE'] = str(row.get('TO_CODETOMBEE', ''))
                recuperation_info['CR_FRAISASSISTANCEETFORMATION'] = float(row.get('CR_FRAISASSISTANCEETFORMATION', 0.0))
                recuperation_info['CR_FRAISETUDE'] = float(row.get('CR_FRAISETUDE', 0.0))
                recuperation_info['FRAISASSITANCEDEFORMATION'] = 0.0
                
                recuperation_infos.append(recuperation_info)
            except ValueError as e:
                # Retourner un message d'erreur en cas de problème de type de données
                return jsonify({"SL_MESSAGE": f"Erreur de type de données : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
            
            except Exception as e:
                # Retourner un message d'erreur en cas d'exception générale
                return jsonify({"SL_MESSAGE": f"Erreur inattendue : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
            
            # Vérification que toutes les données obligatoires sont présentes
            if not all([recuperation_info['AG_CODEAGENCE'], 
                        recuperation_info['OP_CODEOPERATEUR'],
                        recuperation_info['CODECRYPTAGE']]):
                return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes.code erreur (301)", "SL_RESULTAT": 'FALSE'}), 200
            
        # Connexion à la base de données
        db_connection = connect_database()
        db_connection = db_connection.cursor()
        db_connection.execute("BEGIN TRANSACTION")
        
        #try:
            
        #    with db_connection.cursor() as cursor:
        #        cursor.execute("BEGIN TRANSACTION")
        if(SUPPRIMERDONNEES == "O"):
           pvgDelete(db_connection, "")
        for recup_info in recuperation_infos:
                
                # Appeler la fonction d'insertion dans la base de données
            reponse = pvgInsert(db_connection, recup_info)
                
        if reponse is None:
            # Valider la transaction
            #get_commit(db_connection,recuperation_infos)
                    
            return jsonify({"SL_MESSAGE": "Insertion réussie!", "SL_RESULTAT": 'TRUE'})
        else:
            return jsonify({"SL_MESSAGE": f" {str(reponse['SL_MESSAGE'])}", "SL_RESULTAT": 'FALSE'}), 200
        #except Exception as e:
        #    db_connection.rollback()
        #    return jsonify({"SL_MESSAGE": f"Erreur lors de l'insertion : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
        
        #finally:
        #    db_connection.close()



@api_bp.route('/pvgChargerDansDataSet4', methods=['POST'])
def ChargerDansDataSet4():
    request_data = request.json
    
    if 'Objet' not in request_data:
        return jsonify({"SL_MESSAGE": "Données manquantes.code erreur (300) voir le noeud Objet", "SL_RESULTAT": 'FALSE'})
    
    for row in request_data['Objet']:
        valeur_info = {}

        # Validation et récupération des données pour la suppression
        valeur_info['CODECRYPTAGE'] = str(row.get('CODECRYPTAGE', ''))
        valeur_info['AT_LIBELLE'] = str(row.get('AT_LIBELLE', ''))
        CODECRYPTAGE = str(row.get('CODECRYPTAGE', ''))
        AT_LIBELLE = str(row.get('AT_LIBELLE', ''))
        
        # Vérification que toutes les données obligatoires sont présentes
        if not all([valeur_info['CODECRYPTAGE'], valeur_info['AT_LIBELLE']]):
            return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes.code erreur (301) CODECRYPTAGE ou AT_LIBELLE", "SL_RESULTAT": 'FALSE'})

        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                # Appeler la fonction de suppression
                response = pvgChargerDansDataSet4(db_connection,CODECRYPTAGE, AT_LIBELLE)
                
                
            if len(response) > 0:
                return jsonify({"SL_MESSAGE": "Opération éffectuée avec succès !!!", "SL_RESULTAT": 'TRUE'},response)
            else:
                return jsonify({"SL_MESSAGE": "Aucun élément trouvé", "SL_RESULTAT": 'FALSE'})
        
        except Exception as e:
            db_connection.rollback()
            return jsonify({"SL_MESSAGE": "Erreur lors du chargement : " + str(e), "SL_RESULTAT": 'FALSE'})
        
        finally:
            db_connection.close()




################################################################
#   GESTION DES CREDIT                                         #
################################################################

################################################################
#   GESTION DES COMPTABILISATION CREDIT                        #
################################################################
@api_bp.route('/pvgChargerDansDataSetPourComboParametretableaimporterouexporters', methods=['POST'])
def ChargerDansDataSetPourComboParametretableaimporterouexporter():
    request_data = request.json
    
    for row in request_data['Objet']:
        Valeur_info = {}

        # Validation et récupération des données pour la suppression
        Valeur_info['CODEPARAMETRE'] = str(row.get('CODEPARAMETRE', '')) 
        
        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                # Appeler la fonction de suppression
                if Valeur_info['CODEPARAMETRE']:  # Si une valeur est fournie
                    response = pvgChargerDansDataSetPourComboParametretableaimporterouexporters(db_connection, str(row.get('CODEPARAMETRE', '')))
                else:  # Sinon, on appelle sans paramètre
                    response = pvgChargerDansDataSetPourComboParametretableaimporterouexporters(db_connection)
                
                
            if len(response) > 0 :
                return jsonify({"SL_MESSAGE": "Opération éffectuée avec succès !!!", "SL_RESULTAT": 'TRUE'},response)
            else:
                return jsonify({"SL_MESSAGE": response['SL_MESSAGE'], "SL_RESULTAT": 'FALSE'})
        
        except Exception as e:
            db_connection.rollback()
            return jsonify({"SL_MESSAGE": "Erreur lors de la recuperation : " + str(e), "SL_RESULTAT": 'FALSE'})
        
        finally:
            db_connection.close()

@api_bp.route('/pvgChargerDansDataSetPourComboOP_AGENTCOLLECTEETCREDIT', methods=['POST'])
def ChargerDansDataSetPourComboOP_AGENTCOLLECTEETCREDIT():
    request_data = request.json
    
    for row in request_data['Objet']:
        Valeur_info = {}

        # Validation et récupération des données pour la suppression
        Valeur_info['CODECRYPTAGE'] = str(row.get('CODECRYPTAGE', '')) 
        Valeur_info['AG_CODEAGENCE'] = str(row.get('AG_CODEAGENCE', '')) 
        

        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                # Appeler la fonction de suppression
                response = pvgChargerDansDataSetPourComboOP_AGENTCOLLECTEETCREDIT(db_connection, str(row.get('CODECRYPTAGE', '')), str(row.get('AG_CODEAGENCE', '')))
                
                
            if len(response) > 0 :
                return jsonify({"SL_MESSAGE": "Opération éffectuée avec succès !!!", "SL_RESULTAT": 'TRUE'},response)
            else:
                return jsonify({"SL_MESSAGE": response['SL_MESSAGE'], "SL_RESULTAT": 'FALSE'})
        
        except Exception as e:
            db_connection.rollback()
            return jsonify({"SL_MESSAGE": "Erreur lors de la recuperation : " + str(e), "SL_RESULTAT": 'FALSE'})
        
        finally:
            db_connection.close()


@api_bp.route('/pvgChargerDansDataSetPourComboEcranComptabilisationVirement', methods=['POST'])
def ChargerDansDataSetPourComboEcranComptabilisationVirement():
    request_data = request.json
    
    for row in request_data['Objet']:
        Valeur_info = {}

        # Validation et récupération des données pour la suppression
        Valeur_info['TS_AFFICHERECRANCOMPTABILISATION'] = str(row.get('TS_AFFICHERECRANCOMPTABILISATION', '')) 
        
        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                # Appeler la fonction de suppression
                if Valeur_info['TS_AFFICHERECRANCOMPTABILISATION']:  # Si une valeur est fournie
                    response = pvgChargerDansDataSetPourComboEcranComptabilisationVirement(db_connection, str(row.get('TS_AFFICHERECRANCOMPTABILISATION', '')))
                else:  # Sinon, on appelle sans paramètre
                    response = pvgChargerDansDataSetPourComboEcranComptabilisationVirement(db_connection)
                
                
            if len(response) > 0 :
                return jsonify({"SL_MESSAGE": "Opération éffectuée avec succès !!!", "SL_RESULTAT": 'TRUE'},response)
            else:
                return jsonify({"SL_MESSAGE": response['SL_MESSAGE'], "SL_RESULTAT": 'FALSE'})
        
        except Exception as e:
            db_connection.rollback()
            return jsonify({"SL_MESSAGE": "Erreur lors de la recuperation : " + str(e), "SL_RESULTAT": 'FALSE'})
        
        finally:
            db_connection.close()


@api_bp.route('/pvgDeleteMicsalaireimportezedtachesautomatique', methods=['POST'])
def DeleteMicsalaireimportezedtachesautomatique():
    request_data = request.json
    
    for row in request_data['Objet']:
        Valeur_info = {}

        # Validation et récupération des données pour la suppression
        AG_CODEAGENCE = str(row.get('AG_CODEAGENCE', ''))
        
        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                # Appeler la fonction de suppression
                response = pvgDeleteMicsalaireimportezedtachesautomatique(db_connection, AG_CODEAGENCE)
                
                
            if response and 'SL_RESULTAT' in response and response['SL_RESULTAT'] != "FALSE":
                return jsonify({"SL_MESSAGE": "Opération éffectuée avec succès !!!", "SL_RESULTAT": 'TRUE'})
            else:
                return jsonify({"SL_MESSAGE": response['SL_MESSAGE'], "SL_RESULTAT": 'FALSE'})
        
        except Exception as e:
            db_connection.rollback()
            Retour = {}
            Retour['SL_MESSAGE'] = str(e.args[1])
            if "[SQL Server]" in Retour['SL_MESSAGE']:
                Retour['SL_MESSAGE'] = Retour['SL_MESSAGE'].split("[SQL Server]", 1)[1].split(".", 1)[0]
            Retour['SL_RESULTAT'] = "FALSE"
            return jsonify({"SL_MESSAGE": "Erreur lors de la suppresion : " + response['SL_MESSAGE'], "SL_RESULTAT": 'FALSE'})
        
        finally:
            db_connection.close()



@api_bp.route('/pvgDelete1', methods=['POST'])
def Delete1():
    request_data = request.json
    
    for row in request_data['Objet']:
        Valeur_info = {}

        # Validation et récupération des données pour la suppression
        CODECRYPTAGE = str(row.get('CODECRYPTAGE', ''))
        OP_CODEOPERATEUR = str(row.get('OP_CODEOPERATEUR', ''))
        
        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                # Appeler la fonction de suppression
                response = pvgDelete1(db_connection, CODECRYPTAGE,OP_CODEOPERATEUR)
                
                
            if response and 'SL_RESULTAT' in response and response['SL_RESULTAT'] != "FALSE":
                return jsonify({"SL_MESSAGE": "Opération éffectuée avec succès !!!", "SL_RESULTAT": 'TRUE'})
            else:
                return jsonify({"SL_MESSAGE": response['SL_MESSAGE'], "SL_RESULTAT": 'FALSE'})
        
        except Exception as e:
            db_connection.rollback()
            Retour = {}
            Retour['SL_MESSAGE'] = str(e.args[1])
            if "[SQL Server]" in Retour['SL_MESSAGE']:
                Retour['SL_MESSAGE'] = Retour['SL_MESSAGE'].split("[SQL Server]", 1)[1].split(".", 1)[0]
            Retour['SL_RESULTAT'] = "FALSE"
            return jsonify({"SL_MESSAGE": "Erreur lors de la suppresion : " + response['SL_MESSAGE'], "SL_RESULTAT": 'FALSE'})
        
        finally:
            db_connection.close()

@api_bp.route('/pvgInsertValidation', methods=['POST'])
def InsertValidation():
        # Récupérer les données du corps de la requête
        request_data = request.json
        recuperation_infos = []
        CODECRYPTAGE = ""
        OP_CODEOPERATEUR = ""
        if 'Objet' not in request_data:
            return jsonify({"SL_MESSAGE": "Données manquantes.code erreur (300) voir le noeud Objet", "SL_RESULTAT": 'FALSE'})
        
        for row in request_data['Objet']:
            recuperation_info = {}

            try:
                # Validation des données
                # Récupération des valeurs en les convertissant correctement
                CODECRYPTAGE = 'Y}@128eVIXfoi7'
                OP_CODEOPERATEUR = str(row.get('OP_CODEOPERATEUR', ''))
                recuperation_info['SL_IDINDEX'] = str(row.get('SL_IDINDEX', ''))
                recuperation_info['AG_CODEAGENCE'] = str(row.get('AG_CODEAGENCE', ''))
                recuperation_info['OP_CODEOPERATEUR'] = str(row.get('OP_CODEOPERATEUR', ''))
                recuperation_info['CL_NUMEROCOMPTE'] = str(row.get('CL_NUMEROCOMPTE', ''))
                recuperation_info['CO_CODECOMPTE'] = str(row.get('CO_CODECOMPTE', ''))
                recuperation_info['CL_IDCLIENT'] = str(row.get('CL_IDCLIENT', ''))
                recuperation_info['PL_CODENUMCOMPTE'] = str(row.get('PL_CODENUMCOMPTE', ''))
                recuperation_info['PS_CODESOUSPRODUIT'] = str(row.get('PS_CODESOUSPRODUIT', ''))
                recuperation_info['PV_CODEPOINTVENTE'] = str(row.get('PV_CODEPOINTVENTE', ''))
                recuperation_info['PV_RAISONSOCIAL'] = str(row.get('PV_RAISONSOCIAL', ''))
                recuperation_info['TS_CODETYPESCHEMACOMPTABLE'] = str(row.get('TS_CODETYPESCHEMACOMPTABLE', ''))
                recuperation_info['CODEPARAMETRE'] = str(row.get('CODEPARAMETRE', ''))
                recuperation_info['AT_CODEACTIVITE'] = str(row.get('AT_CODEACTIVITE', ''))
                recuperation_info['TI_CODETYPEAMORTISSEMENT'] = str(row.get('TI_CODETYPEAMORTISSEMENT', ''))
                recuperation_info['PE_CODEPERIODICITE'] = str(row.get('PE_CODEPERIODICITE', ''))
                recuperation_info['CR_NUMERODOSSIER'] = str(row.get('CR_NUMERODOSSIER', ''))
                recuperation_info['CR_DATEDEBLOCAGE'] = parse_datetime(row.get('CR_DATEDEBLOCAGE')) if row.get('CR_DATEDEBLOCAGE') else None
                recuperation_info['CR_DATEPECHEANCE'] = parse_datetime(row.get('CR_DATEPECHEANCE')) if row.get('CR_DATEPECHEANCE') else None
                recuperation_info['CR_NOMBREECHEANCE'] = int(row.get('CR_NOMBREECHEANCE', 0))
                recuperation_info['CR_TAUX'] = float(row.get('CR_TAUX', 0.0))
                recuperation_info['CR_DUREE'] = int(row.get('CR_DUREE', 0))
                recuperation_info['CR_MONTCREDITACCORDE'] = float(row.get('CR_MONTCREDITACCORDE', 0.0))
                recuperation_info['CR_MONTANTINTERETATTENDU'] = float(row.get('CR_MONTANTINTERETATTENDU', 0.0))
                recuperation_info['CR_LIBELLEOPERATION'] = str(row.get('CR_LIBELLEOPERATION', ''))
                recuperation_info['CR_NOMCLIENT'] = str(row.get('CR_NOMCLIENT', ''))
                recuperation_info['CR_PRENOMCLIENT'] = str(row.get('CR_PRENOMCLIENT', ''))
                recuperation_info['CR_TELEPHONE'] = str(row.get('CR_TELEPHONE', ''))
                recuperation_info['SL_TYPEOPERATION'] = str(row.get('SL_TYPEOPERATION', ''))
                recuperation_info['CR_DATEOPERATION'] = parse_datetime(row.get('CR_DATEOPERATION')) if row.get('CR_DATEOPERATION') else None
                recuperation_info['CODECRYPTAGE'] = 'Y}@128eVIXfoi7'
                recuperation_info['CR_FRAISDOSSIER'] = float(row.get('CR_FRAISDOSSIER', 0.0))
                recuperation_info['CR_FONDASSURANCE'] = float(row.get('CR_FONDASSURANCE', 0.0))
                recuperation_info['CR_COMMISSIONAVANCECHEQUE'] = float(row.get('CR_COMMISSIONAVANCECHEQUE', 0.0))
                recuperation_info['CR_COMMISSIONENGAGEMENT'] = float(row.get('CR_COMMISSIONENGAGEMENT', 0.0))
                recuperation_info['CR_ASSURENCEDECESINVALIDITE'] = float(row.get('CR_ASSURENCEDECESINVALIDITE', 0.0))
                recuperation_info['CR_COMMISSIONAVANCESALAIRE'] = float(row.get('CR_COMMISSIONAVANCESALAIRE', 0.0))
                recuperation_info['CR_MONTANTGARENTIE'] = float(row.get('CR_MONTANTGARENTIE', 0.0))
                recuperation_info['CR_ASSURENCEFACULTATIVE'] = float(row.get('CR_ASSURENCEFACULTATIVE', 0.0))
                
                recuperation_info['CR_EPARGNE'] = float(row.get('CR_EPARGNE', 0.0))
                recuperation_info['TO_CODETOMBEE'] = str(row.get('TO_CODETOMBEE', ''))
                recuperation_info['CR_FRAISASSISTANCEETFORMATION'] = float(row.get('CR_FRAISASSISTANCEETFORMATION', 0.0))
                recuperation_info['CR_FRAISETUDE'] = float(row.get('CR_FRAISETUDE', 0.0))

                
                recuperation_infos.append(recuperation_info)
            except ValueError as e:
                # Retourner un message d'erreur en cas de problème de type de données
                return jsonify({"SL_MESSAGE": f"Erreur de type de données : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
            
            except Exception as e:
                # Retourner un message d'erreur en cas d'exception générale
                return jsonify({"SL_MESSAGE": f"Erreur inattendue : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
            
            # Vérification que toutes les données obligatoires sont présentes
            if not all([recuperation_info['AG_CODEAGENCE'], 
                        recuperation_info['OP_CODEOPERATEUR'], 
                        recuperation_info['CODECRYPTAGE']]):
                return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes.code erreur (301)", "SL_RESULTAT": 'FALSE'}), 200
            
        # Connexion à la base de données
        db_connection = connect_database()
        db_connection = db_connection.cursor()
        db_connection.execute("BEGIN TRANSACTION")
        
        #try:
            
        #    with db_connection.cursor() as cursor:
        #        cursor.execute("BEGIN TRANSACTION")
        response = pvgDelete1(db_connection, CODECRYPTAGE,OP_CODEOPERATEUR)
        if response is None: 
            for recup_info in recuperation_infos:
                    
                    # Appeler la fonction d'insertion dans la base de données
                reponse =  pvgInsertValidation(db_connection, recup_info)
                    
            
            if reponse is None:
                # Valider la transaction
                pvgDeleteValide(db_connection,'')
                get_commit(db_connection,recuperation_infos)
                        
                return jsonify({"SL_MESSAGE": "Insertion réussie!", "SL_RESULTAT": 'TRUE'})
            else:
                return jsonify({"SL_MESSAGE": f" {str(reponse['SL_MESSAGE'])}", "SL_RESULTAT": 'FALSE'}), 200
        else:
                return jsonify({"SL_MESSAGE": f" {str(response['SL_MESSAGE'])}", "SL_RESULTAT": 'FALSE'}), 200
        #except Exception as e:
        #    db_connection.rollback()
        #    return jsonify({"SL_MESSAGE": f"Erreur lors de l'insertion : {str(e)}", "SL_RESULTAT": 'FALSE'}), 200
        
        #finally:
        #    db_connection.close()


@api_bp.route('/pvgChargerDansDataSetMICSALAIREIMPORTECREDIT', methods=['POST'])
def ChargerDansDataSetMICSALAIREIMPORTECREDIT():
    request_data = request.json
    
    if 'Objet' not in request_data:
        return jsonify({"SL_MESSAGE": "Données manquantes.code erreur (300) voir le noeud Objet", "SL_RESULTAT": 'FALSE'})
    
    for row in request_data['Objet']:
        valeur_info = {}

        # Validation et récupération des données pour la suppression
        valeur_info['CODECRYPTAGE'] = str(row.get('CODECRYPTAGE', ''))
        valeur_info['AG_CODEAGENCE'] = str(row.get('AG_CODEAGENCE', ''))
        CODECRYPTAGE = str(row.get('CODECRYPTAGE', ''))
        AG_CODEAGENCE = str(row.get('AG_CODEAGENCE', ''))
        OP_CODEOPERATEUR = str(row.get('OP_CODEOPERATEUR', ''))
        CODEPARAMETRE = str(row.get('CODEPARAMETRE', ''))
        
        # Vérification que toutes les données obligatoires sont présentes
        if not all([valeur_info['CODECRYPTAGE'], valeur_info['AG_CODEAGENCE']]):
            return jsonify({"SL_MESSAGE": "Données manquantes ou incorrectes.code erreur (301) CODECRYPTAGE ou AG_CODEAGENCE", "SL_RESULTAT": 'FALSE'})

        # Connexion à la base de données
        db_connection = connect_database()

        try:
            with db_connection.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION")
                
                # Appeler la fonction de suppression
                response = pvgChargerDansDataSetMICSALAIREIMPORTECREDIT(db_connection,CODECRYPTAGE, AG_CODEAGENCE,OP_CODEOPERATEUR, CODEPARAMETRE)
                
                
            if len(response) > 0:
                return jsonify({"SL_MESSAGE": "Opération éffectuée avec succès !!!", "SL_RESULTAT": 'TRUE'},response)
            else:
                return jsonify({"SL_MESSAGE": "Aucun élement trouvé", "SL_RESULTAT": 'FALSE'})
        
        except Exception as e:
            db_connection.rollback()
            return jsonify({"SL_MESSAGE": "Erreur lors du chargement : " + str(e), "SL_RESULTAT": 'FALSE'})
        
        finally:
            db_connection.close()

################################################################
#   GESTION DES COMPTABILISATION CREDIT                        #
################################################################


# Fonction pour générer un code aléatoire
def generer_code_aleatoire(taille=6):
    lettres_chiffres = string.ascii_uppercase + string.digits
    return ''.join(random.choice(lettres_chiffres) for _ in range(taille))               

def parse_numeric(value):
    """Vérifie si la valeur est un nombre et la convertit. Renvoie une exception si la conversion échoue."""
    if value is None or value == '':
        return None
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"Format numérique invalide: {value}")


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