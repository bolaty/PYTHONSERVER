from flask import Flask,jsonify,request,render_template
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from utils import connect_database
from config import connected_cashiers,socketio
import logging as logger
logger.basicConfig(level="DEBUG")
from routes import api_bp
import os
app = Flask(__name__)
socketio = SocketIO(app)
CORS(app)



# Quand une caissière se connecte au WebSocket
@socketio.on('connect_cashier')
def handle_connect_cashier(data):
    cashier_id = data.get('OP_CODEOPERATEUR')
    if cashier_id:
        connected_cashiers[cashier_id] = request.sid
        emit('status', {'message': 'Caissière connectée', 'cashier_id': cashier_id}, broadcast=True)

# Quand une caissière se déconnecte
@socketio.on('disconnect')
def handle_disconnect():
    for cashier_id, sid in list(connected_cashiers.items()):
        if sid == request.sid:
            del connected_cashiers[cashier_id]
            break

# Exemple de route pour tester la connexion à la base de données
UPLOAD_FOLDER = 'D:/UploadFile'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Assurez-vous que le dossier existe
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
@app.route('/')
def hello():
    #global db_connection
    
    #cursor = db_connection.cursor()
    #insert_query = "INSERT INTO TABLEINSERTION (IDTABLEINSET, LIBELLETABLEINSET) VALUES (3,'euloge')"
    #cursor.execute(insert_query)
    # Exécuter une requête SQL
    # cursor.execute('SELECT * FROM SOCIETE') 

    # Récupérer les résultats de la requête
    # rows = cursor.fetchall()
    # Exécution de la commande
    #db_connection.commit()
    # Fermer la connexion à la base de données
    #db_connection.close()
    #return 'ok'
    return render_template('home.html')


# Enregistrer le blueprint API
app.register_blueprint(api_bp, url_prefix='/api')

if __name__ == '__main__':
    #from waitress import serve
    #serve(app, host='192.168.1.124', port=5000)
    app.run(host="0.0.0.0", port=6001,debug=True)
    #socketio.run(app, debug=True)
    #app.run(debug=True)