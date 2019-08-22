# База жителей

from flask import Flask, jsonify, request
from flask_restful import Resource, Api
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow

import os
from datetime import datetime

# Init app

app = Flask(__name__)
api = Api(app)

root_dir = os.path.abspath(os.path.dirname(__file__))

# Init database

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(root_dir, 'db.sqlite')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Init marshmallow

ma = Marshmallow(app)

# Citizen Class
               
class Citizen(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    citizen_id = db.Column(db.Integer)
    town = db.Column(db.String(256))
    street = db.Column(db.String(256))
    building = db.Column(db.String(256))
    apartment = db.Column(db.Integer)
    name = db.Column(db.String(256))
    birth_date = db.Column(db.Date())
    gender = db.Column(db.String)
    
    def __init__(self, citizen_id, town, street, building, apartment, name, birth_date, gender):
        self.citizen_id = citizen_id
        self.town = town
        self.street = street
        self.building = building
        self.apartment = apartment
        self.name = name
        self.birth_date = birth_date
        self.gender = gender
        
# Citizen Schema

class CitizenSchema(ma.Schema):
    class Meta:
        fields = ('import_id', 'citizen_id', 'town', 'street', 'building', 'apartment', 'name', 'birth_date', 'gender')
        
# Init Schema

citizen_schema = CitizenSchema()
citizens_schema = CitizenSchema(many = True)

# Add data set

class import_data(Resource):
    def post(self):
        
        #import_id = int(db.session.query(db.func.max(Citizen.import_id)).scalar()) + 1
        
        for citizen in request.json:
            
            citizen_id = citizen['citizen_id']
            town = citizen['town']
            street = citizen['street']
            building = citizen['building']
            apartment = citizen['apartment']
            name = citizen['name']
            birth_date = datetime.strptime(citizen['birth_date'], "%d.%m.%Y").date()
            gender = citizen['gender']
            relatives = citizen['relatives']
            
            new_citizen = Citizen(citizen_id, town, street, building, apartment, name, birth_date, gender)
            
            db.session.add(new_citizen)
            
        db.session.commit()
            
        return jsonify({
                            'data': 2
                       }
        )
               
api.add_resource(import_data, '/imports')

#Run server

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
