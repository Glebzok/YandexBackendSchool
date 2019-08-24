# База жителей

from flask import Flask, jsonify, request, make_response
from flask_restful import Resource, Api
from flask_sqlalchemy import SQLAlchemy
from cerberus import Validator
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

# Import Class

class Import(db.Model):
    import_id = db.Column(db.Integer, primary_key = True)
    
    def __init__(self):
        pass

# Citizen Class
               
class Citizen(db.Model):
    import_id = db.Column(db.Integer, primary_key = True)
    citizen_id = db.Column(db.Integer, primary_key = True)
    town = db.Column(db.String(256), nullable = False)
    street = db.Column(db.String(256), nullable = False)
    building = db.Column(db.String(256), nullable = False)
    apartment = db.Column(db.Integer, nullable = False)
    name = db.Column(db.String(256), nullable = False)
    birth_date = db.Column(db.Date(), nullable = False)
    gender = db.Column(db.String, nullable = False)
    
    def __init__(self, import_id, citizen_id, town, street, building, apartment, name, birth_date, gender):
        self.import_id = import_id
        self.citizen_id = citizen_id
        self.town = town
        self.street = street
        self.building = building
        self.apartment = apartment
        self.name = name
        self.birth_date = birth_date
        self.gender = gender
        

db.create_all()
db.session.commit()

# Citizen validator

validation_schema = {
        'list':{
            'type': 'list',
            'schema': {
                'type': 'dict',
                'schema': {
                    'citizen_id': {
                    'type': 'integer',
                    'min' : 0
                    },
                    'town': {
                        'type': 'string',
                        'minlength': 1,
                        'maxlength': 256,
                        'regex': '(?=.*?([0-9А-Яа-яA-Za-z])).+'
                    },
                    'street': {
                        'type': 'string',
                        'minlength': 1,
                        'maxlength': 256,
                        'regex': '(?=.*?([0-9А-Яа-яA-Za-z])).+'
                    },
                    'building': {
                        'type': 'string',
                        'minlength': 1,
                        'maxlength': 256,
                        'regex': '(?=.*?([0-9А-Яа-яA-Za-z])).+'
                    },
                    'apartment': {
                        'type': 'integer',
                        'min': 0
                    },
                    'name': {
                        'type': 'string',
                        'minlength': 1,
                        'maxlength': 256
                    },
                    'birth_date': {
                        'type': 'datetime',
                        'coerce': lambda s: datetime.strptime(s, '%d.%m.%Y')
                    },
                    'gender': {
                        'type': 'string',
                        'allowed': ['male', 'female']
                    },
                    'relatives': {
                        'type': 'list',
                        'schema': {
                            'type': 'integer',
                            'min' : 0
                        }
                    }
                }
                
            }
        }
    }
        
# Import Schema

class ImportSchema(ma.Schema):
    class Meta:
        fields = ('import_id',)
        
# Citizen Schema

class CitizenSchema(ma.Schema):
    class Meta:
        fields = ('import_id', 'citizen_id', 'town', 'street', 'building', 'apartment', 'name', 'birth_date', 'gender')
        
# Init Schema

import_schema = ImportSchema()
imports_schema = ImportSchema(many = True)

citizen_schema = CitizenSchema()
citizens_schema = CitizenSchema(many = True)

# Add data set

class import_data(Resource):
    def post(self):              
        validator = Validator()
        if validator.validate({'list':request.json}, validation_schema): 
            new_import = Import()
            
            db.session.add(new_import)
            db.session.commit()
        
            import_id = int(new_import.import_id)            
        
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
                
                new_citizen = Citizen(import_id, citizen_id, town, street, building, apartment, name, birth_date, gender)
                
                db.session.add(new_citizen)
                
            db.session.commit()
                
            return make_response(jsonify({'data': import_id}), 201)
        else:
            return '', 400
               
api.add_resource(import_data, '/imports')

# get data set

class get_data(Resource):
    def get(self, import_id):     
        try:
            is_valid_id = (Import.query.filter(Import.import_id == import_id).first() != None)
            if is_valid_id:
                all_citizens = Citizen.query.filter(Citizen.import_id == import_id)
#                result = citizens_schema.dump(all_citizens)
                result = imports_schema.dump(Import.query.all())
                return make_response(jsonify({'data': result}), 200)
            else:
                return '', 400
        except:
            return '', 400
            
api.add_resource(get_data, '/imports/<int:import_id>/citizens')

#Run server

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)  
