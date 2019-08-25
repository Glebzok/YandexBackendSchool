# База жителей

from flask import Flask, jsonify, request, make_response, redirect, url_for
from flask_restful import Resource, Api
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import and_, func, update
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

# Import Class (Table)

class Import(db.Model):
    import_id = db.Column(db.Integer, primary_key = True)
    
    def __init__(self):
        pass
        
# Family ties Class (Table)

class Tie(db.Model):
    import_id = db.Column(db.Integer, primary_key = True)
    first_citizen_id = db.Column(db.Integer, primary_key = True)
    second_citizen_id = db.Column(db.Integer, primary_key = True)
    
    def __init__(self, import_id, first_citizen_id, second_citizen_id):
        self.import_id = import_id
        self.first_citizen_id = first_citizen_id
        self.second_citizen_id = second_citizen_id

# Citizen Class (Table)
               
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

def is_passed(field, value, error):
    if not value.date() < datetime.utcnow().date():
        error(field, "Must be passed data")
        
def has_no_duplicates(field, value, error):
    if not len(value) == len(list(set(value))):
        error(field, "All values must be unique")
        
post_validation_schema = {
        'list':{
            'type': 'list',
            'schema': {
                'type': 'dict',
                'allow_unknown': False,
                'schema': {
                    'citizen_id': {
                    'type': 'integer',
                    'min' : 0,
                    'required': True
                    },
                    'town': {
                        'type': 'string',
                        'minlength': 1,
                        'maxlength': 256,
                        'regex': '(?=.*?([0-9А-Яа-яA-Za-z])).+',
                        'required': True
                        
                    },
                    'street': {
                        'type': 'string',
                        'minlength': 1,
                        'maxlength': 256,
                        'regex': '(?=.*?([0-9А-Яа-яA-Za-z])).+',
                        'required': True
                    },
                    'building': {
                        'type': 'string',
                        'minlength': 1,
                        'maxlength': 256,
                        'regex': '(?=.*?([0-9А-Яа-яA-Za-z])).+',
                        'required': True
                    },
                    'apartment': {
                        'type': 'integer',
                        'min': 0,
                        'required': True
                    },
                    'name': {
                        'type': 'string',
                        'minlength': 1,
                        'maxlength': 256,
                        'required': True
                    },
                    'birth_date': {
                        'type': 'datetime',
                        'coerce': lambda s: datetime.strptime(s, '%d.%m.%Y'),
                        'validator': is_passed,
                        'required': True
                    },
                    'gender': {
                        'type': 'string',
                        'allowed': ['male', 'female'],
                        'required': True
                    },
                    'relatives': {
                        'type': 'list',
                        'validator': has_no_duplicates,
                        'schema': {
                            'type': 'integer',
                            'min' : 0
                        },
                        'required': True
                    }
                }
                
            }
        }
    }
     
patch_validation_schema = {
    'dict': {
        'type': 'dict',
        'minlength': 1,
        'allow_unknown': False,
        'schema': {
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
                'coerce': lambda s: datetime.strptime(s, '%d.%m.%Y'),
                'validator': is_passed
            },
            'gender': {
                'type': 'string',
                'allowed': ['male', 'female']
            },
            'relatives': {
                'type': 'list',
                'validator': has_no_duplicates,
                'schema': {
                    'type': 'integer',
                    'min' : 0
                }
            }
        }                
    }
}

# Import Schema

class ImportSchema(ma.Schema):
    class Meta:
        fields = ('import_id',)
        
# Tie Schema
        
class TieSchema(ma.Schema):
    class Meta:
        fields = ('import_id', 'first_citizen_id', 'second_citizen_id')
        
# Citizen Schema
class Relatives(ma.Field):
    def _serialize(self, value, attr, obj):
        if value:
            return [int(i) for i in value.split(',')]
            
class Birth_date(ma.Field):
    def _serialize(self, value, attr, obj):
        if value:
            return value.strftime('%d.%m.%Y')

class CitizenSchema(ma.Schema):
    relatives = Relatives()
    birth_date = Birth_date()
    class Meta:
        fields = ('citizen_id', 'town', 'street', 'building', 'apartment', 'name', 'birth_date', 'gender', 'relatives')
        
# Init Schema

import_schema = ImportSchema()
imports_schema = ImportSchema(many = True)

tie_schema = TieSchema()
ties_schema = TieSchema(many = True)

citizen_schema = CitizenSchema()
citizens_schema = CitizenSchema(many = True)

# Add data set

class import_data(Resource):
    def post(self):              
        validator = Validator()
        if validator.validate({'list':request.json}, post_validation_schema): 
            new_import = Import()
            
            db.session.add(new_import)
            db.session.commit()
        
            import_id = int(new_import.import_id)
            
            family_ties = {}            
        
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
                
                for relative in relatives:
                    new_tie = Tie(import_id, citizen_id, relative)                    
                    db.session.add(new_tie)
                    
                family_ties[citizen_id] = set(relatives)
                
            # Check if family ties are valid
            
            for citizen in family_ties.keys():
                for relative in family_ties[citizen]:
                    if relative not in family_ties.keys() or citizen not in family_ties[relative]:
                        db.session.rollback()
                        Import.query.filter(Import.import_id == import_id).delete()
                        db.session.commit()
                        return '', 400
                
            db.session.commit()
                
            return make_response(jsonify({'data': {'import_id': import_id}}), 201)
        else:
            db.session.rollback()
            db.session.commit()
            return '', 400
               
api.add_resource(import_data, '/imports')

# change data

class change_data(Resource):
    def patch(self, import_id, citizen_id):
        validator = Validator()
        is_valid = (Citizen.query.filter(Citizen.import_id == import_id, Citizen.citizen_id == citizen_id).first() != None) and\
                   (validator.validate({'dict':request.json}, patch_validation_schema))
        if is_valid:
            if 'relatives' in request.json.keys():
                
                return '', 200
            else:
                db.session.query(Citizen).filter(Citizen.import_id == import_id, Citizen.citizen_id == citizen_id).update(request.json)
                 
                db.session.commit()
                
                citizen = db.session.query(Citizen, Citizen.import_id, Citizen.citizen_id, Citizen.town, Citizen.street, Citizen.building, Citizen.apartment, Citizen.name, Citizen.birth_date, Citizen.gender, func.group_concat(Tie.second_citizen_id).label('relatives'))\
                .join(Tie, and_(Citizen.citizen_id==Tie.first_citizen_id, Citizen.import_id==import_id, Tie.import_id==import_id, Citizen.citizen_id == citizen_id, Tie.first_citizen_id == citizen_id))\
                .group_by(Citizen.import_id, Citizen.citizen_id, Citizen.town, Citizen.street, Citizen.building, Citizen.apartment, Citizen.name, Citizen.birth_date, Citizen.gender)\
                .first()
                result = citizen_schema.dump(citizen)
                return make_response(jsonify({'data': result}), 200)
        else:
            return '', 400
            
api.add_resource(change_data, '/imports/<int:import_id>/citizens/<int:citizen_id>')

# get data set

class get_data(Resource):
    def get(self, import_id): 
        is_valid_id = (Import.query.filter(Import.import_id == import_id).first() != None)
#        is_valid_id = True
        if is_valid_id:
#            all_citizens = Citizen.query.filter(Citizen.import_id == import_id)
            citizens = db.session.query(Citizen, Citizen.import_id, Citizen.citizen_id, Citizen.town, Citizen.street, Citizen.building, Citizen.apartment, Citizen.name, Citizen.birth_date, Citizen.gender, func.group_concat(Tie.second_citizen_id).label('relatives'))\
            .join(Tie, and_(Citizen.citizen_id==Tie.first_citizen_id, Citizen.import_id==import_id, Tie.import_id==import_id))\
            .group_by(Citizen.import_id, Citizen.citizen_id, Citizen.town, Citizen.street, Citizen.building, Citizen.apartment, Citizen.name, Citizen.birth_date, Citizen.gender)
            result = citizens_schema.dump(citizens)
#                result = imports_schema.dump(Import.query.all())
#            result = ties_schema.dump(ties_in_import)
            return make_response(jsonify({'data': result}), 200)
        else:
            return '', 400
            
api.add_resource(get_data, '/imports/<int:import_id>/citizens')

#Run server

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)  
