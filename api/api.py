# База жителей

from flask import Flask, jsonify, request, make_response, redirect, url_for
from flask_restful import Resource, Api
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import and_, func, update, or_, cast
from cerberus import Validator
from flask_marshmallow import Marshmallow

import os
from datetime import datetime

from numpy import percentile
from numpy import round as rnd

# Init app

app = Flask(__name__)
api = Api(app)

root_dir = os.path.abspath(os.path.dirname(__file__))

# Init database

#app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql:///' + os.path.join(root_dir, 'db.mysql')
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqldb://root:password@database/db?charset=utf8mb4'
app.config['MYSQL_DATABASE_CHARSET'] = 'utf8mb4'
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
    gender = db.Column(db.String(6), nullable = False)
    
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
    if value == None:
        error(field, "Is None")
    elif not value.date() < datetime.utcnow().date():
        error(field, "Must be passed data")
        
def has_no_duplicates(field, value, error):
    if value == None:
        error(field, "Is None")
    elif not len(value) == len(list(set(value))):
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
                    'required': True,
                    'nullable': False
                    },
                    'town': {
                        'type': 'string',
                        'minlength': 1,
                        'maxlength': 256,
                        'regex': '(?=.*?([ёЁ0-9А-Яа-яA-Za-z])).+',
                        'required': True,
                        'nullable': False
                        
                    },
                    'street': {
                        'type': 'string',
                        'minlength': 1,
                        'maxlength': 256,
                        'regex': '(?=.*?([ёЁ0-9А-Яа-яA-Za-z])).+',
                        'required': True,
                        'nullable': False
                    },
                    'building': {
                        'type': 'string',
                        'minlength': 1,
                        'maxlength': 256,
                        'regex': '(?=.*?([ёЁ0-9А-Яа-яA-Za-z])).+',
                        'required': True,
                        'nullable': False
                    },
                    'apartment': {
                        'type': 'integer',
                        'min': 0,
                        'required': True,
                        'nullable': False
                    },
                    'name': {
                        'type': 'string',
                        'minlength': 1,
                        'maxlength': 256,
                        'regex': "^[.\-\'Ёё а-яА-Яa-zA-Z\\s]+",
                        'required': True,
                        'nullable': False
                    },
                    'birth_date': {
                        'type': 'datetime',
                        'coerce': lambda s: datetime.strptime(s, '%d.%m.%Y'),
                        'validator': is_passed,
                        'required': True,
                        'nullable': False
                    },
                    'gender': {
                        'type': 'string',
                        'allowed': ['male', 'female'],
                        'required': True,
                        'nullable': False
                    },
                    'relatives': {
                        'type': 'list',
                        'validator': has_no_duplicates,
                        'schema': {
                            'type': 'integer',
                            'min' : 0,
                            'nullable': False
                        },
                        'required': True,
                        'nullable': False
                    }
                },
                'required': True,
                'nullable': False
                
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
                'regex': '(?=.*?([ёЁ0-9А-Яа-яA-Za-z])).+',
                'nullable': False
                
            },
            'street': {
                'type': 'string',
                'minlength': 1,
                'maxlength': 256,
                'regex': '(?=.*?([ёЁ0-9А-Яа-яA-Za-z])).+',
                'nullable': False
            },
            'building': {
                'type': 'string',
                'minlength': 1,
                'maxlength': 256,
                'regex': '(?=.*?([ёЁ0-9А-Яа-яA-Za-z])).+',
                'nullable': False
            },
            'apartment': {
                'type': 'integer',
                'min': 0,
                'nullable': False
            },
            'name': {
                'type': 'string',
                'minlength': 1,
                'maxlength': 256,
                'regex': "^[.\-\'Ёё а-яА-Яa-zA-Z\\s]+",
                'nullable': False
            },
            'birth_date': {
                'type': 'datetime',
                'coerce': lambda s: datetime.strptime(s, '%d.%m.%Y'),
                'validator': is_passed,
                'nullable': False
            },
            'gender': {
                'type': 'string',
                'allowed': ['male', 'female'],
                'nullable': False
            },
            'relatives': {
                'type': 'list',
                'validator': has_no_duplicates,
                'schema': {
                    'type': 'integer',
                    'min' : 0,
                    'nullable': False
                },
                'nullable': False
            }
        },
        'required': True,
        'nullable': False                
    }
}

# Import Schema

class ImportSchema(ma.Schema):
    class Meta:
        fields = ('import_id',)
        
# Tie Schema
        
class TieSchema(ma.Schema):
    class Meta:
        fields = ('import_id', 'first_citizen_id', 'second_citizen_id', 'relatives')
        
# Citizen Schema
class Relatives(ma.Field):
    def _serialize(self, value, attr, obj):
        if value:
            return [int(i) for i in value.split(',')]
        else:
            return []
            
class Birth_date(ma.Field):
    def _serialize(self, value, attr, obj):
        if value:
            return value.strftime('%d.%m.%Y')

class CitizenSchema(ma.Schema):
    relatives = Relatives()
    birth_date = Birth_date()
    class Meta:
        fields = ('citizen_id', 'town', 'street', 'building', 'apartment', 'name', 'birth_date', 'gender', 'relatives')
        
class Birth_info_Schema(ma.Schema):
    class Meta:
        fields = ('citizen_id', 'number_of_relatives', 'birth_month')
        
class Age_info_Schema(ma.Schema):
    class Meta:
        fields = ('town', 'birth_dates')
        
# Init Schema

import_schema = ImportSchema()
imports_schema = ImportSchema(many = True)

tie_schema = TieSchema()
ties_schema = TieSchema(many = True)

citizen_schema = CitizenSchema()
citizens_schema = CitizenSchema(many = True)

birth_info_schema = Birth_info_Schema()
births_info_schema = Birth_info_Schema(many = True)

age_info_schema = Age_info_Schema()
ages_info_schema = Age_info_Schema(many = True)


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
            
            citizens_ids = []
            
            for citizen in request.json:
                
                citizen_id = citizen['citizen_id']
                citizens_ids.append(citizen_id)
                
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
                
            # Check if there is now duplicate citizen_ids
            
            if not len(citizens_ids) == len(list(set(citizens_ids))):
                db.session.rollback()
                db.session.query(Import).filter(Import.import_id == import_id).delete()
                db.session.commit()
                return '', 400
                
            # Check if family ties are valid
            
            for citizen in family_ties.keys():
                for relative in family_ties[citizen]:
                    if relative not in family_ties.keys() or citizen not in family_ties[relative]:
                        db.session.rollback()
                        db.session.query(Import).filter(Import.import_id == import_id).delete()
                        db.session.commit()
                        return '', 400
                
            db.session.commit()
                
            return make_response(jsonify({'data': {'import_id': import_id}}), 201)
        else:
            db.session.rollback()
            db.session.commit()
            return 'oops', 400
               
api.add_resource(import_data, '/imports')

# change data

class change_data(Resource):
    def patch(self, import_id, citizen_id):
        validator = Validator()
        is_valid = (db.session.query(Citizen).filter(Citizen.import_id == import_id, Citizen.citizen_id == citizen_id).first() != None) and\
                   (validator.validate({'dict':request.json}, patch_validation_schema))
        if is_valid:
        
            # process new relatives information
            
            if 'relatives' in request.json.keys():
                                
                new_relatives = set(request.json['relatives'])
                
                possible_relatives = [i['citizen_id'] for i in citizens_schema.dump(db.session.query(Citizen.citizen_id).filter(Citizen.import_id == import_id).all())]

                for new_relative in new_relatives:
                    if new_relative not in possible_relatives:
                        return '', 400
                
                old_relatives = db.session.query(func.group_concat(Tie.second_citizen_id).label('relatives')).filter(Tie.import_id == import_id, Tie.first_citizen_id == citizen_id).group_by(Tie.import_id, Tie.first_citizen_id).first()
                old_relatives = tie_schema.dump(old_relatives).get('relatives', '')
                if old_relatives != '':
                    old_relatives = set([int(i) for i in old_relatives.split(',')])
                else:
                    old_relatives = set([])
                    
                to_delete = old_relatives - new_relatives
                to_insert = new_relatives - old_relatives
                
                if len(to_delete) != 0:
                    db.session.query(Tie).filter(or_(and_(Tie.import_id == import_id, Tie.first_citizen_id == citizen_id, Tie.second_citizen_id.in_(to_delete)), and_(Tie.import_id == import_id, Tie.second_citizen_id == citizen_id, Tie.first_citizen_id.in_(to_delete)))).delete(synchronize_session=False)
                    
                if len(to_insert) != 0:
                    for relative in to_insert:
                        first_tie = Tie(import_id, citizen_id, relative)                    
                        db.session.add(first_tie)
                        
                        if relative != citizen_id:
                            second_tie = Tie(import_id, relative, citizen_id)                    
                            db.session.add(second_tie)
                
                db.session.commit()
                
                request.json.pop('relatives')
                
            # process othe new information
                
            if len(request.json.keys()) != 0:
                
                db.session.query(Citizen).filter(Citizen.import_id == import_id, Citizen.citizen_id == citizen_id).update(request.json)
                 
                db.session.commit()
            
            citizen = db.session.query(Citizen.import_id, Citizen.citizen_id, Citizen.town, Citizen.street, Citizen.building, Citizen.apartment, Citizen.name, Citizen.birth_date, Citizen.gender, func.group_concat(Tie.second_citizen_id).label('relatives'))\
            .filter(Citizen.import_id == import_id)\
            .outerjoin(Tie, and_(Citizen.citizen_id==Tie.first_citizen_id, Citizen.import_id==import_id, Tie.import_id==import_id, Citizen.citizen_id == citizen_id, Tie.first_citizen_id == citizen_id))\
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
        is_valid_id = (db.session.query(Import).filter(Import.import_id == import_id).first() != None)
        if is_valid_id:
            citizens = db.session.query(Citizen.import_id, Citizen.citizen_id, Citizen.town, Citizen.street, Citizen.building, Citizen.apartment, Citizen.name, Citizen.birth_date, Citizen.gender, func.group_concat(Tie.second_citizen_id).label('relatives'))\
            .filter(Citizen.import_id == import_id)\
            .outerjoin(Tie, and_(Citizen.citizen_id==Tie.first_citizen_id, Citizen.import_id==import_id, Tie.import_id==import_id))\
            .group_by(Citizen.import_id, Citizen.citizen_id, Citizen.town, Citizen.street, Citizen.building, Citizen.apartment, Citizen.name, Citizen.birth_date, Citizen.gender)
            result = citizens_schema.dump(citizens)
            return make_response(jsonify({'data': result}), 200)
        else:
            return '', 400
            
api.add_resource(get_data, '/imports/<int:import_id>/citizens')

# get birthdays info

class get_birthdays(Resource):
    def get(self, import_id):
        is_valid_id = (db.session.query(Import).filter(Import.import_id == import_id).first() != None)
        if is_valid_id:
            ties = db.session.query(Tie.first_citizen_id, func.count().label('number_of_relatives'), func.extract('month', Citizen.birth_date).label('birth_month'))\
            .join(Citizen, and_(Citizen.citizen_id == Tie.second_citizen_id, Citizen.import_id == import_id, Tie.import_id == import_id))\
            .group_by(Tie.first_citizen_id, 'birth_month').subquery()
            
            ties = db.session.query(ties.c.birth_month, func.group_concat(ties.c.first_citizen_id).label('citizen_id'),func.group_concat(ties.c.number_of_relatives).label('number_of_relatives'))\
            .group_by('birth_month')
            
            result = {str(i): [] for i in range(1,13)}
            for month in births_info_schema.dump(ties):
                for citizen, count in zip(month['citizen_id'].split(','), month['number_of_relatives'].split(',')):
                    result[str(month['birth_month'])].append({'citizen_id': int(citizen), 'presents': int(count)})
                    
            return make_response(jsonify({'data':result}), 200)
        else:
            return '', 400
        
api.add_resource(get_birthdays, '/imports/<int:import_id>/citizens/birthdays')

# get age distribution info

class get_age(Resource):
    def get(self, import_id ):
        is_valid_id = (db.session.query(Import).filter(Import.import_id == import_id).first() != None)
        if is_valid_id:
            ages = db.session.query(Citizen.town, func.group_concat(Citizen.birth_date).label('birth_dates')).filter(Citizen.import_id == import_id)\
            .group_by(Citizen.town)
            
            result = []
            
            current_date = datetime.utcnow()
            
            for town in ages_info_schema.dump(ages):
                town_name = town['town']
                birth_dates = [datetime.strptime(date, '%Y-%m-%d') for date in town['birth_dates'].split(',')]
                ages = [current_date.year - born.year - ((current_date.month, current_date.day) < (born.month, born.day)) for born in birth_dates]
                
                p50 = rnd(percentile(ages, 50), 2) 
                p75 = rnd(percentile(ages, 75), 2) 
                p99 = rnd(percentile(ages, 99), 2)
                
                result.append({'town': town_name,
                               'p50': p50,
                               'p75': p75,
                               'p99': p99})                
            
            return make_response(jsonify({'data': result}), 200)
        else:
            return '', 400
            
api.add_resource(get_age, '/imports/<int:import_id>/towns/stat/percentile/age')

#Run server

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)  
