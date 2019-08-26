import requests
import time
import json
from datetime import datetime, timedelta
from faker import Faker
import random
import string
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import argparse

MAX_TIES = 1000
server_url = 'http://0.0.0.0:8080'

def strTimeProp(start, end, format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formated in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))


def randomDate(start, end, format='%d.%m.%Y'):
    return strTimeProp(start, end, format, random.random())

def get_citizen_json(faker, citizen_id):
    citizen = {}

    citizen['citizen_id'] = citizen_id

    address = faker.address().split(',')
    citizen['town'] = address[0]
    citizen['street'] = address[-3]
    citizen['building'] = address[-2]
    try:
        citizen['apartment'] = int(address[-1])
    except:
        print(address)
    
    citizen['birth_date'] = randomDate("1.1.1930", datetime.strftime(datetime.utcnow()- timedelta(days=1), '%d.%m.%Y'))

    citizen['gender'] = random.choice(['male', 'female'])
    if citizen['gender'] == 'male':
        citizen['name'] = faker.name_male()
    else:
        citizen['name'] = faker.name_female()

    citizen['relatives'] = []            
            
    return citizen

def make_citizen_json_invalid(citizen):
    string_fields = ['town', 'street', 'building', 'name', 'gender']
    int_fields = ['citizen_id', 'apartment']
    modifications = ['extra_field', 'delete_field', 'make_field_None', 'make_int_field_negative', 'make_string_field_overflow',\
                     'make_string_field_with_no_letters_or_digits','make_string_field_emty', 'make_gender_field_invalid',\
                     'make_invalid_date_format', 'make_unpassed_date', 'make_invalid_type']
    
    modification = random.choice(modifications)
    
    if modification == 'extra_field':
        random_string = ''.join(random.choices(string.ascii_letters, k=10))
        citizen[random_string] = random_string
        
    if modification == 'delete_field':
        citizen.pop(random.choice(list(citizen.keys())))
        
    if modification == 'make_field_None':
        citizen[random.choice(list(citizen.keys()))] = None
    
    if modification == 'make_int_field_negative':
        citizen[random.choice(int_fields)] = random.randint(-100, -1)
        
    if modification == 'make_string_field_overflow':
        citizen[random.choice(string_fields)] = ''.join(random.choices(string.ascii_letters, k=random.randint(257, 10000)))
        
    if modification == 'make_string_field_with_no_letters_or_digits':
        citizen[random.choice(string_fields)] = ''.join(random.choices(string.punctuation+string.whitespace, k=random.randint(1, 256)))
        
    if modification == 'make_string_field_emty':
        citizen[random.choice(string_fields)] = ''
    
    if modification == 'make_gender_field_invalid':
        citizen['gender'] = ''.join(random.choices(string.ascii_letters, k=random.randint(1, 256)))
        
    if modification == 'make_invalid_date_format':
        citizen['birth_date'] = randomDate("2008.1.1", datetime.strftime(datetime.utcnow()- timedelta(days=1), '%Y.%m.%d'), '%Y.%m.%d')
        
    if modification == 'make_unpassed_date':
        citizen['birth_date'] = randomDate(datetime.strftime(datetime.utcnow(), '%d.%m.%Y'),\
                                           datetime.strftime(datetime.utcnow()+timedelta(days=random.randint(1, 1000)), '%d.%m.%Y'))
    if modification == 'make_invalid_type':
        random_field = random.choice(list(citizen.keys()))
        if random_field in string_fields+['birth_date']:
            citizen[random_field] = random.randint(0, 1000)
        else:
            citizen[random_field] = ''.join(random.choices(string.ascii_letters, k=10))
        
    return citizen

def get_citizens_json(faker, size = 10, family_ties = 10, valid=True):
    citizens = [get_citizen_json(faker, i) for i in range(size)]
    family_ties = min(family_ties, size*(size-1))
    for i in range(family_ties):
        first_citizen = random.randint(0, size-1)
        second_citizen = random.randint(0, size-1)

        if second_citizen not in citizens[first_citizen]['relatives']:
            citizens[first_citizen]['relatives'].append(second_citizen)
        if first_citizen not in citizens[second_citizen]['relatives']:
            citizens[second_citizen]['relatives'].append(first_citizen)
                
    if valid == False:
        modifications = ['modify_citizen', 'modify_ties']
        modification = random.choice(modifications)
        citizen_to_proccess = random.choice(range(len(citizens)))
        if modification == 'modify_citizen':
            citizens[citizen_to_proccess] = make_citizen_json_invalid(citizens[citizen_to_proccess])
        else:
            if len(citizens[citizen_to_proccess]['relatives']) == 0:
                while True:
                    new_relative = random.randint(0, 1000)
                    if new_relative != citizen_to_proccess:
                        break
                        
                citizens[citizen_to_proccess]['relatives'] = [new_relative]
            else:
                citizens[citizen_to_proccess]['relatives'] = citizens[citizen_to_proccess]['relatives'][:-1]
            
    return citizens

#function to easily compare jsons

def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj
          
def post_and_get_requests_test_worker(faker, n_requests = 10, n_citizen_per_request = 10, p_of_wrong_request = 0.5):
    for r in range(n_requests):
        
        is_request_wrong = random.choices([True, False], weights=[p_of_wrong_request, 1-p_of_wrong_request])[0]
        if is_request_wrong == True:
            data = get_citizens_json(faker, n_citizen_per_request, int(random.random()*MAX_TIES), False)
        else:
            data = get_citizens_json(faker, n_citizen_per_request, int(random.random()*MAX_TIES), True)
                                     
        post_response = requests.post(server_url+'/imports', json=data)
        if is_request_wrong == True:
            assert post_response.status_code == 400
        else:
            assert post_response.status_code == 201
            import_id = post_response.json()['data']['import_id']
            get_response = requests.get(server_url+'/imports/'+str(import_id)+'/citizens')
            assert get_response.status_code == 200
            assert ordered(get_response.json()['data']) == ordered(data)

def post_and_get_requests_test(requests_per_worker = 10, simultaneous_requests = 2, n_citizen_per_request = 10, p_of_wrong_request = 0.5):
    faker = Faker('ru_RU')
    with ThreadPoolExecutor(max_workers=simultaneous_requests) as pool:
        futures = {pool.submit(post_and_get_requests_test_worker, faker, requests_per_worker, n_citizen_per_request, p_of_wrong_request):i for i in range(simultaneous_requests)}
        for future in (concurrent.futures.as_completed(futures)):
            try:
                data = future.result()
                print('%r worker: Tests passed' % futures[future])
            except:
                print('%r worker: Tests did not pass ' % futures[future]) 
   
if __name__ == '__main__':            
    parser = argparse.ArgumentParser(description='REST API test app')
    
    parser.add_argument('rpw', type=int, nargs='?', help='Number of requests to send per worker')
    parser.add_argument('nw', type=int, nargs='?', help='Number of workers')
    parser.add_argument('ncr', type=int, nargs='?', help='Number of citizens in post request json')
    parser.add_argument('puvr', type=float, nargs='?', help='Probability of unvalid request')
    
    args = parser.parse_args()
    requests_per_worker = args.rpw
    simultaneous_requests = args.nw
    n_citizen_per_request = args.ncr
    p_of_wrong_request = args.puvr
    
    post_and_get_requests_test(requests_per_worker, simultaneous_requests, n_citizen_per_request, p_of_wrong_request)
    post_and_get_requests_test    
