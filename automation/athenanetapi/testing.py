

import athenahealthapi
import pprint
import datetime
# from utilities.read_write_utilities import write_to_csv, read_csv

####################################################################################################
# Setup
####################################################################################################
key = 'ycxghhrqecd35qyhk7eurrmt'
secret = 'B4JbfXHQGsjhUx3'
version = 'preview1'
practiceid = 195900 #sandbox practiceid
# practiceid = 1959388
write_out_dir = '/Users/sandeep.dey/Documents/data/athenanetapi'
today = datetime.date.today()
lastyear = datetime.date.today() - datetime.timedelta(days=30)
dateformat = '%m/%d/%Y'

api = athenahealthapi.APIConnection(version, key, secret, practiceid)

# If you want to change which practice you're working with after initialization, this is how.
# api.practiceid = 195900

# Before we start, here's a useful function.
def path_join(*args):
    return ''.join('/' + str(arg).strip('/') for arg in args if arg)  

pp = pprint.PrettyPrinter(indent=4)
def print_response(obj):
    pp.pprint(obj)

def get_departments():
    departments = api.GET('/departments', {
        'showalldepartments': True
    })
    print_response(departments)
    # write_to_csv(write_out_dir+'/departments.csv',departments['departments'])
    return departments['departments']


def get_appointments(department):

    department_id = department['departmentid']
    booked_appts = api.GET('/appointments/booked' , {
        'startdate': lastyear.strftime(dateformat),
        'enddate': today.strftime(dateformat),
        'departmentid': department_id
    })
    return booked_appts['appointments']
    # write_to_csv(write_out_dir + '/appointments.csv', booked_appts['appointments'])
    print_response(booked_appts)

def get_patient_pharmacies(patientid,departmentid):
    preferred_pharmacies = api.GET('/chart/%s/pharmacies/preferred'%patientid,{
        'departmentid' : departmentid
    })
    print_response(preferred_pharmacies)
    header = ['acceptfax','address1','address2','city','clinicalproviderid','clinicalprovidername','faxnumber','phonenumber','state','zip']
    # write_to_csv(filename=write_out_dir+'/pharmacies.csv', data=preferred_pharmacies['pharmacies'], header=header)

def get_patient_insurances(patientid,departmentid):
    insurances = api.GET('/patients/%s/insurances'%patientid,{
        'departmentid' : departmentid
    })
    print_response(insurances)
    # write_to_csv(filename=write_out_dir+'/insurances.csv', data=insurances['insurances'])

def get_patient_default_pharmacy(patientid,departmentid):
    preferred_pharmacies = api.GET('/chart/%s/pharmacies/default'%patientid,{
        'departmentid' : departmentid
    })
    print_response(preferred_pharmacies)

def put_patient_default_pharmacy(patientid,departmentid,):
    preferred_pharmacies = api.PUT('/chart/%s/pharmacies/default'%patientid,{
        'departmentid' : departmentid,
        'ncpdpid' : 6008925
    })
    print_response(preferred_pharmacies)


def get_patient_medications(patientid,departmentid):
    medications = api.GET('/chart/%s/medications'%patientid,{
        'departmentid' : departmentid
    })
    header = ['clinicalordertypeid','fdbmedicationid','issafetorenew','medication','medicationentryid','medicationid','source','therapeuticclass','unstructuredsig']
    # print_response(medications)
    flattened_med_list = [item for sublist in  medications['medications'] for item in sublist]
    # write_to_csv(filename=write_out_dir+'/medications.csv', data=flattened_med_list, header=header)
    return flattened_med_list





####################################################################################################
# GET without parameters
####################################################################################################
# customfields = api.GET('/customfields')

# print('Custom fields:', list(customfields[0].keys()))


####################################################################################################
# GET with parameters
####################################################################################################


# departments = get_departments()
# appointments = [item for department in departments for item in get_appointments(department)]
# write_to_csv(write_out_dir + '/appointments.csv', appointments)

# data = read_csv(write_out_dir + '/appointments.csv')

# get_patient_default_pharmacy(1,21)

# put_patient_default_pharmacy(1,21)
# get_patient_insurances(1,21)

# open_appts = api.GET('/appointments/open', {
#     'departmentid': 82,
#     'startdate': lastyear.strftime(dateformat),
#     'enddate': today.strftime(dateformat),
#     'appointmenttypeid': 2,
#     'limit': 1,
# })

# print(open_appts)


# booked_appts = api.GET('/appointments/booked' , {
#     'startdate': lastyear.strftime(dateformat),
#     'enddate': today.strftime(dateformat),
#     'departmentid': 82
# })
# print_response(booked_appts)

# appt = open_appts['appointments'][0]
# print('Open appointment:', appt)

# change the keys in appt to make it usable in scheduling
# appt['appointmenttime'] = appt.pop('starttime')
# appt['appointmentdate'] = appt.pop('date')





# patients = api.GET('/patients/enhancedbestmatch' , {
#     'firstname' : 'john'
# })
# print_response(patients)



####################################################################################################
# POST with parameters
####################################################################################################
# patient_info = {
#     'lastname': 'Foo',
#     'firstname': 'Jason',
#     'address1': '123 Any Street',
#     'city': 'Cambridge',
#     'countrycode3166': 'US',
#     'departmentid': 1,
#     'dob': '6/18/1987',
#     'language6392code': 'declined',
#     'maritalstatus': 'S',
#     'race': 'declined',
#     'sex': 'M',
#     'ssn': '*****1234',
#     'zip': '02139',
# }

# new_patient = api.POST('/patients', patient_info)
#
# new_patient_id = new_patient[0]['patientid']
# print('New patient id:', new_patient_id)


####################################################################################################
# PUT with parameters
####################################################################################################
# appointment_info = {
#     'appointmenttypeid': 82,
#     'departmentid': 1,
#     'patientid': new_patient_id,
# }
#
# booked = api.PUT(path_join('/appointments', appt['appointmentid']), appointment_info)
# print('Response to booking appointment:', booked)


####################################################################################################
# POST without parameters
####################################################################################################
# checked_in = api.POST(path_join('/appointments', appt['appointmentid'], '/checkin'))
# print('Response to check-in:', checked_in)


####################################################################################################
# DELETE with parameters
####################################################################################################
# removed_chart_alert = api.DELETE(path_join('/patients', new_patient_id, 'chartalert'), {'departmentid': 1})
# print('Removed chart alert:', removed_chart_alert)


####################################################################################################
# DELETE without parameters
####################################################################################################
# removed_appointment = api.DELETE(path_join('/appointments', appt['appointmentid']))
# print('Removed appointment:', removed_appointment)

####################################################################################################
# There are no PUTs without parameters
####################################################################################################


####################################################################################################
# Error conditions
####################################################################################################
# bad_path = api.GET('/nothing/at/this/path')
# print('GET /nothing/at/this/path:', bad_path['error'])
# missing_parameters = api.GET('/appointments/open')
# print('Response to missing parameters:', missing_parameters['error'], missing_parameters['missingfields'])


####################################################################################################
# Testing refresh tokens
####################################################################################################

# NOTE: This test takes an hour to run, so it's disabled by default. Change False to True to run it.
# if False:
#     import time
#     import sys
#
#     oldtoken = api.get_token()
#     print('Old token:', oldtoken)
#
#     before_refresh = api.GET('/departments')
#
#     # Wait 3600 seconds = 1 hour for token to expire.
#     time.sleep(3600)
#
#     after_refresh = api.GET('/departments')
#
#     print('New token:', api.get_token())
