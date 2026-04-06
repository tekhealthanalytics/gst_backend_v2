from django.db import connection
from django.http import HttpResponse,JsonResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view




from etymo.database import *

# def check_connection(request):
#     updatePassword('sawantsanket855@gmail.com','hPCHJFEmnhV03q74VX-HQv-ZL4layV2iTHnK3qWrIuU','Sanket@2146')
#     return JsonResponse({'message':'email already exist'})

@csrf_exempt
def get_word_data(request):
    words_list = request.GET.getlist('highlightedWords')
    print(words_list)
    words=tuple(words_list)
    # words=('aspiration','asthma','bradycardia')
    with connection.cursor() as cursor:
        cursor.execute(f"select * from gst_tbl_medical_terms where medical_term in {words}")
        rows=cursor.fetchall()
        return JsonResponse({'results':rows})
    
   
@api_view(['POST'])
def login_api(request):
    print(request.method)
    data=request.data
    message,token=login(data['email'],data['password'],data['loginType'])  #checks login credentials
    return JsonResponse({'message':message,'token':token})
  
    
@api_view(['POST'])
def register_api(request):
    data=request.data
    result=register(data['username'],data['email'],data['password'])
    return JsonResponse({'message':result})

@api_view(['POST'])   
def sendOTP_api(request):
    print('sending email')
    data=request.data
    result=sendOTP(data['email'])
    return JsonResponse({'message':result})

@api_view(['POST'])
def verifyOTP_api(request):
    data=request.data
    result=verifyOTP(data['email'],data['otp'])
    if result[0]=='correct otp':
        return JsonResponse({'message':"correct otp",'token':result[1],'login_type':result[2]})
    return JsonResponse({'message':result})

@api_view(['POST'])
def sendPasswordResetEmail_api(request):
    data=request.data
    result=sendPasswordResetEmail(data['email'])
    return JsonResponse({'message':result})

@api_view(['POST'])    
def updatePassword_api(request):
    data=request.data
    result=updatePassword(data['email'],data['reset_token'],data['password'])
    return JsonResponse({'message':result})
    
@api_view(['POST'])    
def submit_request_api(request):
    print('in function')
    data=request.data
    print(data)
    documents= request.FILES.getlist('documents')
    print(documents)
    name=request.POST.get('name')
    type=request.POST.get('type')
    email=request.POST.get('email')
    mobile=request.POST.get('mobile')
    description=request.POST.get('description')
    token=request.POST.get('token')
    doc_status = request.POST.get('doc_status', 'complete')
    print('calling')
    response= submit_request(name,type,email,mobile,description,documents,token, doc_status)
    print('called')
    print(response)
    return JsonResponse({'message':response})

@api_view(['POST'])    
def get_request_document_api(request):
    data=request.data
    print(data)
    try:
        print(' get_request_document_api')
        response= get_request_document(data['id'])

        print(f'data having {len(response)} data')
        return JsonResponse({'result':response})
    except Exception as e:
        print('api call error')
        print(e)
    
@api_view(['POST'])    
def get_request_data_api(request):
    data=request.data
    try:
        print('in function get_request_data_api')
        response,message= get_request_data(data['token'])
        response= JsonResponse({'result':response,'message':message})
        return response
    except Exception as e:
        print('api call error')
        print(e)
        return('server error')
        

# @api_view(['POST'])    
# def get_ca_cs_data_api(request):
#     data=request.data
#     try:
#         print('in function get_ca_cs_data_api')
#         response,message= get_ca_cs_data(data['token'])
#         print(f'message{message}')
#         response= JsonResponse({'result':response,'message':message})
#         return response
#     except Exception as e:
#         print('api call error')
#         print(e)
#         return('server error')



@api_view(['POST'])    
def get_request_document_data_api(request):
    data=request.data
    print(data)
    print(f'got id =',data['id'])
    try:
        print('in function get_request_document_data_api')
        response= get_request_document_data(data['id'])
        print(response)
        file_data=response[1]
        if isinstance(file_data, memoryview):
            file_data = bytes(file_data)
            print('converted to bytes')
        response= HttpResponse(file_data,content_type=response[0])
        return response
    except Exception as e:
        print('api call error')
        print(e)


@api_view(['POST'])    
def ca_cs_registartion_api(request):
    print('in function ca_cs_registartion_api')
    data=request.data
    print(data)
    certificate= request.FILES.getlist('certificate')
    IdProof= request.FILES.getlist('IdProof')
    print(certificate)
    print(IdProof)
    response= ca_cs_registartion(data,[certificate[0],IdProof[0]])
    return JsonResponse({'message':response})

@api_view(['POST'])
def update_ca_cs_api(request):
    try:
        data = request.data
        ca_cs_id = data['id']
        certificate = request.FILES.get('certificate')
        id_proof = request.FILES.get('idProof')
        message = update_ca_cs(ca_cs_id, data, certificate, id_proof)
        return JsonResponse({'message': message})
    except Exception as e:
        print(f"API Error in update_ca_cs_api: {e}")
        return JsonResponse({'message': 'error'})

@api_view(['POST'])
def update_request_status_api(request):
    try:
        data=request.data
        print(data)
        response= update_request_status(data['requestID'],data['requestStatus'],data['requestInstruction'])
        return JsonResponse({'message':response})
    except Exception as e:
        print('error',e)
        return JsonResponse({'message':'server error'})
    

@api_view(['POST'])    
def get_ca_cs_data_api(request):
    data = request.data
    try:
        available_now = data.get('available_now', False)
        print(f'get_ca_cs_data_api, available_now: {available_now}')
        response, message = get_ca_cs_data(data['token'], available_now)
        print(response, message)
        response = JsonResponse({'result': response, 'message': message})
        return response
    except Exception as e:
        print('api call error')
        print(e)
        return JsonResponse({'result': [], 'message': 'server error'})




@api_view(['POST'])
def assign_ca_cs_api(request):
    try:
        data=request.data
        print(data)
        response= assign_ca_cs(ca_cs_id=data['ca_cs_id'],requestId=data['request_id'])
        return JsonResponse({'result':response})
    except Exception as e:
        print('error',e)
        return JsonResponse({'result':'server error'})


@api_view(['POST'])    
def get_verified_request_data_api(request):
    try:
        print('get_verified_request_data_api')
        response= get_verified_request_data()
        response= JsonResponse({'result':response})
        return response
    except Exception as e:
        print('api call error')
        print(e)

        

@api_view(['POST'])    
def submit_payment_request_api(request):
    print('in submit_payment_request_api')
    data=request.data
    print(data)
    documents= request.FILES.getlist('documents')
    print(documents)
    name=request.POST.get('name')
    amount=request.POST.get('amount')
    paymentMethod=request.POST.get('paymentMethod')
    bankName=request.POST.get('bankName')
    accountNumber=request.POST.get('accountNumber')
    ifscCode=request.POST.get('ifscCode')
    upiId=request.POST.get('upiId')
    token=request.POST.get('token')
    response= submit_payment_request(name,amount,paymentMethod,bankName,accountNumber,ifscCode,upiId,documents,token)
    return JsonResponse({'message':response})



@api_view(['POST'])    
def get_payment_request_data_api(request):
    data =request.data
    try:
        print('in function get_payment_request_data_api')
        response= get_payment_request_data(data['token'])
        response= JsonResponse({'result':response[0],'message':response[1]})
        return response
    except Exception as e:
        print('api call error')
        print(e)


@api_view(['POST'])    
def get_payment_request_document_api(request):
    data=request.data
    print(data)
    try:
        print(' get_request_document_api')
        response= get_payment_request_document(data['id'])
        print(f'data having {len(response)} data')
        return JsonResponse({'result':response})
    except Exception as e:
        print('api call error')
        print(e)


@api_view(['POST'])    
def get_payment_request_document_data_api(request):
    data=request.data
    print(data)
    print(f'got id =',data['id'])
    try:
        print('in function get_payment_request_document_data_api')
        response= get_payment_request_document_data(data['id'])
        print(response)
        file_data=response[1]
        if isinstance(file_data, memoryview):
            file_data = bytes(file_data)
            print('converted to bytes')
        response= HttpResponse(file_data,content_type=response[0])
        return response
    except Exception as e:
        print('api call error')
        print(e)


@api_view(['POST'])
def update_payment_request_status_api(request):
    try:
        data=request.data
        print(data)
        response= update_payment_request_status(data['paymentRequestID'],data['requestInstruction'])
        return JsonResponse({'message':response})
    except Exception as e:
        print('error',e)
        return JsonResponse({'message':'server error'})


@api_view(['POST'])
def reject_payment_request_api(request):
    try:
        data = request.data
        print(data)
        response = reject_payment_request(data['paymentRequestID'], data.get('rejectReason', 'Rejected by admin'))
        return JsonResponse({'message': response})
    except Exception as e:
        print('error in reject_payment_request_api', e)
        return JsonResponse({'message': 'server error'})


@api_view(['POST'])
def admin_pay_amount_api(request):
    try:
        data = request.data
        print('admin_pay_amount_api', data)
        response = admin_pay_amount(
            data['requestId'],
            data['amount'],
            data['paymentMethod'],
            data.get('transactionId', ''),
            data.get('notes', '')
        )
        return JsonResponse({'message': response})
    except Exception as e:
        print('error in admin_pay_amount_api', e)
        return JsonResponse({'message': 'server error'})

@api_view(['POST'])    
def get_ca_cs_document_api(request):
    data=request.data
    print(data)
    try:
        print(' get_ca_cs_document_api')
        response= get_ca_cs_document(data['id'])

        print(f'data having {len(response)} data')
        return JsonResponse({'result':response})
    except Exception as e:
        print('api call error')
        print(e)

@api_view(['POST'])    
def get_ca_cs_document_data_api(request):
    data=request.data
    print(data)
    print(f'got id =',data['id'])
    try:
        print('in function get_ca_cs_document_data_api')
        response= get_ca_cs_document_data(data['id'])
        print(response)
        file_data=response[1]
        if isinstance(file_data, memoryview):
            file_data = bytes(file_data)
            print('converted to bytes')
        response= HttpResponse(file_data,content_type=response[0])
        return response
    except Exception as e:
        print('api call error')
        print(e)

@api_view(['POST'])    
def get_agent_balance_api(request):
    data=request.data
    try:
        print('in function get_agent_balance_api')
        response,balance= get_agent_balance(data['token'])
        response= JsonResponse({'result':response,'balance':balance})
        return response
    except Exception as e:
        print('api call error')
        print(e)
        return  JsonResponse({'result':'server error','balance':0})
    

@api_view(['POST'])    
def get_transaction_data_api(request):
    data =request.data
    try:
        print('in function get_transaction_data_api')
        response= get_transaction_data(data['token'])
        response= JsonResponse({'result':response[0],'message':response[1]})
        return response
    except Exception as e:
        print('api call error')
        print(e)



@api_view(['POST'])    
def complete_request_api(request):
    print('in complete_request_api')
    data=request.data
    print(data)
    documents= request.FILES.getlist('documents')
    print(documents)
    description=request.POST.get('description')
    token=request.POST.get('token')
    request_id=request.POST.get('request_id')
    print('complete_request_api call')
    response= complete_request(request_id,description,documents,token)
    print('called complete_request')
    print(response)
    return JsonResponse({'message':response})

@api_view(['POST'])    
def get_request_completion_document_api(request):
    data=request.data
    print(data)
    try:
        print(' get_request_completion_document_api')
        response= get_request_completion_document(data['id'])
        print(f'data having {len(response)} data')
        return JsonResponse({'result':response})
    except Exception as e:
        print('api call error')
        print(e)


@api_view(['POST'])    
def get_request_completion_document_data_api(request):
    data=request.data
    print(data)
    print(f'got id =',data['id'])
    try:
        print('in function get_request_document_data_api')
        response= get_request_completion_document_data(data['id'])
        print(response)
        file_data=response[1]
        if isinstance(file_data, memoryview):
            file_data = bytes(file_data)
            print('converted to bytes')
        response= HttpResponse(file_data,content_type=response[0])
        return response
    except Exception as e:
        print('api call error')
        print(e)

@api_view(['POST'])
def get_agent_data_list_api(request):
    data =request.data
    try:
        print('in function get_agent_data_list_api')
        response= get_agent_data_list(data['token'])
        response= JsonResponse({'result':response[0],'message':response[1]})
        return response
    except Exception as e:
        print('api call error in get_agent_data_list_api')
        print(e)
        return JsonResponse({'result': [], 'message': 'server error'})

@api_view(['GET'])
def get_services_api(request):
    try:
        data, message = get_services()
        return JsonResponse({'result': data, 'message': message})
    except Exception as e:
        print(f"API Error in get_services_api: {e}")
        return JsonResponse({'result': [], 'message': 'error'})

@api_view(['POST'])
def add_service_api(request):
    try:
        data = request.data
        message = add_service(data['name'], data['price'], data.get('required_documents', ''), data.get('category_id'))
        return JsonResponse({'message': message})
    except Exception as e:
        print(f"API Error in add_service_api: {e}")
        return JsonResponse({'message': 'error'})

@api_view(['POST'])
def update_service_api(request):
    try:
        data = request.data
        message = update_service(data['id'], data['name'], data['price'], data.get('required_documents', ''), data.get('category_id'))
        return JsonResponse({'message': message})
    except Exception as e:
        print(f"API Error in update_service_api: {e}")
        return JsonResponse({'message': 'error'})

@api_view(['GET'])
def get_service_categories_api(request):
    try:
        data, message = get_service_categories()
        return JsonResponse({'result': data, 'message': message})
    except Exception as e:
        print(f"API Error in get_service_categories_api: {e}")
        return JsonResponse({'result': [], 'message': 'error'})

@api_view(['POST'])
def add_service_category_api(request):
    try:
        data = request.data
        message = add_service_category(data['name'])
        return JsonResponse({'message': message})
    except Exception as e:
        print(f"API Error in add_service_category_api: {e}")
        return JsonResponse({'message': 'error'})

@api_view(['POST'])
def update_service_category_api(request):
    try:
        data = request.data
        message = update_service_category(data['id'], data['name'])
        return JsonResponse({'message': message})
    except Exception as e:
        print(f"API Error in update_service_category_api: {e}")
        return JsonResponse({'message': 'error'})

@api_view(['POST'])
def delete_service_category_api(request):
    try:
        data = request.data
        message = delete_service_category(data['id'])
        return JsonResponse({'message': message})
    except Exception as e:
        print(f"API Error in delete_service_category_api: {e}")
        return JsonResponse({'message': 'error'})

@api_view(['POST'])
def delete_service_api(request):
    try:
        data = request.data
        message = delete_service(data['id'])
        return JsonResponse({'message': message})
    except Exception as e:
        print(f"API Error in delete_service_api: {e}")
        return JsonResponse({'message': 'error'})

@api_view(['POST'])
def get_ca_cs_slots_api(request):
    try:
        data = request.data
        response, message = get_ca_cs_slots(data['ca_cs_id'])
        return JsonResponse({'result': response, 'message': message})
    except Exception as e:
        print(f"API Error in get_ca_cs_slots_api: {e}")
        return JsonResponse({'message': 'error'})

@api_view(['POST'])
def update_ca_cs_slots_api(request):
    try:
        data = request.data
        message = update_ca_cs_slots(data['ca_cs_id'], data['slots'])
        return JsonResponse({'message': message})
    except Exception as e:
        print(f"API Error in update_ca_cs_slots_api: {e}")
        return JsonResponse({'message': 'error'})

@api_view(['POST'])
def get_ca_cs_special_slots_api(request):
    try:
        data = request.data
        response, message = get_ca_cs_special_slots(data['ca_cs_id'])
        return JsonResponse({'result': response, 'message': message})
    except Exception as e:
        print(f"API Error in get_ca_cs_special_slots_api: {e}")
        return JsonResponse({'message': 'error'})

@api_view(['POST'])
def update_ca_cs_special_slots_api(request):
    try:
        data = request.data
        message = update_ca_cs_special_slots(data['ca_cs_id'], data['date'], data['slots'])
        return JsonResponse({'message': message})
    except Exception as e:
        print(f"API Error in update_ca_cs_special_slots_api: {e}")
        return JsonResponse({'message': 'error'})





@api_view(['POST'])
def get_my_cacs_data_api(request):
    try:
        data = request.data
        result, message = get_my_cacs_data(data['token'])
        return JsonResponse({'result': result, 'message': message})
    except Exception as e:
        print(f"API Error in get_my_cacs_data_api: {e}")
        return JsonResponse({'result': None, 'message': 'error'})

@api_view(['POST'])
def update_admin_bank_details_api(request):
    try:
        data = request.POST
        upi_qr_code = request.FILES.get('upiQRCode')
        message = update_admin_bank_details(
            data.get('bankName', ''),
            data.get('accountName', ''),
            data.get('accountNumber', ''),
            data.get('ifscCode', ''),
            data.get('upiId', ''),
            data.get('token', ''),
            upi_qr_code=upi_qr_code
        )
        return JsonResponse({'message': message})
    except Exception as e:
        print(f"API Error in update_admin_bank_details_api: {e}")
        return JsonResponse({'message': 'error'})

@api_view(['GET'])
def get_admin_bank_details_api(request):
    try:
        result, message = get_admin_bank_details()
        return JsonResponse({'result': result, 'message': message})
    except Exception as e:
        print(f"API Error in get_admin_bank_details_api: {e}")
        return JsonResponse({'result': None, 'message': 'error'})

@api_view(['POST'])
def update_cacs_bank_details_api(request):
    try:
        data = request.data
        message = update_cacs_bank_details(
            data.get('bankName', ''),
            data.get('accountName', ''),
            data.get('accountNumber', ''),
            data.get('ifscCode', ''),
            data.get('upiId', ''),
            data.get('token', '')
        )
        return JsonResponse({'message': message})
    except Exception as e:
        print(f"API Error in update_cacs_bank_details_api: {e}")
        return JsonResponse({'message': 'error'})

@api_view(['POST'])
def get_cacs_bank_details_api(request):
    try:
        data = request.data
        result, message = get_cacs_bank_details(
            cacs_id=data.get('cacs_id'),
            token=data.get('token')
        )
        return JsonResponse({'result': result, 'message': message})
    except Exception as e:
        print(f"API Error in get_cacs_bank_details_api: {e}")
        return JsonResponse({'result': None, 'message': 'error'})

@api_view(['GET'])
def get_admin_bank_qr_code_api(request):
    try:
        response = get_admin_bank_qr_code_data()
        if not response or not response[1]:
            return HttpResponse(status=404)
        
        file_data = response[1]
        if isinstance(file_data, memoryview):
            file_data = bytes(file_data)
            
        return HttpResponse(file_data, content_type=response[0])
    except Exception as e:
        print(f"Error in get_admin_bank_qr_code_api: {e}")
        return HttpResponse(status=500)
