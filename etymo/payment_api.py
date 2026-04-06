
from django.http import HttpResponse,JsonResponse
from rest_framework.decorators import api_view

from etymo.payment_database import razorpay_create_request, razorpay_payment_data

@api_view(['POST'])
def razorpay_create_request_api(request):
    try:
        data =request.data
        response=razorpay_create_request(data['token'],data['amount'])
        if(response[0]=='success'):
            return JsonResponse({'message':response[0],'amount':response[1],'order_id':response[2]})
        else:
            return JsonResponse({'message':response[0]})
    except Exception as e:
        print('api call error')
        print(e)
        return JsonResponse({'message':'internal server error'})
    
@api_view(['POST'])
def razorpay_payment_data_api(request):
    try:
        data = request.data
        result = razorpay_payment_data(
            payment_id=data.get('razorpay_payment_id'),
            order_id=data.get('razorpay_order_id'),
            signature=data.get('razorpay_signature')
        )
        
        if result == 'success':
            return JsonResponse({'status': 'success', 'message': 'Payment successful and transaction recorded'})
        elif result == 'signature_failed':
            return JsonResponse({'status': 'error', 'message': 'Payment verification failed'}, status=400)
        elif result == 'order_not_found':
            return JsonResponse({'status': 'error', 'message': 'Order not found in record'}, status=404)
        else:
            return JsonResponse({'status': 'error', 'message': 'Error updating transaction'}, status=500)
            
    except Exception as e:
        print('api call error in razorpay_payment_data_api')
        print(e)
        return JsonResponse({'status': 'error', 'message': 'internal server error'}, status=500)