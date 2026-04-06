"""
URL configuration for config project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import include, path
# from etymo.api import ca_cs_registartion_api, check_connection, get_request_data_api, get_request_document_api,get_word_data,login_api,register_api,sendOTP_api, sendPasswordResetEmail_api, submit_request_api, updatePassword_api,verifyOTP_api,get_request_document_data_api
from etymo.api import *
from etymo.payment_api import razorpay_create_request_api, razorpay_payment_data_api
urlpatterns = [
    path('admin/', admin.site.urls),
    # path('check_connection/', check_connection),
    path('get_word_data/',get_word_data),
    path('login/',login_api),
    path('register/',register_api),
    path('send_otp/',sendOTP_api),
    path('verify_otp/',verifyOTP_api),
    path('update_password/',updatePassword_api),
    path('send_password_reset_email/',sendPasswordResetEmail_api),
    path('submit_request/',submit_request_api),
    path('get_request_data/',get_request_data_api),
    path('get_request_document/',get_request_document_api),
    path('get_request_document_data/',get_request_document_data_api),
    path('submit_ca_cs_details/',ca_cs_registartion_api),
    path('update_request_status/',update_request_status_api),
    path('get_ca_cs_data/',get_ca_cs_data_api), 
    path('assign_ca_cs/',assign_ca_cs_api),
    path('get_verified_request_data/',get_verified_request_data_api), 
    path('submit_payment_request/',submit_payment_request_api),
    path('get_payment_request_data/',get_payment_request_data_api),
    path('get_payment_request_document/',get_payment_request_document_api),
    path('get_payment_request_document_data/',get_payment_request_document_data_api),
    path('verifyPaymentRequest/',update_payment_request_status_api),
    path('rejectPaymentRequest/', reject_payment_request_api),
    path('get_ca_cs_document/',get_ca_cs_document_api),
    path('get_ca_cs_document_data/',get_ca_cs_document_data_api),
    path('get_agent_balance/',get_agent_balance_api),
    path('get_transaction_data/',get_transaction_data_api),  
    path('complete_request/',complete_request_api),
    path('get_request_completion_document/',get_request_completion_document_api),
    path('get_request_completion_document_data/',get_request_completion_document_data_api),
    path('get_agent_data_list/',get_agent_data_list_api),
    path('get_services/', get_services_api),
    path('add_service/', add_service_api),
    path('update_service/', update_service_api),
    path('delete_service/', delete_service_api),
    
    path('get_service_categories/', get_service_categories_api),
    path('add_service_category/', add_service_category_api),
    path('update_service_category/', update_service_category_api),
    path('delete_service_category/', delete_service_category_api),
    path('update_ca_cs/', update_ca_cs_api),
    path('get_ca_cs_slots/', get_ca_cs_slots_api),
    path('update_ca_cs_slots/', update_ca_cs_slots_api),
    path('get_ca_cs_special_slots/', get_ca_cs_special_slots_api),
    path('update_ca_cs_special_slots/', update_ca_cs_special_slots_api),
    path('razorpay_payment_data/',razorpay_payment_data_api),
    path('razorpay_create_request/',razorpay_create_request_api),
    path('get_my_cacs_data/', get_my_cacs_data_api),
    path('admin_pay_amount/', admin_pay_amount_api),
    path('update_admin_bank_details/', update_admin_bank_details_api),
    path('get_admin_bank_details/', get_admin_bank_details_api),
    path('get_admin_bank_qr_code/', get_admin_bank_qr_code_api),
    path('update_cacs_bank_details/', update_cacs_bank_details_api),
    path('get_cacs_bank_details/', get_cacs_bank_details_api),
]
