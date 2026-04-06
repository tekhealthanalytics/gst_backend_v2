import razorpay
from django.db import connection
import jwt
from django.conf import settings
from etymo.email import sendMail


client = razorpay.Client(auth=("rzp_test_RmS9j2gPUxb05Y", "51Uh6aITDjgkce4ufp74fNY0"))
def razorpay_create_request(token,amount):
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        data = { "amount": amount, "currency": "INR", "receipt": "order_rcptid_11" }
        payment = client.order.create(data) # Amount is in currency subunits.
        print(payment['id'])

        with connection.cursor() as cursor:
            cursor.execute(f"""
                            CREATE TABLE IF NOT EXISTS gst_tbl_razor_order_id(col_id SERIAL PRIMARY KEY, col_order_id TEXT,col_agent_email TEXT,col_amount INT);
                            """)
            cursor.execute(f"""
                           INSERT INTO gst_tbl_razor_order_id (col_order_id,col_agent_email,col_amount)  VALUES (%s,%s,%s);
                            """,(payment['id'],payload['email'],amount/100) )
            print('submitted order id')
            return ('success',amount,payment['id'])
    except jwt.ExpiredSignatureError:
        return ("Token expired, Please login again!")
    except jwt.InvalidTokenError:
        return ("Invalid token, Please login again!")
    except Exception as e:
        print(e)
        return ('server error')
    
def razorpay_payment_data(payment_id, order_id, signature):
    params = {
        'razorpay_order_id': order_id,
        'razorpay_payment_id': payment_id,
        'razorpay_signature': signature
    }
    try:
        client.utility.verify_payment_signature(params)
        print("Payment Signature Verified ✔")
        try:
            with connection.cursor() as cursor:
                # 1. Fetch order details
                cursor.execute(
                    "SELECT col_agent_email, col_amount FROM gst_tbl_razor_order_id WHERE col_order_id = %s;",
                    (order_id,)
                )
                row = cursor.fetchone()
                if not row:
                    print(f"Order ID {order_id} not found in gst_tbl_razor_order_id")
                    return 'order_not_found'
                
                agent_email = row[0]
                amount = row[1]
                print(f'agent email: {agent_email}, amount: {amount}')

                # 2. Ensure transactions table exists
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS gst_tbl_transactions(
                        col_id SERIAL PRIMARY KEY,
                        col_amount INT, 
                        col_type TEXT, 
                        col_user_email TEXT,
                        col_purpose TEXT,
                        col_reference_id TEXT, 
                        col_created_at TIMESTAMPTZ DEFAULT NOW()
                    );
                """)

                # 3. Record transaction
                cursor.execute(
                    """
                    INSERT INTO gst_tbl_transactions(col_type, col_amount, col_user_email, col_purpose, col_reference_id) 
                    VALUES('credit', %s, %s, 'razorpay_payment', %s);
                    """,
                    (amount, agent_email, payment_id)
                )

                # 4. Update agent balance
                cursor.execute(
                    """
                    UPDATE gst_tbl_agent_data 
                    SET col_balance = CAST(col_balance AS INT) + %s 
                    WHERE col_email = %s;
                    """,
                    (amount, agent_email)
                )
                
                print('Amount added to wallet and transaction recorded')
                return 'success'
        except Exception as e:
            print('Error while updating wallet or recording transaction:')
            print(e)
            return 'db_error'
       
    except Exception as e:
        print("Signature Verification Failed ❌")
        print(e)
        return 'signature_failed'