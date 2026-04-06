from django.db import connection
import jwt
from django.core.mail import EmailMultiAlternatives
import random
from datetime import datetime, timezone
import secrets
import psycopg2
from psycopg2 import Binary
from django.conf import settings
from datetime import datetime, timedelta

from etymo.email import sendMail

db_config = settings.DATABASES['default']
if 'OPTIONS' in db_config:
    db_config = {
        'dbname': db_config['NAME'],
        'user': db_config['USER'],
        'password': db_config['PASSWORD'],
        'host': db_config['HOST'],
        'port': db_config['PORT'],
    }

def get_base_template(title, body):
    """Returns a unified, professional HTML template for emails."""
    return f"""
<div style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; max-width: 600px; margin: 20px auto; border: 1px solid #e0e0e0; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.05); color: #333;">
    <div style="background-color: #2c3e50; padding: 30px; text-align: center; color: white;">
        <h1 style="margin: 0; font-size: 24px; letter-spacing: 1px;">GST WEB PORTAL</h1>
        <p style="margin: 5px 0 0 0; font-size: 14px; opacity: 0.8;">{title}</p>
    </div>
    
    <div style="padding: 40px; line-height: 1.6; background-color: #ffffff;">
        {body}
    </div>
    
    <div style="background-color: #f9f9f9; padding: 20px; text-align: center; color: #7f8c8d; font-size: 12px; border-top: 1px solid #eee;">
        <p style="margin: 0 0 10px 0;">This is an automated notification from the GST Web Portal.</p>
        <p style="margin: 0;">&copy; {datetime.now().year} GST Portal Team. All rights reserved.</p>
        <div style="margin-top: 15px; padding-top: 15px; border-top: 1px dashed #ddd;">
            <p style="margin: 0;"><b>Security Notice:</b> Never share your login credentials or OTP with anyone.</p>
        </div>
    </div>
</div>
"""
def ensure_all_tables():
    """Recreates all database tables if they do not exist and seeds a default Admin if empty."""
    print("Checking database schema...")
    try:
        with connection.cursor() as cursor:
            # 1. Core Authentication & Agent Tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gst_tbl_login_data(
                    col_username TEXT,
                    col_email TEXT UNIQUE,
                    col_password TEXT,
                    col_login_type TEXT DEFAULT 'Agent'
                );
                CREATE TABLE IF NOT EXISTS gst_tbl_agent_data(
                    col_email TEXT REFERENCES gst_tbl_login_data(col_email) ON DELETE CASCADE,
                    col_balance INT DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS gst_tbl_otp(
                    col_email_id TEXT,
                    col_otp INT,
                    col_gen_time TIMESTAMPTZ DEFAULT NOW(),
                    col_isused BOOLEAN DEFAULT FALSE
                );
                CREATE TABLE IF NOT EXISTS gst_tbl_reset_token(
                    col_email_id TEXT,
                    col_reset_token TEXT,
                    col_gen_time TIMESTAMPTZ DEFAULT NOW(),
                    col_isused BOOLEAN DEFAULT FALSE
                );
            """)

            # 2. CA/CS Management Tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gst_tbl_ca_cs(
                    col_id SERIAL PRIMARY KEY, 
                    col_name TEXT,
                    col_role TEXT, 
                    col_specialization TEXT,
                    col_email TEXT,
                    col_mobile TEXT,
                    col_regNumber TEXT,
                    col_workingDays TEXT[], 
                    col_created_at TIMESTAMPTZ DEFAULT NOW(),
                    col_assigned_request INT[] DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS gst_tbl_ca_cs_documents (
                    col_id SERIAL PRIMARY KEY,
                    col_ca_cs_id INT REFERENCES gst_tbl_ca_cs(col_id) ON DELETE CASCADE,
                    col_filename TEXT,
                    col_content_type TEXT,
                    col_file_data BYTEA,
                    col_created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS gst_tbl_ca_cs_slots(
                    col_id SERIAL PRIMARY KEY,
                    col_ca_cs_id INT REFERENCES gst_tbl_ca_cs(col_id) ON DELETE CASCADE,
                    col_day TEXT,
                    col_slot_number INT
                );
                CREATE TABLE IF NOT EXISTS gst_tbl_ca_cs_special_slots(
                    col_id SERIAL PRIMARY KEY,
                    col_ca_cs_id INT REFERENCES gst_tbl_ca_cs(col_id) ON DELETE CASCADE,
                    col_date DATE,
                    col_slot_number INT
                );
            """)

            # 3. Request & Service Tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gst_tbl_services(
                    col_id SERIAL PRIMARY KEY,
                    col_name TEXT,
                    col_price TEXT,
                    col_created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS gst_tbl_request(
                    col_id SERIAL PRIMARY KEY, 
                    col_name TEXT,
                    col_type TEXT,
                    col_email TEXT,
                    col_mobile TEXT,
                    col_description TEXT,
                    col_status TEXT DEFAULT 'Under Review',
                    col_instruction TEXT DEFAULT '',
                    col_created_at TIMESTAMPTZ DEFAULT NOW(),
                    col_assigned_ca_cs_id INT DEFAULT 0, 
                    col_agent_email_id TEXT,
                    col_com_des TEXT DEFAULT 'none',
                    col_approved_at TIMESTAMPTZ,
                    col_completed_at TIMESTAMPTZ,
                    col_rejected_at TIMESTAMPTZ,
                    col_assigned_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS gst_tbl_documents (
                    col_id SERIAL PRIMARY KEY,
                    col_request_id INT REFERENCES gst_tbl_request(col_id) ON DELETE CASCADE,
                    col_filename TEXT,
                    col_content_type TEXT,
                    col_file_data BYTEA,
                    col_created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # 4. Payment & Transaction Tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gst_tbl_payment_request(
                    col_id SERIAL PRIMARY KEY, 
                    col_name TEXT,
                    col_amount TEXT,
                    col_payment_method TEXT,
                    col_bank_name TEXT,
                    col_account_number TEXT,
                    col_ifsc_code TEXT, 
                    col_upi_id TEXT,
                    col_status TEXT DEFAULT 'Pending',
                    col_instruction TEXT DEFAULT '',
                    col_created_at TIMESTAMPTZ DEFAULT NOW(),
                    col_agent_email_id TEXT
                );
                CREATE TABLE IF NOT EXISTS gst_tbl_payment_documents (
                    col_id SERIAL PRIMARY KEY,
                    col_request_id INT REFERENCES gst_tbl_payment_request(col_id) ON DELETE CASCADE,
                    col_filename TEXT,
                    col_content_type TEXT,
                    col_file_data BYTEA,
                    col_created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS gst_tbl_transactions(
                    col_id SERIAL PRIMARY KEY,
                    col_amount INT, 
                    col_type TEXT, 
                    col_user_email TEXT,
                    col_purpose TEXT,
                    col_reference_id TEXT, 
                    col_created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS gst_tbl_completion_documents(
                    col_id SERIAL PRIMARY KEY,
                    col_request_id INT REFERENCES gst_tbl_request(col_id) ON DELETE CASCADE,
                    col_filename TEXT,
                    col_content_type TEXT,
                    col_file_data BYTEA,
                    col_created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS gst_tbl_cacs_bank_details (
                    col_cacs_id INT PRIMARY KEY REFERENCES gst_tbl_ca_cs(col_id) ON DELETE CASCADE,
                    col_bank_name TEXT,
                    col_account_name TEXT,
                    col_account_number TEXT,
                    col_ifsc_code TEXT,
                    col_upi_id TEXT,
                    col_updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # Seed Admin if no users exist
            cursor.execute("SELECT COUNT(*) FROM gst_tbl_login_data")
            if cursor.fetchone()[0] == 0:
                print("Seeding default Admin account...")
                cursor.execute("""
                    INSERT INTO gst_tbl_login_data (col_username, col_email, col_password, col_login_type)
                    VALUES ('Admin', 'admin@gst.com', 'admin123', 'Admin');
                """)
            
            print("Database schema verified.")
    except Exception as e:
        print(f"Database Initialization Error: {e}")

def login(email,password,loginType):
    ensure_all_tables()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"select col_email from gst_tbl_login_data where col_email = '{email}' and col_password = '{password}' and col_login_type='{loginType}';")
            rows=cursor.fetchone()
            if rows:
                print(f'data for jwt {rows}')
                payload = {
                       "email": rows[0],
                       "exp": datetime.now(timezone.utc) + timedelta(seconds=settings.JWT_EXP_DELTA_SECONDS),
                       "iat": datetime.now(timezone.utc),
                     }
                print(payload)

                token = jwt.encode(payload, settings.SECRET_KEY, algorithm="HS256")
                print(token)
                return ('correct credentials',token)
            else:
                return ('invalid credentials','')
    
    except Exception as e:
        print(e)
        return ('server error',e)
    
def register(username,email,password):
    ensure_all_tables()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
                           insert into gst_tbl_login_data (col_username,col_email,col_password) values('{username}','{email}','{password}');
                           insert into gst_tbl_agent_data(col_email) values('{email}')
                           """)
            rows=cursor.rowcount
            print(rows)
            return 'registered'
            
    except Exception as e:
        return 'email already exist'


def generate_otp():
    otp=random.randint(1000,9999)
    return otp

def sendOTP(email):


    otp=generate_otp()

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
                            select * from gst_tbl_login_data where col_email = '{email}' 
                            """)
            rows=cursor.rowcount
            if(rows):
                cursor.execute(f'''CREATE TABLE IF NOT EXISTS gst_tbl_otp(
                               col_email_id TEXT,
                               col_otp INT,
                               col_gen_time TIMESTAMPTZ DEFAULT NOW(),
                               col_isused BOOLEAN DEFAULT FALSE);
                               insert into gst_tbl_otp(col_email_id,col_otp) values('{email}','{otp}');''')
            else:
                return 'email not registerd'
    except Exception as e:
        return 'error'
    

    subject = "Welcome to GST Web Portal !"
    # from_email = "sanketsawant4123@gmail.com"
    # to = [email]
    to=[{"email": email}]
    # text_content = "OTP."
    # html_content = f"<p><b>{otp}</b> This is your otp for login. Please don't share it with others</p>"
    body = f"""
    <p>Dear User,</p>
    <p>Your One-Time Password (OTP) for logging into the <b>GST Web Portal</b> is:</p>
    <div style="background-color: #f4f7f6; border: 1px solid #d1d8d7; padding: 20px; text-align: center; border-radius: 8px; margin: 25px 0;">
        <span style="font-size: 32px; font-weight: bold; color: #2c3e50; letter-spacing: 5px;">{otp}</span>
    </div>
    <p>This OTP is valid for <b>5 minutes</b>. Please do not share this code with anyone for security reasons.</p>
    <p>If you did not request this OTP, please ignore this email.</p>
    """
    html_content = get_base_template("Login Verification", body)

    print('start otp sending')
    if sendMail(subject=subject,to=to,html_content=html_content):
        return "otp sent"
    else:
        return 'error'
    
   
        
    


def verifyOTP(email,otp):
    ensure_all_tables()
    if not otp.isnumeric():
        return 'incorrect otp'
    login_type=''
            
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"select col_login_type from gst_tbl_login_data where col_email = '{email}';")
            rows=cursor.fetchone()
            login_type=rows[0]
            if not rows:
                return ('email not registered',)
            # cursor.execute(f"select * from gst_tbl_otp where col_email_id='{email}' AND col_gen_time > NOW() - INTERVAL '5 minutes' ORDER BY col_gen_time DESC LIMIT 5")
            cursor.execute(f"select * from gst_tbl_otp where col_email_id='{email}' ORDER BY col_gen_time DESC LIMIT 1")
            rows=cursor.fetchall()
            if(rows):
                data=rows[0]
                diff=datetime.now(timezone.utc)-data[2]
                if(diff.total_seconds()<300 and not data[3]):
                    print('otp gen',data[1])
                    if(data[1]==int(otp)):
                        print('correct otp')
                        try:
                            cursor.execute(f"update gst_tbl_otp SET col_isused = True WHERE col_email_id ='{email}' AND col_otp='{data[1]}';")
                            print('otp status updated')
                            payload = {
                                    "email": email,
                                    "exp": datetime.now(timezone.utc) + timedelta(seconds=settings.JWT_EXP_DELTA_SECONDS),
                                    "iat": datetime.now(timezone.utc),
                                }
                            print(payload)

                            token = jwt.encode(payload, settings.SECRET_KEY, algorithm="HS256")
                            return('correct otp',token,login_type)
                        except Exception as e:
                            print(e)
                            return ('server error',)
                    else:
                        return ('incorrect otp',)
                else:
                    return ('otp expired',)
            else:
                return ('otp not sent',)
    except Exception as e:
        print(e)
        return ('server error',)
    
def sendPasswordResetEmail(email):
    username=None    
    reset_token=createResetPasswordToken(email)
    reset_link=f"https://effulgent-torte-d90e0a.netlify.app/resetpassword?email={email}&token={reset_token}"
    # reset_link=f"http://localhost:3000/resetpassword?email={email}&token={reset_token}"
    try:
        with connection.cursor() as cursor:
            print(email)
            cursor.execute(f"""
                           create table IF NOT EXISTS gst_tbl_login_data(col_username TEXT,col_email TEXT UNIQUE,col_password TEXT);
                           select col_username from gst_tbl_login_data where col_email ='{email}'""")
            rows=cursor.fetchall()
            username=rows[0][0]
            print(f'username is {username}')
        subject = "Reset Password"
        # from_email="sanketsawant4123@gmail.com"
        to=[{"email": email}]
        # text_content='Reset Password'
        # html_content= f"<p>Hello {username},<br> We received a request to reset your password for your GST webportal account.<br>Click the link below to set a new password: <br><a href='{reset_link}'>reset_link</a><br>This link will expire in 15 minutes. If you did not request a password reset, you can safely ignore this email.</p>"
        body = f"""
        <p>Hello <b>{username}</b>,</p>
        <p>We received a request to reset the password for your GST Web Portal account.</p>
        <p>Please click the button below to set a new password:</p>
        <div style="text-align: center; margin: 30px 0;">
            <a href="{reset_link}" style="background-color: #3498db; color: white; padding: 14px 28px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;">Reset Password</a>
        </div>
        <p style="font-size: 13px; color: #666;">This link will expire in 15 minutes for your security. If you did not request a password reset, you can safely ignore this email.</p>
        <p style="font-size: 13px; color: #666; word-break: break-all;">If the button doesn't work, copy and paste this link into your browser:<br>{reset_link}</p>
        """
        html_content = get_base_template("Password Reset Request", body)
        
        if sendMail(subject=subject,to=to,html_content=html_content):
            return 'reset password email sent'
        else:
            return 'error'
    except Exception as e:
         print(e)
         return 'server error'

def createResetPasswordToken(email):
    reset_token=secrets.token_urlsafe(32)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'''CREATE TABLE IF NOT EXISTS gst_tbl_reset_token(
                               col_email_id TEXT,
                               col_reset_token TEXT,
                               col_gen_time TIMESTAMPTZ DEFAULT NOW(),
                               col_isused BOOLEAN DEFAULT FALSE);
                               insert into gst_tbl_reset_token(col_email_id,col_reset_token) values('{email}','{reset_token}');''') 
            return reset_token

    except Exception as e:
        print(f'error: {e}')
        return 0
    
def updatePassword(email,reset_token,password):
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT col_email_id FROM gst_tbl_reset_token where col_email_id='{email}' and col_reset_token='{reset_token}' and col_gen_time > NOW() - INTERVAL '15 minutes' and col_isused=False;")
            rows=cursor.fetchall()
            if rows:
                cursor.execute(f'''update gst_tbl_login_data set col_password='{password}' where col_email='{email}';
                                   update gst_tbl_reset_token set col_isused = TRUE where col_email_id='{email}' and col_reset_token='{reset_token}'
                               ''')
                return 'password changed'
            else:
                return 'token expired'
    except Exception as e:
        print(e)
        return 'error'

        
def submit_request(name,type_,email,mobile,description,documents,token, doc_status='complete'):
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        with connection.cursor() as cursor:
            cursor.execute(f"""
                           CREATE TABLE IF NOT EXISTS gst_tbl_transactions(col_id SERIAL PRIMARY KEY,col_amount INT, col_type TEXT, col_user_email TEXT,col_purpose TEXT,col_reference_id INT, col_created_at TIMESTAMPTZ default NOW());
                            CREATE TABLE IF NOT EXISTS gst_tbl_request(col_id SERIAL PRIMARY KEY, col_name TEXT,col_type TEXT ,col_email TEXT,col_mobile TEXT,col_description TEXT,col_status TEXT default 'Under Review',col_instruction TEXT DEFAULT '' ,col_created_at TIMESTAMPTZ default NOW(),col_assigned_ca_cs_id INT DEFAULT 0, col_agent_email_id TEXT,col_com_des text default 'none',col_approved_at TIMESTAMPTZ,col_completed_at TIMESTAMPTZ,col_rejected_at TIMESTAMPTZ,col_assigned_at TIMESTAMPTZ, col_doc_status TEXT DEFAULT 'complete');
                            ALTER TABLE gst_tbl_request ADD COLUMN IF NOT EXISTS col_doc_status TEXT DEFAULT 'complete';
                            """)
            
            # Fetch price for the specific service type
            cursor.execute("SELECT col_price FROM gst_tbl_services WHERE col_name = %s", (type_,))
            service_row = cursor.fetchone()
            service_price = int(service_row[0]) if service_row else 500

            cursor.execute(f"""
                           INSERT INTO gst_tbl_request (col_name,col_type,col_email,col_mobile,col_description,col_agent_email_id, col_doc_status)  VALUES (%s, %s, %s, %s, %s,%s, %s) RETURNING col_id;
                            """,(name, type_, email, mobile, description,payload['email'], doc_status) )
            new_id = cursor.fetchone()[0]
            print(new_id)
            # Record debit transaction
            cursor.execute(
                """
                INSERT INTO gst_tbl_transactions(col_type, col_amount, col_user_email, col_purpose, col_reference_id) 
                VALUES('debit', %s, %s, 'request_generation', %s);
                """,
                (service_price, payload['email'], str(new_id))
            )

            # Update agent balance
            cursor.execute(
                """
                UPDATE gst_tbl_agent_data 
                SET col_balance = CAST(col_balance AS INT) - %s 
                WHERE col_email = %s;
                """,
                (service_price, payload['email'])
            )

            for doc in documents:
                byte_data=doc.read()
                print("Name:",doc.name ) 
                print("Type:", doc.content_type)
                print("Size:", len(byte_data))
            
                cursor.execute(f"""
                                CREATE TABLE IF NOT EXISTS gst_tbl_documents (
                                                        col_id SERIAL PRIMARY KEY,
                                                        col_request_id INT REFERENCES gst_tbl_request(col_id) ON DELETE CASCADE, -- link to request
                                                        col_filename TEXT,
                                                        col_content_type TEXT,
                                                        col_file_data BYTEA,
                                                        col_created_at TIMESTAMPTZ DEFAULT NOW()
                                                    );
                               INSERT INTO gst_tbl_documents (col_request_id,col_filename,col_content_type,col_file_data)  VALUES (%s, %s, %s, %s);
                                """,(new_id,doc.name,doc.content_type,byte_data))
            
            print('submitted')
            return 'submitted'
    except jwt.ExpiredSignatureError:
        return "Token expired, Please login again!"
    except jwt.InvalidTokenError:
        return "Invalid token, Please login again!"
    except Exception as e:
        print(e)
        return 'server error'


def get_request_document(request_id):
    print(request_id)
    try:
        
        with connection.cursor() as cursor:
            cursor.execute(f"""
                    select col_id ,col_filename, col_content_type from gst_tbl_documents where col_request_id={request_id}
                """)
            data=cursor.fetchall()
            print(f'documents got {data}')
            return data
    except Exception as e:
        print(e)
        return []

def get_request_data(token):
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        print(payload['email'])
        email=payload['email']
        with connection.cursor() as cursor:
            cursor.execute(f"""
                    select col_login_type,col_username from gst_tbl_login_data where col_email='{email}';
                """)
            data=cursor.fetchone()
            print(data[0])
            username=data[1]
            if(data[0]=='Admin'):
                cursor.execute("""
                        select request.*, agent_login.col_username, ca_cs.col_name 
                        FROM gst_tbl_request request 
                        JOIN gst_tbl_login_data agent_login ON request.col_agent_email_id = agent_login.col_email 
                        LEFT JOIN gst_tbl_ca_cs ca_cs ON request.col_assigned_ca_cs_id = ca_cs.col_id
                        where request.col_status <> 'Cancelled' 
                        order by request.col_created_at DESC;
                    """)
            elif data[0] == 'CA/CS':
                # For CA/CS, fetch the CA/CS ID first
                cursor.execute(f"SELECT col_id FROM gst_tbl_ca_cs WHERE col_email='{email}'")
                ca_cs_row = cursor.fetchone()
                if ca_cs_row:
                    ca_cs_id = ca_cs_row[0]
                    cursor.execute(f"""
                        SELECT request.*, agent_login.col_username, ca_cs.col_name 
                        FROM gst_tbl_request request 
                        JOIN gst_tbl_login_data agent_login ON request.col_agent_email_id = agent_login.col_email 
                        LEFT JOIN gst_tbl_ca_cs ca_cs ON request.col_assigned_ca_cs_id = ca_cs.col_id
                        WHERE request.col_assigned_ca_cs_id = {ca_cs_id} 
                        ORDER BY request.col_created_at DESC;
                    """)
                else:
                    return ([], 'success') # No CA/CS record found for this email
            else:
                cursor.execute(f"""
                        select request.*, agent_login.col_username, ca_cs.col_name 
                        FROM gst_tbl_request request 
                        JOIN gst_tbl_login_data agent_login ON request.col_agent_email_id = agent_login.col_email 
                        LEFT JOIN gst_tbl_ca_cs ca_cs ON request.col_assigned_ca_cs_id = ca_cs.col_id
                        WHERE request.col_agent_email_id='{email}'  
                        order by request.col_created_at DESC;
                    """)
                # select * from gst_tbl_request where col_agent_email_id='{email}' order by col_created_at DESC
            data=cursor.fetchall()
            return (data,'success')
    except jwt.ExpiredSignatureError:
        return ([],"Token expired, Please login again!")
    except jwt.InvalidTokenError:
        return ([],"Invalid token, Please login again!")
    except Exception as e:
        print(e)
        return ([],'data not found')



def get_ca_cs_data(token, available_now=False):
    print(f"in get_ca_cs_data, available_now={available_now}")
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        print(payload['email'])
        email=payload['email']
        with connection.cursor() as cursor:
            cursor.execute(f"""
                    select col_login_type from gst_tbl_login_data where col_email='{email}';
                """)
            data=cursor.fetchone()
            print(data[0])
            if(data[0]=='Admin'):
                if available_now:
                    # Compute current IST slot number: slot = (hour // 2) + 1  (1..12)
                    ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
                    ist_date = ist_now.date()
                    ist_weekday = ist_now.strftime('%A')  # e.g. 'Monday'
                    current_slot = (ist_now.hour // 2) + 1
                    print(f"IST now: date={ist_date}, weekday={ist_weekday}, slot={current_slot}")

                    # Ensure slot tables exist before querying
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS gst_tbl_ca_cs_slots(
                            col_id SERIAL PRIMARY KEY,
                            col_ca_cs_id INT REFERENCES gst_tbl_ca_cs(col_id) ON DELETE CASCADE,
                            col_day TEXT,
                            col_slot_number INT
                        );
                        CREATE TABLE IF NOT EXISTS gst_tbl_ca_cs_special_slots(
                            col_id SERIAL PRIMARY KEY,
                            col_ca_cs_id INT REFERENCES gst_tbl_ca_cs(col_id) ON DELETE CASCADE,
                            col_date DATE,
                            col_slot_number INT
                        );
                    """)

                    # Logic:
                    #   1. If special slots exist for today → CA/CS available if
                    #      current_slot is among those special slots.
                    #   2. If NO special slots for today → check weekly slots
                    #      for today's weekday and current_slot.
                    cursor.execute("""
                        SELECT ca_cs.*
                        FROM gst_tbl_ca_cs ca_cs
                        WHERE
                            CASE
                                -- Case 1: special slots exist for today
                                WHEN EXISTS (
                                    SELECT 1 FROM gst_tbl_ca_cs_special_slots ss
                                    WHERE ss.col_ca_cs_id = ca_cs.col_id
                                      AND ss.col_date = %s
                                )
                                THEN EXISTS (
                                    SELECT 1 FROM gst_tbl_ca_cs_special_slots ss
                                    WHERE ss.col_ca_cs_id = ca_cs.col_id
                                      AND ss.col_date = %s
                                      AND ss.col_slot_number = %s
                                )
                                -- Case 2: no special slots → use general weekly schedule
                                ELSE EXISTS (
                                    SELECT 1 FROM gst_tbl_ca_cs_slots s
                                    WHERE s.col_ca_cs_id = ca_cs.col_id
                                      AND LOWER(TRIM(s.col_day)) = LOWER(%s)
                                      AND s.col_slot_number = %s
                                )
                            END
                        ORDER BY ca_cs.col_created_at DESC;
                    """, (ist_date, ist_date, current_slot, ist_weekday, current_slot))
                else:
                    cursor.execute("""
                            select * from gst_tbl_ca_cs order by col_created_at DESC
                        """)
                data=cursor.fetchall()
                return (data,'success')
            else:
                return ([],"Unauthorized request")
                
            
                # print(data)
    except jwt.ExpiredSignatureError:
        return ([],"Token expired, Please login again!")
    except jwt.InvalidTokenError:
        return ([],"Invalid token, Please login again!")
    except Exception as e:
        print(e)
        return ([],'data not found')



def get_request_document_data(id):
    try:
        print('get_request_document_data')
        with connection.cursor() as cursor:
            cursor.execute(f"""
                    select col_content_type,col_file_data from gst_tbl_documents where col_id={id}
                """)
            data= cursor.fetchone()
            print(data)
            return data
    except Exception as e:
        print(e)



def ca_cs_registartion(data,docs):
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                            CREATE TABLE IF NOT EXISTS gst_tbl_ca_cs(
                                col_id SERIAL PRIMARY KEY, 
                                col_name TEXT,
                                col_role TEXT, 
                                col_specialization TEXT,
                                col_email TEXT,
                                col_mobile TEXT,
                                col_regNumber TEXT,
                                col_workingDays TEXT[], 
                                col_created_at TIMESTAMPTZ default NOW(),
                                col_assigned_request INT[] DEFAULT '{}'
                            );
                            """)
            # Check if email already exists
            cursor.execute("SELECT col_id FROM gst_tbl_ca_cs WHERE col_email = %s", (data['email'],))
            if cursor.fetchone():
                return 'email already exist'

            cursor.execute("""
                           INSERT INTO gst_tbl_ca_cs (col_name,col_role,col_specialization, col_email,col_mobile,col_regNumber,col_workingDays) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING col_id;
                            """, (data['name'], data['role'], data['specialization'], data['email'], data['mobile'], data['regNumber'], ['mon','tue','wed']))

            new_id = cursor.fetchone()[0]
            print(new_id)

            # Create gst_tbl_ca_cs_documents and fix any wrong FK constraint
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gst_tbl_ca_cs_documents (
                    col_id SERIAL PRIMARY KEY,
                    col_ca_cs_id INT REFERENCES gst_tbl_ca_cs(col_id) ON DELETE CASCADE,
                    col_filename TEXT,
                    col_content_type TEXT,
                    col_file_data BYTEA,
                    col_created_at TIMESTAMPTZ DEFAULT NOW()
                );
                -- Fix incorrect FK that may reference gst_tbl_request instead of gst_tbl_ca_cs
                DO $$
                DECLARE
                    bad_constraint TEXT;
                BEGIN
                    SELECT conname INTO bad_constraint
                    FROM pg_constraint c
                    JOIN pg_class t ON c.conrelid = t.oid
                    JOIN pg_class r ON c.confrelid = r.oid
                    WHERE t.relname = 'gst_tbl_ca_cs_documents'
                      AND r.relname = 'gst_tbl_request'
                      AND c.contype = 'f';

                    IF bad_constraint IS NOT NULL THEN
                        EXECUTE 'ALTER TABLE gst_tbl_ca_cs_documents DROP CONSTRAINT ' || quote_ident(bad_constraint);
                        ALTER TABLE gst_tbl_ca_cs_documents
                            ADD CONSTRAINT gst_tbl_ca_cs_documents_col_ca_cs_id_fkey
                            FOREIGN KEY (col_ca_cs_id) REFERENCES gst_tbl_ca_cs(col_id) ON DELETE CASCADE;
                    END IF;
                END $$;
            """)

            for doc in docs:
                byte_data = doc.read()
                print("Name:", doc.name)
                print("Type:", doc.content_type)
                print("Size:", len(byte_data))

                cursor.execute("""
                    INSERT INTO gst_tbl_ca_cs_documents (col_ca_cs_id, col_filename, col_content_type, col_file_data)
                    VALUES (%s, %s, %s, %s);
                """, (new_id, doc.name, doc.content_type, byte_data))

            print('submitted')

            # Create login credentials for the CA/CS user
            temp_password = data['mobile']  # Use mobile as initial password
            login_type = 'CA/CS'  # Unified login type
            cursor.execute("""
                create table IF NOT EXISTS gst_tbl_login_data(col_username TEXT, col_email TEXT UNIQUE, col_password TEXT, col_login_type TEXT DEFAULT 'Agent');
            """)
            cursor.execute("""
                INSERT INTO gst_tbl_login_data (col_username, col_email, col_password, col_login_type)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (col_email) DO UPDATE SET col_login_type = EXCLUDED.col_login_type;
            """, (data['name'], data['email'], temp_password, login_type))

            # Send welcome email with login credentials
            try:
                portal_link = 'https://effulgent-torte-d90e0a.netlify.app/login'
                body = f"""
                <h2 style="color: #2c3e50; margin-top: 0;">Welcome aboard!</h2>
                <p>Dear <b>{data['name']}</b>,</p>
                <p>Your account has been successfully registered on the <b>GST Web Portal</b> as a <b>CA/CS</b>.</p>

                <div style="background-color: #f4f6f9; padding: 25px; border-radius: 8px; margin: 25px 0; border-left: 5px solid #3498db;">
                    <p style="margin: 0 0 12px 0; font-weight: bold; color: #2c3e50;">Your Login Credentials:</p>
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 5px 0; color: #7f8c8d; width: 80px;">Email:</td>
                            <td style="padding: 5px 0; font-weight: bold;">{data['email']}</td>
                        </tr>
                        <tr>
                            <td style="padding: 5px 0; color: #7f8c8d;">Password:</td>
                            <td style="padding: 5px 0; font-weight: bold;">{temp_password} <span style="font-weight: normal; font-size: 12px; color: #e67e22;">(Your registered mobile number)</span></td>
                        </tr>
                        <tr>
                            <td style="padding: 5px 0; color: #7f8c8d;">Role:</td>
                            <td style="padding: 5px 0; font-weight: bold;">CA/CS</td>
                        </tr>
                    </table>
                </div>

                <div style="text-align: center; margin: 30px 0;">
                    <a href="{portal_link}" style="background-color: #2ecc71; color: white; padding: 14px 28px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;">Login to Your Dashboard</a>
                </div>

                <div style="background-color: #fff3cd; border-left: 5px solid #f39c12; padding: 15px; margin: 25px 0; border-radius: 4px;">
                    <p style="margin: 0; font-size: 14px; color: #856404;"><b>⚠️ Security Notice:</b> For your account security, please log in and <b>change your password immediately</b>.</p>
                </div>

                <p style="font-size: 14px;">If you have any questions, our support team is always here to help.</p>
                """
                sendMail(
                    'Welcome to GST Portal – Your Account is Ready',
                    [{"email": data['email']}],
                    get_base_template("Account Registration", body)
                )
            except Exception as mail_err:
                print(f"Warning: Could not send welcome email: {mail_err}")

            return 'submitted'
    except Exception as e:
        print(e)
        return 'server error'


def update_ca_cs(ca_cs_id, data, certificate=None, id_proof=None):
    try:
        with connection.cursor() as cursor:
            # Update main record
            cursor.execute("""
                UPDATE gst_tbl_ca_cs 
                SET col_name = %s, col_role = %s, col_specialization = %s, 
                    col_email = %s, col_mobile = %s, col_regNumber = %s
                WHERE col_id = %s;
            """, (data['name'], data['role'], data['specialization'], 
                  data['email'], data.get('phone', data.get('mobile')), 
                  data.get('registrationNumber', data.get('regNumber')), 
                  ca_cs_id))

            
            # Fetch existing documents to update them by index
            cursor.execute(f"SELECT col_id FROM gst_tbl_ca_cs_documents WHERE col_ca_cs_id = {ca_cs_id} ORDER BY col_id ASC")
            docs_ids = cursor.fetchall()

            # Update Certificate (Index 0)
            if certificate:
                byte_data = certificate.read()
                if len(docs_ids) > 0:
                    cursor.execute("""
                        UPDATE gst_tbl_ca_cs_documents 
                        SET col_filename = %s, col_content_type = %s, col_file_data = %s 
                        WHERE col_id = %s;
                    """, (certificate.name, certificate.content_type, byte_data, docs_ids[0][0]))
                else:
                    cursor.execute("""
                        INSERT INTO gst_tbl_ca_cs_documents (col_ca_cs_id, col_filename, col_content_type, col_file_data) 
                        VALUES (%s, %s, %s, %s);
                    """, (ca_cs_id, certificate.name, certificate.content_type, byte_data))

            # Update ID Proof (Index 1)
            if id_proof:
                byte_data = id_proof.read()
                if len(docs_ids) > 1:
                    cursor.execute("""
                        UPDATE gst_tbl_ca_cs_documents 
                        SET col_filename = %s, col_content_type = %s, col_file_data = %s 
                        WHERE col_id = %s;
                    """, (id_proof.name, id_proof.content_type, byte_data, docs_ids[1][0]))
                else:
                    cursor.execute("""
                        INSERT INTO gst_tbl_ca_cs_documents (col_ca_cs_id, col_filename, col_content_type, col_file_data) 
                        VALUES (%s, %s, %s, %s);
                    """, (ca_cs_id, id_proof.name, id_proof.content_type, byte_data))

            return 'updated'
    except Exception as e:
        print(f"Error in update_ca_cs: {e}")
        return 'server error'


def sendStatusUpdateEmail(agentEmail,agentUserName,requestId,requesCustomerName,requestStatus,requestInstruction,attachments=None):
    subject = "Request Status Update"
    # from_email="sanketsawant4123@gmail.com"
    to=[{"email": agentEmail}]
    # text_content='Request Status Update'
    # html_content= f"<p>Dear {agentUserName},<br> We would like to inform you that the status of your request has been updated. Please find the details below : <br><br>Request ID: {requestId}<br>Customer Name: {requesCustomerName}<br>Current Status: <b>{requestStatus}</b><br>Instruction: {requestInstruction if requestInstruction else 'NONE'}<br><br><br> If you have any questions or need further assistance, please feel free to contact us.<br><br>Thank you!</p>"
    status_colors = {
        "Approved": "#2ecc71",
        "Rejected": "#e74c3c",
        "Completed": "#3498db",
        "Assigned": "#f39c12",
        "Under Review": "#95a5a6",
        "Cancelled": "#7f8c8d"
    }
    color = status_colors.get(requestStatus, "#2c3e50")
    
    body = f"""
    <p>Dear <b>{agentUserName}</b>,</p>
    <p>We are writing to inform you that the status of your request has been updated. Please find the details below:</p>
    
    <div style="background-color: #f8f9fa; border: 1px solid #e9ecef; border-radius: 8px; padding: 25px; margin: 20px 0;">
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="padding: 8px 0; color: #6c757d; width: 130px;">Request ID:</td>
                <td style="padding: 8px 0; font-weight: bold;">#{requestId}</td>
            </tr>
            <tr>
                <td style="padding: 8px 0; color: #6c757d;">Customer Name:</td>
                <td style="padding: 8px 0; font-weight: bold;">{requesCustomerName}</td>
            </tr>
            <tr>
                <td style="padding: 8px 0; color: #6c757d;">Current Status:</td>
                <td style="padding: 8px 0;">
                    <span style="background-color: {color}; color: white; padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: bold; text-transform: uppercase;">{requestStatus}</span>
                </td>
            </tr>
            <tr>
                <td style="padding: 8px 0; color: #6c757d; vertical-align: top;">Instruction:</td>
                <td style="padding: 8px 0; font-style: italic; color: #2c3e50;">{requestInstruction if requestInstruction else 'No instructions provided.'}</td>
            </tr>
        </table>
    </div>
    
    <p>You can view more details about this request by logging into your dashboard.</p>
    <p>If you have any questions or need further assistance, please contact our support team.</p>
    """
    html_content = get_base_template("Request Status Update", body)
  
    if sendMail(subject=subject,to=to,html_content=html_content,attachments=attachments):
        print('status update mail sent')
    else:
        print('status update mail not sent')
    

def update_request_status(requestId,requestStatus,requestInstruction,attachment=None):
    try:
        print('in update_request_status')
        with connection.cursor() as cursor:
            #get old status
            cursor.execute(
                f"""
                    SELECT col_status from gst_tbl_request where col_id = %s;
                 """ ,(requestId,)  
            )
            row=cursor.fetchone()
            old_status=row[0]
            status_time_col=''
            status_des_col='col_instruction'
            match(requestStatus):
                case "Approved":
                    if old_status!='Under Review':
                        return 'refresh data'
                    status_time_col="col_approved_at"
                case "Rejected":
                    if old_status!='Under Review':
                        return 'refresh data'
                    status_time_col="col_rejected_at"
                case "Completed":
                    if old_status!='Assigned':
                        return 'refresh data'
                    status_time_col="col_completed_at"
                    status_des_col='col_com_des'
                case "Cancelled":
                    if old_status!='Under Review':
                        return 'refresh data'
                    status_time_col="col_rejected_at"
                    status_des_col='col_com_des'
                case _:
                    return "invalid status"
                
            cursor.execute(
                f"""
                    UPDATE gst_tbl_request SET col_status= %s, {status_des_col} = %s, {status_time_col}= NOW() where col_id = %s;
                 """ ,(requestStatus,requestInstruction,requestId)  
            )
            
            cursor.execute(
                f"""
                    SELECT col_agent_email_id,col_id,col_name from gst_tbl_request where col_id = %s;
                 """ ,(requestId,)  
            )
            row=cursor.fetchone()
            if not row:
                print(f"Request {requestId} not found after update")
                return 'error'
                
            agent_email=row[0]
            request_id=row[1]
            reques_customer_name=row[2]
            print(f'agent email : {agent_email}')

            cursor.execute(
                f"""
                    SELECT col_username from gst_tbl_login_data where col_email= %s;
                 """ ,(agent_email,)  
            )
            row=cursor.fetchone()
            agent_username=row[0]
            print(agent_email,agent_username ,request_id,reques_customer_name,requestStatus,requestInstruction)
            sendStatusUpdateEmail(agentEmail=agent_email,agentUserName=agent_username , requestId=request_id,requesCustomerName=reques_customer_name,requestStatus=requestStatus,requestInstruction=requestInstruction,attachments=attachment)
            return "success"

    except Exception as e:
        print(e)
        return 'server error'


# def get_ca_cs_data():
#     try:
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                     select * from gst_tbl_ca_cs;
#                 """)
#             data=cursor.fetchall()
#             print(data)
#             return data
#     except Exception as e:
#         print(e)
#         return []


    
def assign_ca_cs(ca_cs_id, requestId):
    try:
        with connection.cursor() as cursor:
            # First, check the current status of the request
            cursor.execute(
                "SELECT col_status FROM gst_tbl_request WHERE col_id = %s;",
                (requestId,)
            )
            row = cursor.fetchone()
            if not row:
                print(f"No request found with ID {requestId}")
                return 'error'
            
            old_status = row[0]
            if old_status != 'Approved':
                return 'refresh data'

            # Assign the CA/CS to the request
            cursor.execute(
                "UPDATE gst_tbl_request SET col_assigned_ca_cs_id = %s, col_status = 'Assigned', col_assigned_at = NOW() WHERE col_id = %s;",
                (ca_cs_id, requestId)
            )
            
            # Update the CA/CS record with the assigned request
            cursor.execute(
                "UPDATE gst_tbl_ca_cs SET col_assigned_request = array_append(col_assigned_request, %s) WHERE col_id = %s;",
                (requestId, ca_cs_id)
            )
            
            # Fetch request details for the email notification
            cursor.execute(
                "SELECT col_agent_email_id, col_id, col_name FROM gst_tbl_request WHERE col_id = %s;",
                (requestId,)
            )
            row = cursor.fetchone()
            if not row:
                print(f"Could not fetch details for request {requestId} after update")
                return 'error'
            
            agent_email = row[0]
            request_id = row[1]
            request_customer_name = row[2]
            cursor.execute(
                f"""
                    SELECT col_username from gst_tbl_login_data where col_email= %s;
                 """ ,(agent_email,)  
            )
            row = cursor.fetchone()
            agent_username = row[0]
            sendStatusUpdateEmail(agentEmail=agent_email, agentUserName=agent_username, requestId=request_id, requesCustomerName=request_customer_name, requestStatus="Assigned", requestInstruction="")
            return 'success'
            
    except Exception as e:
        print('error in assign_ca_cs')
        print(e)
        return 'error'

def get_verified_request_data():
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                    select * from gst_tbl_request where col_status = 'Verified' order by col_created_at DESC
                """)
            data=cursor.fetchall()
            print(data)
            return data
    except Exception as e:
        print(e)
        return []
    

def submit_payment_request(name,amount,paymentMethod,bankName,accountNumber,ifscCode,upiId,documents,token):
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        with connection.cursor() as cursor:
            cursor.execute(f"""
                            CREATE TABLE IF NOT EXISTS gst_tbl_payment_request(col_id SERIAL PRIMARY KEY, col_name TEXT,col_amount TEXT,col_payment_method TEXT ,col_bank_name TEXT,col_account_number TEXT,col_ifsc_code TEXT, col_upi_id TEXT,col_status TEXT default 'Pending',col_instruction TEXT DEFAULT '' ,col_created_at TIMESTAMPTZ default NOW(),col_agent_email_id TEXT);
                            """)
            cursor.execute(f"""
                           INSERT INTO gst_tbl_payment_request (col_name,col_amount,col_payment_method,col_bank_name,col_account_number,col_ifsc_code,col_upi_id,col_agent_email_id)  VALUES (%s,%s, %s, %s, %s, %s,%s,%s) RETURNING col_id;
                            """,(name,amount, paymentMethod, bankName, accountNumber, ifscCode,upiId,payload['email']) )
            new_id = cursor.fetchone()[0]
            print(new_id)

            for doc in documents:
                byte_data=doc.read()
                print("Name:",doc.name ) 
                print("Type:", doc.content_type)
                print("Size:", len(byte_data))
            
                cursor.execute(f"""
                                CREATE TABLE IF NOT EXISTS gst_tbl_payment_documents (
                                                        col_id SERIAL PRIMARY KEY,
                                                        col_request_id INT REFERENCES gst_tbl_payment_request(col_id) ON DELETE CASCADE, -- link to request
                                                        col_filename TEXT,
                                                        col_content_type TEXT,
                                                        col_file_data BYTEA,
                                                        col_created_at TIMESTAMPTZ DEFAULT NOW()
                                                    );
                               INSERT INTO gst_tbl_payment_documents (col_request_id,col_filename,col_content_type,col_file_data)  VALUES (%s, %s, %s, %s);
                                """,(new_id,doc.name,doc.content_type,byte_data))
            
            print('submitted')
            return 'submitted'
    except jwt.ExpiredSignatureError:
        return "Token expired, Please login again!"
    except jwt.InvalidTokenError:
        return "Invalid token, Please login again!"
    except Exception as e:
        print(e)
        return 'server error'
    

    

def get_payment_request_data(token):
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        email=payload['email']
        with connection.cursor() as cursor:
            cursor.execute(f"""
                    select col_login_type from gst_tbl_login_data where col_email='{email}';
                """)
            data=cursor.fetchone()
            print(data[0])
            if(data[0]=='Admin'):
                cursor.execute("""
                        select * from gst_tbl_payment_request order by col_created_at DESC
                    """)
            else:
                cursor.execute(f"""
                        select * from gst_tbl_payment_request where col_agent_email_id='{email}' order by col_created_at DESC
                    """)
            
            data=cursor.fetchall()
            print(data)
            return (data,'success')
    except jwt.ExpiredSignatureError:
        return ([],"Token expired, Please login again!")
    except jwt.InvalidTokenError:
        return ([],"Invalid token, Please login again!")
    except Exception as e:
        print(e)
        return ([],"data not found")
    
# def get_payment_request_document_data(id):
#     try:
#         print('get_payment_request_document_data')
#         with connection.cursor() as cursor:
#             cursor.execute(f"""
#                     select col_content_type,col_file_data from gst_tbl_payment_documents where col_id={id}
#                 """)
#             data= cursor.fetchone()
#             print(data)
#             return data
#     except Exception as e:
#         print(e)


def get_payment_request_document(request_id):
    print(request_id)
    try:
        with connection.cursor() as cursor:
            # cursor.execute("""
            #         select col_id from gst_tbl_request order by col_created_at DESC
            #     """)
            # request_id=cursor.fetchone()[0]
            # print(request_id)
            cursor.execute(f"""
                    select col_id ,col_filename, col_content_type from gst_tbl_payment_documents where col_request_id={request_id}
                """)
            data=cursor.fetchall()
            print(f'documents got {data}')
            return data
    except Exception as e:
        print(e)
        return []


def get_payment_request_document_data(id):
    try:
        print('get_payment_request_document_data')
        with connection.cursor() as cursor:
            cursor.execute(f"""
                    select col_content_type,col_file_data from gst_tbl_payment_documents where col_id={id}
                """)
            data= cursor.fetchone()
            print(data)
            return data
    except Exception as e:
        print(e)


def update_payment_request_status(paymentRequestId, requestInstruction):
    try:
        print('in update_payment_request_status')
        with connection.cursor() as cursor:
            # 1. Fetch payment request details
            cursor.execute(
                "SELECT col_agent_email_id, col_name, col_amount FROM gst_tbl_payment_request WHERE col_id = %s;",
                (paymentRequestId,)
            )
            row = cursor.fetchone()
            if not row:
                print(f"Payment request {paymentRequestId} not found")
                return 'not_found'
                
            agent_email = row[0]
            request_customer_name = row[1]
            amount = row[2]
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
                VALUES('credit', %s, %s, 'manual_payment_verification', %s);
                """,
                (int(amount), agent_email, str(paymentRequestId))
            )

            # 4. Update agent balance
            cursor.execute(
                """
                UPDATE gst_tbl_agent_data 
                SET col_balance = CAST(col_balance AS INT) + %s 
                WHERE col_email = %s;
                """,
                (int(amount), agent_email)
            )

            # 5. Update payment request status (missing in original code shown)
            cursor.execute(
                """
                UPDATE gst_tbl_payment_request 
                SET col_status = 'Approved', col_instruction = %s 
                WHERE col_id = %s;
                """,
                (requestInstruction, paymentRequestId)
            )

            # 6. Fetch agent username and send status update email
            cursor.execute(
                "SELECT col_username FROM gst_tbl_login_data WHERE col_email = %s;",
                (agent_email,)
            )
            username_row = cursor.fetchone()
            agent_username = username_row[0] if username_row else agent_email

            sendStatusUpdateEmail(
                agentEmail=agent_email,
                agentUserName=agent_username,
                requestId=paymentRequestId,
                requesCustomerName=request_customer_name,
                requestStatus="Approved",
                requestInstruction=requestInstruction
            )

            print('Payment request approved and transaction recorded')
            return "success"

    except Exception as e:
        print('Error in update_payment_request_status:')
        print(e)
        return 'server error'


def reject_payment_request(paymentRequestId, rejectReason):
    try:
        print('in reject_payment_request')
        with connection.cursor() as cursor:
            # 1. Fetch payment request details
            cursor.execute(
                "SELECT col_agent_email_id, col_name, col_amount, col_status FROM gst_tbl_payment_request WHERE col_id = %s;",
                (paymentRequestId,)
            )
            row = cursor.fetchone()
            if not row:
                print(f"Payment request {paymentRequestId} not found")
                return 'not_found'

            agent_email = row[0]
            request_customer_name = row[1]
            current_status = row[3]

            if current_status != 'Pending':
                return 'refresh data'

            # 2. Update payment request status to Rejected
            cursor.execute(
                """
                UPDATE gst_tbl_payment_request
                SET col_status = 'Rejected', col_instruction = %s
                WHERE col_id = %s;
                """,
                (rejectReason, paymentRequestId)
            )

            # 3. Fetch agent username and send email
            cursor.execute(
                "SELECT col_username FROM gst_tbl_login_data WHERE col_email = %s;",
                (agent_email,)
            )
            username_row = cursor.fetchone()
            agent_username = username_row[0] if username_row else agent_email

            sendStatusUpdateEmail(
                agentEmail=agent_email,
                agentUserName=agent_username,
                requestId=paymentRequestId,
                requesCustomerName=request_customer_name,
                requestStatus="Rejected",
                requestInstruction=rejectReason
            )

            print('Payment request rejected')
            return "success"

    except Exception as e:
        print('Error in reject_payment_request:')
        print(e)
        return 'server error'


def admin_pay_amount(requestId, amount, paymentMethod, transactionId, notes):
    try:
        print('in admin_pay_amount')
        with connection.cursor() as cursor:
            # 1. Ensure new payment columns exist on gst_tbl_request
            cursor.execute("""
                ALTER TABLE gst_tbl_request
                    ADD COLUMN IF NOT EXISTS col_paid_amount TEXT DEFAULT '',
                    ADD COLUMN IF NOT EXISTS col_payment_method TEXT DEFAULT '',
                    ADD COLUMN IF NOT EXISTS col_transaction_id TEXT DEFAULT '',
                    ADD COLUMN IF NOT EXISTS col_payment_notes TEXT DEFAULT '',
                    ADD COLUMN IF NOT EXISTS col_paid_at TIMESTAMPTZ;
            """)

            # Ensure col_balance exists on gst_tbl_ca_cs
            cursor.execute("""
                ALTER TABLE gst_tbl_ca_cs
                    ADD COLUMN IF NOT EXISTS col_balance INT DEFAULT 0;
            """)

            # 2. Fetch request details and validate status
            cursor.execute(
                "SELECT col_status, col_agent_email_id, col_name, col_assigned_ca_cs_id FROM gst_tbl_request WHERE col_id = %s;",
                (requestId,)
            )
            row = cursor.fetchone()
            if not row:
                return 'not_found'
            current_status = row[0]
            agent_email = row[1]
            customer_name = row[2]
            cacs_id = row[3]

            if current_status != 'Completed':
                return 'refresh data'

            if not cacs_id:
                return 'cacs_not_assigned'

            # Fetch CACS email
            cursor.execute("SELECT col_email FROM gst_tbl_ca_cs WHERE col_id = %s;", (cacs_id,))
            cacs_row = cursor.fetchone()
            if not cacs_row:
                return 'cacs_not_found'
            cacs_email = cacs_row[0]

            # 3. Update request to Paid with payment details
            cursor.execute(
                """
                UPDATE gst_tbl_request
                SET col_status = 'Paid',
                    col_paid_amount = %s,
                    col_payment_method = %s,
                    col_transaction_id = %s,
                    col_payment_notes = %s,
                    col_paid_at = NOW()
                WHERE col_id = %s;
                """,
                (str(amount), paymentMethod, transactionId, notes, requestId)
            )

            # 4. Record credit transaction for CA/CS
            cursor.execute(
                """
                INSERT INTO gst_tbl_transactions(col_type, col_amount, col_user_email, col_purpose, col_reference_id)
                VALUES('credit', %s, %s, 'ca_cs_payment', %s);
                """,
                (int(amount), cacs_email, str(requestId))
            )

            # 5. Update CA/CS balance
            cursor.execute(
                """
                UPDATE gst_tbl_ca_cs
                SET col_balance = col_balance + %s
                WHERE col_id = %s;
                """,
                (int(amount), cacs_id)
            )

            # 6. Send status email to Agent
            cursor.execute(
                "SELECT col_username FROM gst_tbl_login_data WHERE col_email = %s;",
                (agent_email,)
            )
            username_row = cursor.fetchone()
            agent_username = username_row[0] if username_row else agent_email
            sendStatusUpdateEmail(
                agentEmail=agent_email,
                agentUserName=agent_username,
                requestId=requestId,
                requesCustomerName=customer_name,
                requestStatus='Paid',
                requestInstruction=f'Amount: ₹{amount} | Method: {paymentMethod} | Txn ID: {transactionId}'
            )

            print('admin_pay_amount: payment recorded and status set to Paid for CA/CS')
            return 'success'

    except Exception as e:
        print(f'Error in admin_pay_amount: {e}')
        return 'server error'


def get_ca_cs_document(ca_cs_id):
    print(ca_cs_id)
    try:
        
        with connection.cursor() as cursor:
            cursor.execute(f"""
                    select col_id ,col_filename, col_content_type from gst_tbl_ca_cs_documents where col_ca_cs_id={ca_cs_id}
                """)
            data=cursor.fetchall()
            print(f'documents got {data}')
            return data
    except Exception as e:
        print(e)
        return []
    
def get_ca_cs_document_data(id):
    print(id)
    try:
        print('get_ca_cs_document_data')
        with connection.cursor() as cursor:
            cursor.execute(f"""
                    select col_content_type,col_file_data from gst_tbl_ca_cs_documents where col_id={id}
                """)
            data= cursor.fetchone()
            print(data)
            return data
    except Exception as e:
        print(e)



def get_agent_balance(token):
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        with connection.cursor() as cursor:
            cursor.execute(f"""
                        select col_balance from gst_tbl_agent_data where col_email = '{payload['email']}'
                        """)
            balance=cursor.fetchone()
            return 'success', balance[0]
    except jwt.ExpiredSignatureError:
        return "Token expired, Please login again!",0
    except jwt.InvalidTokenError:
        return "Invalid token, Please login again!",0
    except Exception as e:
        print(e)
        return 'server error',0
    


def get_transaction_data(token):
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        email=payload['email']
        with connection.cursor() as cursor:
            cursor.execute(f"""
                    select col_login_type from gst_tbl_login_data where col_email='{email}';
                """)
            data=cursor.fetchone()
            print(data[0])
            if(data[0]=='Admin'):
                cursor.execute("""
                        select * from gst_tbl_transactions order by col_created_at DESC
                    """)
            else:
                cursor.execute(f"""
                        select * from gst_tbl_transactions where col_user_email='{email}' order by col_created_at DESC
                    """)
            
            data=cursor.fetchall()
            print(data)
            return (data,"success")
    except jwt.ExpiredSignatureError:
        return ([],"Token expired, Please login again!")
    except jwt.InvalidTokenError:
        return ([],"Invalid token, Please login again!")
    except Exception as e:
        print(e)
        return ([],"data not found")



def complete_request(request_id,description,documents,token):
    print('id for update')
    print(request_id)
    print(token)
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        result=update_request_status(request_id,'Completed',description,attachment=documents)

        if result!="success":
            return result
        with connection.cursor() as cursor:

            for doc in documents:
                byte_data=doc.read()
                print("Name:",doc.name ) 
                print("Type:", doc.content_type)
                print("Size:", len(byte_data))
            
                cursor.execute(f"""
                                CREATE TABLE IF NOT EXISTS gst_tbl_completion_documents(
                                                        col_id SERIAL PRIMARY KEY,
                                                        col_request_id INT REFERENCES gst_tbl_request(col_id) ON DELETE CASCADE, -- link to request
                                                        col_filename TEXT,
                                                        col_content_type TEXT,
                                                        col_file_data BYTEA,
                                                        col_created_at TIMESTAMPTZ DEFAULT NOW()
                                                    );
                               INSERT INTO gst_tbl_completion_documents (col_request_id,col_filename,col_content_type,col_file_data)  VALUES (%s, %s, %s, %s);
                                """,(request_id,doc.name,doc.content_type,byte_data))
            cursor.execute(
                    f""" SELECT col_agent_email_id,col_id,col_name from gst_tbl_request where col_id = {request_id};"""
                    )
            row=cursor.fetchone()
            agent_email=row[0]
            request_id=row[1]
            reques_customer_name=row[2]
            cursor.execute(
                f"""
                    SELECT col_username from gst_tbl_login_data where col_email= %s;
                 """ ,(agent_email,)  
            )
            row=cursor.fetchone()
            agent_username=row[0]
            # sendStatusUpdateEmail(agentEmail=agent_email,agentUserName=agent_username , requestId=request_id,requesCustomerName=reques_customer_name,requestStatus="Completed",requestInstruction=description,attachments=documents)
    
            print('submitted completion request')
            return 'submitted'
    except jwt.ExpiredSignatureError:
        return "Token expired, Please login again!"
    except jwt.InvalidTokenError:
        return "Invalid token, Please login again!"
    except Exception as e:
        print(e)
        return 'server error'
    
def get_request_completion_document(request_id):
    print(request_id)
    try:
        
        with connection.cursor() as cursor:
            cursor.execute(f"""
                    select col_id ,col_filename, col_content_type from gst_tbl_completion_documents where col_request_id={request_id}
                """)
            data=cursor.fetchall()
            print(f'documents got {data}')
            return data
    except Exception as e:
        print(e)
        return []

def get_request_completion_document_data(id):
    try:
        print('get_request_document_data')
        with connection.cursor() as cursor:
            cursor.execute(f"""
                    select col_content_type,col_file_data from gst_tbl_completion_documents where col_id={id}
                """)
            data= cursor.fetchone()
            print(data)
            return data
    except Exception as e:
        print(e)


def get_agent_data_list(token):
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        email=payload['email']
        with connection.cursor() as cursor:
            cursor.execute(f"""
                    select col_login_type from gst_tbl_login_data where col_email='{email}';
                """)
            data=cursor.fetchone()
            print(data[0])
            if(data[0]=='Admin'):
                cursor.execute("""
                        select a.col_username,a.col_email,b.col_balance from gst_tbl_login_data as a join gst_tbl_agent_data as b on a.col_email=b.col_email;
                    """)
                data=cursor.fetchall()
                print(data)
                return (data,"success")
            else:
                return ([], "Unauthorized access")
                   
    except jwt.ExpiredSignatureError:
        return ([], "Token expired, Please login again!")
    except jwt.InvalidTokenError:
        return ([], "Invalid token, Please login again!")
    except Exception as e:
        print(f"Error in get_agent_data_list: {e}")
        return ([], "Server error")

def get_services():
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gst_tbl_service_categories(
                    col_id SERIAL PRIMARY KEY,
                    col_name TEXT,
                    col_created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS gst_tbl_services(
                    col_id SERIAL PRIMARY KEY,
                    col_name TEXT,
                    col_price TEXT,
                    col_required_documents TEXT DEFAULT '',
                    col_category_id INT REFERENCES gst_tbl_service_categories(col_id) ON DELETE SET NULL,
                    col_created_at TIMESTAMPTZ DEFAULT NOW()
                );
                ALTER TABLE gst_tbl_services ADD COLUMN IF NOT EXISTS col_required_documents TEXT DEFAULT '';
                ALTER TABLE gst_tbl_services ADD COLUMN IF NOT EXISTS col_category_id INT REFERENCES gst_tbl_service_categories(col_id) ON DELETE SET NULL;
                
                SELECT s.col_id, s.col_name, s.col_price, s.col_required_documents, c.col_name as col_category_name, s.col_category_id 
                FROM gst_tbl_services s 
                LEFT JOIN gst_tbl_service_categories c ON s.col_category_id = c.col_id
                ORDER BY s.col_created_at DESC;
            """)
            data = cursor.fetchall()
            return (data, "success")
    except Exception as e:
        print(f"Error in get_services: {e}")
        return ([], "error")

def add_service(name, price, required_documents='', category_id=None):
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO gst_tbl_services (col_name, col_price, col_required_documents, col_category_id) VALUES (%s, %s, %s, %s);
            """, (name, price, required_documents, category_id))
            return "success"
    except Exception as e:
        print(f"Error in add_service: {e}")
        return "error"

def update_service(service_id, name, price, required_documents='', category_id=None):
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE gst_tbl_services SET col_name = %s, col_price = %s, col_required_documents = %s, col_category_id = %s WHERE col_id = %s;
            """, (name, price, required_documents, category_id, service_id))
            return "success"
    except Exception as e:
        print(f"Error in update_service: {e}")
        return "error"

def delete_service(service_id):
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                DELETE FROM gst_tbl_services WHERE col_id = %s;
            """, (service_id,))
            return "success"
    except Exception as e:
        print(f"Error in delete_service: {e}")
        return "error"

def get_service_categories():
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gst_tbl_service_categories(
                    col_id SERIAL PRIMARY KEY,
                    col_name TEXT,
                    col_created_at TIMESTAMPTZ DEFAULT NOW()
                );
                SELECT col_id, col_name FROM gst_tbl_service_categories ORDER BY col_name ASC;
            """)
            data = cursor.fetchall()
            return (data, "success")
    except Exception as e:
        print(f"Error in get_service_categories: {e}")
        return ([], "error")

def add_service_category(name):
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO gst_tbl_service_categories (col_name) VALUES (%s);
            """, (name,))
            return "success"
    except Exception as e:
        print(f"Error in add_service_category: {e}")
        return "error"

def update_service_category(category_id, name):
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE gst_tbl_service_categories SET col_name = %s WHERE col_id = %s;
            """, (name, category_id))
            return "success"
    except Exception as e:
        print(f"Error in update_service_category: {e}")
        return "error"

def delete_service_category(category_id):
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                DELETE FROM gst_tbl_service_categories WHERE col_id = %s;
            """, (category_id,))
            return "success"
    except Exception as e:
        print(f"Error in delete_service_category: {e}")
        return "error"

def get_ca_cs_slots(ca_cs_id):
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gst_tbl_ca_cs_slots(
                    col_id SERIAL PRIMARY KEY,
                    col_ca_cs_id INT REFERENCES gst_tbl_ca_cs(col_id) ON DELETE CASCADE,
                    col_day TEXT,
                    col_slot_number INT
                );
            """)
            cursor.execute("""
                SELECT col_day, col_slot_number
                FROM gst_tbl_ca_cs_slots
                WHERE col_ca_cs_id = %s
                ORDER BY col_day, col_slot_number;
            """, (ca_cs_id,))
            data = cursor.fetchall()
            return (data, "success")
    except Exception as e:
        print(f"Error in get_ca_cs_slots: {e}")
        return ([], "error")

def update_ca_cs_slots(ca_cs_id, slots):
    # slots: list of { 'day': 'Monday', 'slot_number': 5 }
    try:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM gst_tbl_ca_cs_slots WHERE col_ca_cs_id = %s", (ca_cs_id,))
            for slot in slots:
                cursor.execute("""
                    INSERT INTO gst_tbl_ca_cs_slots (col_ca_cs_id, col_day, col_slot_number)
                    VALUES (%s, %s, %s);
                """, (ca_cs_id, slot['day'], slot['slot_number']))
            return "success"
    except Exception as e:
        print(f"Error in update_ca_cs_slots: {e}")
        return "error"

def get_ca_cs_special_slots(ca_cs_id):
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gst_tbl_ca_cs_special_slots(
                    col_id SERIAL PRIMARY KEY,
                    col_ca_cs_id INT REFERENCES gst_tbl_ca_cs(col_id) ON DELETE CASCADE,
                    col_date DATE,
                    col_slot_number INT
                );
            """)
            cursor.execute("""
                SELECT col_date, col_slot_number
                FROM gst_tbl_ca_cs_special_slots
                WHERE col_ca_cs_id = %s
                ORDER BY col_date DESC, col_slot_number;
            """, (ca_cs_id,))
            data = cursor.fetchall()
            return (data, "success")
    except Exception as e:
        print(f"Error in get_ca_cs_special_slots: {e}")
        return ([], "error")

def update_ca_cs_special_slots(ca_cs_id, date, slots):
    # slots: list of slot numbers e.g. [5, 6, 7]
    try:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM gst_tbl_ca_cs_special_slots WHERE col_ca_cs_id = %s AND col_date = %s", (ca_cs_id, date))
            for slot_number in slots:
                cursor.execute("""
                    INSERT INTO gst_tbl_ca_cs_special_slots (col_ca_cs_id, col_date, col_slot_number)
                    VALUES (%s, %s, %s);
                """, (ca_cs_id, date, slot_number))
            return "success"
    except Exception as e:
        print(f"Error in update_ca_cs_special_slots: {e}")
        return "error"

def get_my_cacs_data(token):
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        email = payload['email']
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM gst_tbl_ca_cs WHERE col_email='{email}'")
            row = cursor.fetchone()
            if row:
                return (row, 'success')
            else:
                return (None, 'not found')
    except jwt.ExpiredSignatureError:
        return (None, "Token expired, Please login again!")
    except jwt.InvalidTokenError:
        return (None, "Token expired, Please login again!")
        return (None, 'server error')

def update_admin_bank_details(bank_name, account_name, account_number, ifsc_code, upi_id, token, upi_qr_code=None):
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        email = payload['email']
        with connection.cursor() as cursor:
            # Verify if user is Admin
            cursor.execute(f"SELECT col_login_type FROM gst_tbl_login_data WHERE col_email='{email}';")
            data = cursor.fetchone()
            if not data or data[0] != 'Admin':
                return "Unauthorized request"

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gst_tbl_admin_bank_details(
                    col_id SERIAL PRIMARY KEY,
                    col_bank_name TEXT,
                    col_account_name TEXT,
                    col_account_number TEXT,
                    col_ifsc_code TEXT,
                    col_upi_id TEXT,
                    col_upi_qr_code_data BYTEA,
                    col_upi_qr_code_content_type TEXT,
                    col_updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                
                -- Ensure columns exist if table was already created
                ALTER TABLE gst_tbl_admin_bank_details 
                ADD COLUMN IF NOT EXISTS col_upi_qr_code_data BYTEA,
                ADD COLUMN IF NOT EXISTS col_upi_qr_code_content_type TEXT;
            """)
            
            # Check if record exists
            cursor.execute("SELECT col_id FROM gst_tbl_admin_bank_details LIMIT 1")
            existing = cursor.fetchone()
            
            qr_data = None
            qr_content_type = None
            if upi_qr_code:
                qr_data = upi_qr_code.read()
                qr_content_type = upi_qr_code.content_type

            if existing:
                if upi_qr_code:
                    cursor.execute("""
                        UPDATE gst_tbl_admin_bank_details 
                        SET col_bank_name = %s, col_account_name = %s, col_account_number = %s, 
                            col_ifsc_code = %s, col_upi_id = %s, col_upi_qr_code_data = %s, 
                            col_upi_qr_code_content_type = %s, col_updated_at = NOW()
                        WHERE col_id = %s
                    """, (bank_name, account_name, account_number, ifsc_code, upi_id, qr_data, qr_content_type, existing[0]))
                else:
                    cursor.execute("""
                        UPDATE gst_tbl_admin_bank_details 
                        SET col_bank_name = %s, col_account_name = %s, col_account_number = %s, 
                            col_ifsc_code = %s, col_upi_id = %s, col_updated_at = NOW()
                        WHERE col_id = %s
                    """, (bank_name, account_name, account_number, ifsc_code, upi_id, existing[0]))
            else:
                cursor.execute("""
                    INSERT INTO gst_tbl_admin_bank_details (col_bank_name, col_account_name, col_account_number, 
                                                            col_ifsc_code, col_upi_id, col_upi_qr_code_data, 
                                                            col_upi_qr_code_content_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (bank_name, account_name, account_number, ifsc_code, upi_id, qr_data, qr_content_type))
            
            return 'success'
    except jwt.ExpiredSignatureError:
        return "Token expired, Please login again!"
    except jwt.InvalidTokenError:
        return "Invalid token, Please login again!"
    except Exception as e:
        print(f"Error in update_admin_bank_details: {e}")
        return 'server error'

def get_admin_bank_details():
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gst_tbl_admin_bank_details(
                    col_id SERIAL PRIMARY KEY,
                    col_bank_name TEXT,
                    col_account_name TEXT,
                    col_account_number TEXT,
                    col_ifsc_code TEXT,
                    col_upi_id TEXT,
                    col_upi_qr_code_data BYTEA,
                    col_upi_qr_code_content_type TEXT,
                    col_updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            cursor.execute("SELECT col_bank_name, col_account_name, col_account_number, col_ifsc_code, col_upi_id, col_upi_qr_code_data IS NOT NULL FROM gst_tbl_admin_bank_details LIMIT 1")
            data = cursor.fetchone()
            if data:
                return ({
                    'bankName': data[0],
                    'accountName': data[1],
                    'accountNumber': data[2],
                    'ifscCode': data[3],
                    'upiId': data[4],
                    'hasQRCode': data[5]
                }, 'success')
    except Exception as e:
        print(f"Error getting admin bank details: {e}")
        return None, "error"

def get_admin_bank_qr_code_data():
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT col_upi_qr_code_content_type, col_upi_qr_code_data FROM gst_tbl_admin_bank_details LIMIT 1")
            data = cursor.fetchone()
            return data
    except Exception as e:
        print(f"Error in get_admin_bank_qr_code_data: {e}")
        return None


def update_cacs_bank_details(bankName, accountName, accountNumber, ifscCode, upiId, token):
    try:
        connection = psycopg2.connect(**db_config)
        with connection.cursor() as cursor:
            # Verify CA/CS token and get their ID
            try:
                user_data = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
            except jwt.ExpiredSignatureError:
                return 'invalid_token'
            except jwt.InvalidTokenError:
                return 'invalid_token'
            
            if not user_data:
                return 'invalid_token'
            
            cacs_email = user_data.get('email')
            
            # Find the CA/CS ID
            cursor.execute("SELECT col_id FROM gst_tbl_ca_cs WHERE col_email = %s;", (cacs_email,))
            cacs_row = cursor.fetchone()
            if not cacs_row:
                return 'cacs_not_found'
            
            cacs_id = cacs_row[0]

            cursor.execute("""
                INSERT INTO gst_tbl_cacs_bank_details 
                (col_cacs_id, col_bank_name, col_account_name, col_account_number, col_ifsc_code, col_upi_id, col_updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (col_cacs_id) DO UPDATE SET 
                col_bank_name = EXCLUDED.col_bank_name,
                col_account_name = EXCLUDED.col_account_name,
                col_account_number = EXCLUDED.col_account_number,
                col_ifsc_code = EXCLUDED.col_ifsc_code,
                col_upi_id = EXCLUDED.col_upi_id,
                col_updated_at = NOW()
            """, (cacs_id, bankName, accountName, accountNumber, ifscCode, upiId))

            connection.commit()
            return 'success'
    except Exception as e:
        print(f"Database Error in update_cacs_bank_details: {e}")
        return 'error'
    finally:
        if 'connection' in locals() and connection:
            connection.close()


def get_cacs_bank_details(cacs_id=None, token=None):
    try:
        connection = psycopg2.connect(**db_config)
        with connection.cursor() as cursor:
            target_cacs_id = None
            
            # If an explicit ID is provided (e.g., from Admin), prioritize that
            if cacs_id:
                target_cacs_id = int(cacs_id)
                
            # Otherwise, if a token is provided, the CA/CS is fetching their own details
            elif token:
                try:
                    user_data = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
                except Exception:
                    return None, 'invalid_token'
                
                if not user_data:
                    return None, 'invalid_token'
                
                cacs_email = user_data.get('email')
                cursor.execute("SELECT col_id FROM gst_tbl_ca_cs WHERE col_email = %s;", (cacs_email,))
                cacs_row = cursor.fetchone()
                if cacs_row:
                    target_cacs_id = cacs_row[0]
                
            if not target_cacs_id:
               return None, 'cacs_not_found' 

            cursor.execute("""
                SELECT col_bank_name, col_account_name, col_account_number, col_ifsc_code, col_upi_id, col_updated_at
                FROM gst_tbl_cacs_bank_details
                WHERE col_cacs_id = %s;
            """, (target_cacs_id,))
            
            row = cursor.fetchone()
            if not row:
                return None, 'not_found'
                
            details = {
                'bankName': row[0],
                'accountName': row[1],
                'accountNumber': row[2],
                'ifscCode': row[3],
                'upiId': row[4],
                'updatedAt': row[5].strftime("%Y-%m-%d %H:%M:%S") if row[5] else None
            }
            return details, 'success'
    except Exception as e:
        print(f"Error getting CA/CS bank details: {e}")
        return None, "error"
    finally:
         if 'connection' in locals() and connection:
            connection.close()
