import os
import django
from django.db import connection

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

with connection.cursor() as cursor:
    try:
        print("Checking/Creating gst_tbl_cacs_bank_details table...")
        cursor.execute("""
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
        
        # Verify unique/PK constraint (it's already PK, but we'll try to add constraint for robustness if needed)
        try:
            cursor.execute("ALTER TABLE gst_tbl_cacs_bank_details ADD CONSTRAINT col_cacs_id_unique UNIQUE (col_cacs_id);")
            print("Success adding unique constraint.")
        except Exception:
            print("Unique constraint already exists or handled by PK.")

        # Perform a test query
        print("Performing test query...")
        cursor.execute("SELECT * FROM gst_tbl_cacs_bank_details LIMIT 1;")
        print("Query successful.")
        
    except Exception as e:
        print("Error during database fix:", e)
