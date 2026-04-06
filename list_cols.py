import os
import django
from django.db import connection

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

with connection.cursor() as cursor:
    cursor.execute("SELECT * FROM gst_tbl_transactions LIMIT 0")
    cols = [desc[0] for desc in cursor.description]
    print("\n".join(cols))
