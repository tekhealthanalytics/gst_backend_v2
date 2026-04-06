import os
import django

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from etymo.database import ensure_all_tables

if __name__ == "__main__":
    print("Starting database initialization...")
    ensure_all_tables()
    print("Done. Use Admin (admin@gst.com / admin123) to login.")
