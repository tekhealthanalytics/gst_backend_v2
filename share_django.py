import subprocess
from pyngrok import ngrok

# Start Django server
subprocess.Popen(["python", "manage.py", "runserver", "0.0.0.0:8000"])

# Open Ngrok tunnel
public_url = ngrok.connect(8000)
print(f"Public URL: {public_url}")
print("Your Django server is now accessible over the internet.")
