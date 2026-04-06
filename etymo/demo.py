import requests
url = "https://5174b1de1fac.ngrok-free.app/api/"
headers = {"ngrok-skip-browser-warning": "true"}
r = requests.get(url, headers=headers)
print(r.text) 