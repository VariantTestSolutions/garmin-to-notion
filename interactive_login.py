import os
from garminconnect import Garmin

def get_mfa():
    return input("\n>>> Enter MFA Code: ")

email = input("Email: ")
password = input("Password: ")

# Define the folder (starts with a dot, which is fine on Windows)
token_dir = os.path.expanduser("~/.garminconnect")
os.makedirs(token_dir, exist_ok=True)

client = Garmin(email, password, prompt_mfa=get_mfa)

# IMPORTANT: You must pass the directory to login() to trigger the save
print("[status] Logging in and saving tokens...")
client.login(token_dir) 

print(f"\n[success] Check this exact path: {os.path.join(token_dir, 'garmin_tokens.json')}")