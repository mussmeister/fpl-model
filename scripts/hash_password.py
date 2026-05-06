"""
Generate bcrypt-hashed passwords for config/auth.yaml.

Usage:
    python scripts/hash_password.py yourpassword
    python scripts/hash_password.py pass1 pass2 pass3
"""
import sys

try:
    import streamlit_authenticator as stauth
except ImportError:
    print("streamlit-authenticator not installed.")
    print("Run: pip install streamlit-authenticator")
    sys.exit(1)

if len(sys.argv) < 2:
    print(__doc__)
    sys.exit(1)

passwords = sys.argv[1:]
hashed    = stauth.Hasher(passwords).generate()

for pw, h in zip(passwords, hashed):
    print(f"Password : {pw!r}")
    print(f"Hash     : {h}")
    print()

print("Paste the hash into the 'password' field in config/auth.yaml")
