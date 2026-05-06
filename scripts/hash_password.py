"""
Generate bcrypt-hashed passwords for config/auth.yaml.

Usage:
    python scripts/hash_password.py yourpassword
    python scripts/hash_password.py pass1 pass2 pass3
"""
import sys
import bcrypt

if len(sys.argv) < 2:
    print(__doc__)
    sys.exit(1)

for pw in sys.argv[1:]:
    h = bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()
    print(f"Password : {pw!r}")
    print(f"Hash     : {h}")
    print()

print("Paste the hash into the 'password' field in config/auth.yaml")
