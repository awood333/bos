'''utilities\_test_sa_access.py'''
from utilities.gdrive_auth import authenticate_gdrive
from googleapiclient.discovery import build

creds = authenticate_gdrive()
svc = build('drive', 'v3', credentials=creds)

q = "name='COWS' and trashed=false"
r = svc.files().list(q=q, fields='files(id,name,mimeType,parents,shared)', pageSize=20).execute()
items = r.get('files', [])
print(f"Search for 'COWS' found {len(items)} items:")
for f in items:
    print(f)

print()
q2 = "trashed=false"
r2 = svc.files().list(q=q2, fields='files(id,name,mimeType,parents,shared)', pageSize=30).execute()
items2 = r2.get('files', [])
print(f"All accessible items ({len(items2)}):")
for f in items2:
    print(f)
