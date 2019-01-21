import os.path
from googleapiclient.discovery import build
from oauth2client import file, client, tools
from apiclient.http import MediaIoBaseDownload
from apiclient import errors
import io
import time
import sys

mimedict = {
    'csv': 'text/csv',
    'doc': 'application/vnd.oasis.opendocument.text',
    'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'epub': 'application/epub+zip',
    'htm': 'text/html',
    'html': 'text/html',
    'jpeg': 'image/jpeg',
    'json': 'application/vnd.google-apps.script+json',
    'png': 'image/png',
    'pdf': 'application/pdf',
    'ppt': 'application/vnd.oasis.opendocument.presentation',
    'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'rtf': 'application/rtf',
    'svg': 'image/svg+xml',
    'txt': 'text/plain',
    'tsv': 'text/tab-separated-values',
    'xhtml': 'application/xhtml+xml',
    'xls': 'application/x-vnd.oasis.opendocument.spreadsheet',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
}

queue_file = []
speed = 3
drive_id = '14Ta-rCJNscUCfWKe5_1LLUMQEpxLHSOV'

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']

def download_folder(service, folder_id, location, folder_name):
    path = location + folder_name + '/'
    os.mkdir(path)
    files = service.files().list(
        q="'{}' in parents".format(folder_id),
        fields='files(id, name, mimeType, size)').execute()
    items = files.get('files', [])

    for item in items:
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            download_folder(service, item['id'], path, item['name'])
        else:
            temp = {
                'id': item['id'],
                'name': item['name'],
                'mimeType': item['mimeType'],
                'location': path
            }
            queue_file.append(temp)

def download_file_single(service, file_id, mimetype, location, file_name):
    # Check file existed
    test_path = location + file_name
    count_duplicate = 0
    while os.path.isfile(test_path):
        count_duplicate += 1
        test_path = location + file_name + '(' + str(count_duplicate) + ')'
    if count_duplicate != 0:
        file_name = file_name + '(' + str(count_duplicate) + ')'
    sys.stdout.write("\rDownload {}".format(file_name))
    sys.stdout.flush()
    if 'google-apps' in mimetype:
        try:
            ext = file_name.split('.')[-1]
            mimeType = 'application/octet-stream'
            if ext in mimedict.keys():
                mimeType = mimedict[ext]
            request = service.files().export_media(fileId=file_id, mimeType=mimeType)
            fh = io.FileIO('{}{}'.format(location, file_name), 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                sys.stdout.write("\rDownload {} with {}%".format(file_name, int(status.progress() * 100)))
                sys.stdout.flush()
        except errors.HttpError as e:
            sys.stdout.write('\rDownload {} fail\n'.format(file_name))
            print(e)
            return False
    else:
        try:
            request = service.files().get_media(fileId=file_id)
            fh = io.FileIO('{}{}'.format(location, file_name), 'wb')
            downloader = MediaIoBaseDownload(fh, request), chunksize=1024*1024)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                sys.stdout.write("\rDownload {} with {}%".format(file_name, int(status.progress() * 100)))
                sys.stdout.flush()
        except errors.HttpError as e:
            sys.stdout.write('\rDownload {} fail\n'.format(file_name))
            print('Check error: ', e)
            return False
    sys.stdout.write(' success\n')
    return True

def download_file(service, queue_file):
    pass

creds = None
store = file.Storage('token.json')
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
    creds = tools.run_flow(flow, store)

service = build('drive', 'v3', credentials=creds)

pwd = os.path.dirname(os.path.abspath(__file__))
location = pwd + '/download/'
if not os.path.isdir(location):
    os.mkdir(location)

objects = service.files().get(fileId='{}'.format(drive_id), fields="name, mimeType").execute()
if objects['mimeType'] == 'application/vnd.google-apps.folder':
    folder_name = objects['name']
    # Check folder existed
    test_path = location + folder_name + '/'
    count_duplicate = 0
    while os.path.isdir(test_path):
        count_duplicate += 1
        test_path = location + folder_name + '(' + str(count_duplicate) + ')/'
    if count_duplicate != 0:
        folder_name = objects['name'] + '(' + str(count_duplicate) + ')'

    download_folder(service, drive_id, location, folder_name)
else:
    file_name = objects['name']
    # Check file existed
    test_path = location + file_name
    count_duplicate = 0
    while os.path.isfile(test_path):
        count_duplicate += 1
        test_path = location + file_name + '(' + str(count_duplicate) + ')'
    file_name = objects['name'] + '(' + str(count_duplicate) + ')'
    temp = {
        'id': drive_id,
        'name': file_name,
        'location': location
    }
    queue_file.append(temp)
print(queue_file)
for item in queue_file:
    download_file_single(service, item['id'], item['mimeType'], item['location'], item['name'])