import os.path
from googleapiclient.discovery import build
from oauth2client import file, client, tools
from apiclient.http import MediaIoBaseDownload
from apiclient import errors
from multiprocessing import Pool
import io

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

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']

queue_file = []
speed = 5
drive_id = '14Ta-rCJNscUCfWKe5_1LLUMQEpxLHSOV'

def download_folder(service, folder_id, location, folder_name):
    global queue_file
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
            try:
                temp['size'] = item['size']
            except:
                temp['size'] = '2000000'
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
    print('Downloading file {} ...'.format(file_name))
    if 'google-apps' in mimetype:
        try:
            ext = file_name.split('.')[-1]
            mimeType = 'application/octet-stream'
            if ext in mimedict.keys():
                mimeType = mimedict[ext]
            request = service.files().export_media(fileId=file_id, mimeType=mimeType)
            fh = io.FileIO('{}{}'.format(location, file_name), 'wb')
            downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
        except errors.HttpError as e:
            print('Downloading file {} fail, Check error: {}'.format(file_name, e))
            return False
    else:
        try:
            request = service.files().get_media(fileId=file_id)
            fh = io.FileIO('{}{}'.format(location, file_name), 'wb')
            downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
        except errors.HttpError as e:
            print('Downloading file {} fail, Check error: {}'.format(file_name, e))
            return False
    print('Download file {} success.'.format(file_name))
    return True

def balance_files(array_file, speed=5):
    files = sorted(array_file, key=lambda x: int(x['size']), reverse=True)
    # print(sum(map(lambda x: int(x['size']), files)))
    split_array = lambda A, n=5: [A[i:i + n] for i in range(0, len(A), n)]
    queue_raw = split_array(files, speed)
    for i in queue_raw[1::2]: i.sort(key=lambda x: int(x['size']))
    queue = []
    for i in range(speed):
        temp = []
        for j in queue_raw:
            if len(j) > i:
                temp.append(j[i])
        queue.append(temp)
    # for i in queue:
    #     print(sum(map(lambda x: int(x['size']), i)))
    return queue

def download_file(attemp):
    service, files = attemp
    for i in files:
        download_file_single(service, i['id'], i['mimeType'], i['location'], i['name'])

def main():
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
    # else:
    #     file_name = objects['name']
    #     test_path = location + file_name
    #     count_duplicate = 0
    #     while os.path.isfile(test_path):
    #         count_duplicate += 1
    #         test_path = location + file_name + '(' + str(count_duplicate) + ')'
    #     file_name = objects['name'] + '(' + str(count_duplicate) + ')'
    #     temp = {
    #         'id': drive_id,
    #         'name': file_name,
    #         'location': location
    #     }
    #     queue_file.append(temp)
    global queue_file
    queue_file = balance_files(queue_file, speed)
    print(queue_file)
    queue = []
    for i in queue_file: queue.append(tuple([service, i]))
    print(queue)
    pool = Pool(speed)
    result = pool.map(download_file, queue)
    pool.close()
    pool.join()


if __name__ == "__main__":
    main()