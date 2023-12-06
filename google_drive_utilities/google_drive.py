'''
Created on Dec 31, 2018

@author: grovesr
'''
from __future__ import print_function
import sys
import os
import io
from pathlib import Path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.errors import HttpError

class GoogleDriveException(Exception):
    '''Generic exception to raise GoogleDrive errors.'''
    def __init__(self, msg):
        super(GoogleDriveException).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

class GoogleDrive(object):
    '''
    classdocs
    '''
    service = None
    verbose = False
    settingsfile = ''
    settings = {}
    database_secrets = {}

    def __init__(self, keyfile=None, tokenfile=None, scopes=None, verbose=False):
        '''
        Constructor
        '''
        super(GoogleDrive).__init__(type(self))
        self.keyfile = keyfile
        self.tokenfile = tokenfile
        self.scopes = scopes
        self.verbose = verbose
        self.root = None
        if self.keyfile is not None and self.tokenfile is not None and self.scopes is not None:
            self.setup()
        return None

    def setup(self):
        self.get_service(self.keyfile, self.tokenfile, self.scopes)
        self.root = self.get_root()
        return self.service
    
    def get_service(self, keyfile, tokenfile, scopes):
        """Get a service that commelse:
                    flow = InstalledAppFlow.from_client_secrets_file(
                            keyfile, scopes
                            )
                    credentials = flow.run_local_server(port=0)
                    # Save theunicates to a Google API.
        Returns:
          A service that is connected to the specified API.
        """
        if self.verbose:
            sys.stdout.write("Acquiring credentials...\n")
        try:
            credentials = None
            if os.path.exists(tokenfile):
                credentials = Credentials.from_authorized_user_file(tokenfile, scopes)
            # If there are no (valid) credentials available, let the user log in.
            if not credentials or not credentials.valid:
                if credentials and credentials.expired and credentials.refresh_token:
                    credentials.refresh(Request())
                    if self.verbose:
                        sys.stdout.write("Credentials refreshed!\n")
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                            keyfile, scopes
                            )
                    credentials = flow.run_local_server(port=0)
                    # Save the credentials for the next run
                    tokenpath = Path(tokenfile)
                    if not tokenpath.parent.is_dir():
                        try:
                            tokenpath.parent.mkdir( parents=True, exist_ok=True )
                        except Exception as e:
                            msg = "[%s] problem saving token file %s" % (tokenfile, str(e))
                            if self.verbose:
                                sys.stderr.write(msg + "\n")
                            raise GoogleDriveException(msg)
                    with tokenpath.open(mode="w") as f:
                        f.write(credentials.to_json())
                    tokenpath.chmod(0o644)
        except Exception as e:
            msg = "problem getting credentials %s" % str(e)
            if self.verbose:
                sys.stderr.write(msg + "\n")
            raise GoogleDriveException(msg)

        # Build the service object for use with any API
        if self.verbose:
            sys.stdout.write("Acquiring service...\n")
        self.service = build(serviceName="drive", version="v3", credentials=credentials,
                                  cache_discovery=False)

        if self.verbose:
            sys.stdout.write("Service acquired!\n")
        return self.service

    def get_root(self):
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        try:
            rootdir = self.service.files().get(fileId='root').execute()
        except HttpError:
            msg = "unable to determine root directory"
            if self.verbose:
                sys.stdout.write("%s\n" % msg)
            raise GoogleDriveException(msg)
        return rootdir

    def create_new_folder(self, name, parentid='root' ):
        """Will create a new folder under the parentid, root if none supplied,
        Retruns:
            The folder resource
        """
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        existingdirs = self.list_files_in_drive(query="name = '%s' and mimeType = 'application/vnd.google-apps.folder' and '%s' in parents and not trashed" % (name, str(parentid)))
        if len(existingdirs) == 0:
            folder_metadata = {
            'name' : name,
            'mimeType' : 'application/vnd.google-apps.folder',
            'parents'  : [str(parentid)],
            }
            try:
                self.service.files().create(body=folder_metadata, fields='id, name').execute()
            except HttpError as e:
                msg = "[%s] unable to create folder %s" % (name, e.reason)
                if self.verbose:
                    sys.stdout.write("%s\n" % msg)
                raise GoogleDriveException(msg)
            # check to see that it was created
            newdirs = self.list_files_in_drive(query="name = '%s' and mimeType = 'application/vnd.google-apps.folder' and not trashed" % name)
            if len(newdirs) == 0:
                msg = "problem creating folder '%s'" % name
                if self.verbose:
                    sys.stdout.write("%s\n" % msg)
                raise GoogleDriveException(msg)
            else:
                folder = newdirs[0]
                if self.verbose:
                    sys.stdout.write("Folder %s creation complete, ID=%s\n" % (folder.get('name'), folder.get('id')))
        else:
            msg = "folder '%s' already exists. Unable to create" % name
            if self.verbose:
                sys.stdout.write("%s\n" % msg)
            raise GoogleDriveException(msg)
        return folder

    def delete_file(self, fileid):
        """Will delete the given fileid on the supplied GDrive,
        Retruns:
            True if sucessful
        """
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        try:
            file = self.service.files().get(fileId=fileid, fields='name').execute()
            self.service.files().delete(fileId=fileid).execute()
            if self.verbose:
                sys.stdout.write("deleted file %s fileid=%s\n" % (file.get('name'), fileid))
            result = True
        except HttpError as e:
            msg = "unable to delete file %s fileid=%s: %s" % (file.get('name'), e.reason)
            if self.verbose:
                sys.stdout.write("%s\n" % msg)
            raise GoogleDriveException(msg)
        return result


    def upload_file_to_folder(self, folderID, fileName):
        """Uploads the file to the specified folder id on the said Google Drive
        Returns:
                file resource
        """
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        file_metadata = None
        if folderID is None:
            file_metadata = {
                'name' : os.path.basename(fileName)
            }
        else:
            file_metadata = {
                  'name' : os.path.basename(fileName),
                  'parents': [ folderID ]
            }

        media = MediaFileUpload(fileName, resumable=True)
        try:
            folder = self.service.files().get(fileId=folderID).execute()
            file = self.service.files().create(body=file_metadata, media_body=media, fields='name,id,size,parents').execute()
            if self.verbose:
                sys.stdout.write("Uploaded file %s size= %s ID=%s to: %s\n" % (file.get('name'), file.get('size'), file.get('id'), folder.get('name')))
        except HttpError as e:
            msg = "unable to upload file %s: %s" % (fileName, e.reason)
            if self.verbose:
                sys.stdout.write("%s\n" % msg)
            raise GoogleDriveException(msg)

        return file

    def download_file(self, fileId, fileName):
        """Downloads the fileId file
        Returns:
                media object
        """
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        if fileId is None or fileName is None:
            return None

        request = self.service.files().get_media(fileId=fileId)
        try:
            file = self.service.files().get(fileId=fileId, fields='id,name,mimeType').execute()
            fileName = file.get("name")
        except HttpError as e:
            msg = "unable to access file ID %s %s" % (fileId, e.reason)
            if self.verbose:
                sys.stdout.write("%s\n" % msg)
            raise GoogleDriveException(msg)
        indx = 1
        name, ext = os.path.splitext(fileName)
        while os.path.isfile(name + ext):
            name = "%s(%d)" % (name, indx)
            indx = indx +1
        fh = io.FileIO(name + ext, mode='wb')
        downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024)
        done = False
        if self.verbose:
            sys.stdout.write("Downloading file %s, id=%s, mimeType=%s\n" % (name + ext, fileId, file.get('mimeType')))
        while done is False:
            try:
                status, done = downloader.next_chunk()
            except HttpError as e:
                msg = "failed to download file %s: %s" %(fh.name, e.reason)
                fh.close()
                os.remove(fh.name)
                raise GoogleDriveException(msg)
            if status:
                if self.verbose:
                    sys.stdout.write("Download %d%%.\n" % int(status.progress() * 100))
        if self.verbose:
            sys.stdout.write("Download Complete!\n")
        return True

    def list_files_in_drive(self, query=None, pathquery=None, fields="id,name,size,modifiedTime", includetrashed=False):
        """Queries Google Drive for all files satisfying query
        Returns:
                list of file resources
        """
        if pathquery is not None and query is not None:
            raise GoogleDriveException("You can't specify path and pathquery at the same time")
        if pathquery is not None:
            filename = pathquery.split('/')[-1]
            query = "name = '%s'" % filename
        if not includetrashed:
            query = query + " and not trashed"
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        try:
            if len(query) > 0:
                files= self.service.files().list(q=query).execute()
            else:
                files = self.service.files().list().execute()
        except HttpError as e:
            msg = "unable to list files from query '%s': %s" %(query, e.reason)
            if self.verbose:
                sys.stdout.write("s\n" % msg)
            raise GoogleDriveException(msg)
        files = files.get('files')
        paths = []
        ids = []
        for file in files:
            pathlist, pathstring = self.get_path(file.get('id'))
            if pathquery is None or pathquery == pathstring: 
                ids.append(pathlist[-1])
                paths.append(pathstring)
                if self.verbose:
                    sys.stdout.write("%s (path IDs'%s', size='%s', modified='%s')\n" % (pathstring,
                                                                                     str(pathlist),
                                                                                     file.get('size'),
                                                                                     file.get('modifiedTime')))
        return paths, ids

    def get_path(self, fileid=None):
        """ return the path to the file"""
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        if fileid is None:
            raise GoogleDriveException("no fileid passed into get_path command")
        try:
            if fileid is not None:
                file= self.service.files().get(fileId=fileid, fields='name,id,parents').execute()
        except HttpError:
            msg = "unable to find file from id '%s'" % str(id)
            if self.verbose:
                sys.stdout.write("s\n" % msg)
            raise GoogleDriveException(msg)
        parents = file.get('parents')
        # only return path traced through the first parent listed for each file
        
        if parents is None or self.root.get('id') in parents:
            # we have found the root directory
            pathlist = [file.get('id')]
            return pathlist, '/' + file.get('name')
        else:
            parent = parents[0]
            pathlist, thisparentpath = self.get_path(fileid=parent)
            pathlist.append(file.get('id'))
            return pathlist, thisparentpath + '/' + file.get('name')
