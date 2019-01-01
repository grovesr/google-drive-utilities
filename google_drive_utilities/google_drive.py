'''
Created on Dec 31, 2018

@author: grovesr
'''
from __future__ import print_function
import logging
import json
import sys
import os
import io
from googleapiclient import discovery
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials
from logging.handlers import SMTPHandler
logger = logging.getLogger(__name__)

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
    secrets = {}

    def __init__(self, settingsfile='', settings={}):
        '''
        Constructor
        '''
        super(GoogleDrive).__init__(type(self))
        self.settingsfile = settingsfile
        self.settings = settings
        if len(self.settingsfile) > 0:
            self.setup()
            return None
        if len(self.settings.keys()) > 0:
            self.setup()
        
        
    def get_service(self, keyfile, scopes):
        """Get a service that communicates to a Google API.
        Returns:
          A service that is connected to the specified API.
        """            
        if self.verbose:
            sys.stdout.write("Acquiring credentials...\n")
        try:
            credentials = ServiceAccountCredentials.from_json_keyfile_name(filename=keyfile, scopes=scopes)
        except FileNotFoundError as e:
            if self.verbose:
                sys.stderr.write(e.strerror + ":\n")
                sys.stderr.write(keyfile + "\n")
            else:
                logger.error("keyfile %s not found" % keyfile)
            return -1    
    
        # Build the service object for use with any API
        if self.verbose:
            sys.stdout.write("Acquiring service...\n")
        self.service = discovery.build(serviceName="drive", version="v3", credentials=credentials,
                                  cache_discovery=False)
        
        if self.verbose:
            sys.stdout.write("Service acquired!\n")
        return self.service
    
    def setup(self):
        if len(self.settingsfile) > 0:
            try:
                with open(self.settingsfile) as f:
                    self.settings=json.loads(f.read())
            except FileNotFoundError as e:
                if self.verbose:
                    sys.stderr.write(e.strerror + ":\n")
                    sys.stderr.write(self.settingsfile + "\n")
                else:
                    logger.error("Settings %s not found" % self.settingsfile)
                return None
        secretfile = self.settings.get("secretfile")
        try:
            with open(secretfile) as f:
                self.secrets=json.loads(f.read())
        except FileNotFoundError as e:
            if self.verbose:
                sys.stderr.write("Secrets %s not found\n" % secretfile)
                sys.stderr.write(e.strerror + ":\n")
                sys.stderr.write(secretfile + "\n")
            else:
                logger.error("Secrets %s not found" % secretfile)
        keyfile = self.settings.get("keyfile")
        scopes = self.settings.get("scopes")
        logfile = self.settings.get("logfile")
        adminemail = self.settings.get("email")
        testlog = self.settings.get("testlog")
        self.verbose = self.settings.get("verbose", False)
        logging.basicConfig(filename=logfile, 
                            format='%(levelname)s:%(asctime)s %(message)s', 
                            level=logging.INFO)
        logging.getLogger('googleapiclient').setLevel(logging.ERROR)
        logging.getLogger('oauth2client').setLevel(logging.ERROR)
        
        emailSubject = "Database backup to Google Drive Information!!!"
        emailHost = self.secrets.get("EMAIL_HOST")
        emailUser = self.secrets.get("EMAIL_USER")
        emailPort = self.secrets.get("EMAIL_PORT")
        emailUseTLS = self.secrets.get("EMAIL_USE_TLS")
        emailPassword = self.secrets.get("EMAIL_PASS")
        emailFromUser = self.secrets.get("EMAIL_FROM_USER")
        if adminemail == "":
            if self.verbose:
                sys.stdout.write("No admin email specified using --email argument, no email logging enabled.\n")
            else:
                logger.info("No admin email specified using --email argument, no email logging enabled.")
        else:
            isSecure = None
            if emailUseTLS == "True":
                isSecure = ()
            smtpHandler = SMTPHandler((emailHost, emailPort), 
                                      emailFromUser, 
                                      adminemail, 
                                      emailSubject, 
                                      credentials=(emailUser, emailPassword,), 
                                      secure=isSecure)
            smtpHandler.setLevel(logging.ERROR)
            logger.addHandler(smtpHandler)
        if testlog:
            logger.info("Test of logging capabilities for info messages")
            logger.error("Test of logging capabilities for error messages")
        self.get_service(keyfile, scopes)
        return self.service
    
    def create_new_folder(self, name):
        """Will create a new folder in the root of the supplied GDrive, 
        Retruns:
            The folder resource
        """
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        backupdirs = self.list_files_in_drive(namequery="= '%s'" % name, 
                                         mimetypequery="= 'application/vnd.google-apps.folder'")
        if len(backupdirs) == 0:
            folder_metadata = {
            'name' : name,
            'mimeType' : 'application/vnd.google-apps.folder'
            }
            try:
                self.service.files().create(body=folder_metadata, fields='id, name').execute()
            except HttpError as e:
                if self.verbose:
                    sys.stdout.write("unable to create folder %s: %s\n" % (name, str(e)))
                else:
                    logger.error("unable to create folder %s: %s" % (name, str(e)))
                return None
            backupdirs = self.list_files_in_drive(namequery="= '%s'" % name, 
                                             mimetypequery="= 'application/vnd.google-apps.folder'")
            folder = backupdirs[0]
            if self.verbose:
                sys.stdout.write("Folder %s creation complete, ID=%s\n" % (folder.get('name'), folder.get('id')))
            else:
                logger.info("Folder %s creation complete, ID=%s" % (folder.get('name'), folder.get('id')))
        else:
            folder = backupdirs[0]
            if self.verbose:
                sys.stdout.write("Folder %s creation complete, ID=%s\n" % (folder.get('name'), folder.get('id')))
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
            else:
                logger.info("deleted file %s fileid=%s" % (file.get('name'), fileid))
            result = True
        except HttpError as e:
            if self.verbose:
                sys.stdout.write("unable to delete file %s fileid=%s: %s\n" % (file.get('name'), fileid, str(e)))
            else:
                logger.error("unable to delete file %s fileid=%s: %s" % (file.get('name'), fileid, str(e)))
            result = False
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
            else:
                logger.info("Uploaded file %s size= %s ID=%s to: %s" % (file.get('name'), file.get('size'), file.get('id'), folder.get('name')))
        except HttpError as e:
            if self.verbose:
                sys.stdout.write("unable to upload file  %s: %s\n" % (fileName, str(e)))
            else:
                logger.error ("Unable to upload file %s to: %s\n" % (fileName, folderID))
            return None
    
        return file
    
    def download_file_from_folder(self, fileId, fileName):
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
            file = self.service.files().get(fileId=fileId, fields='id,name').execute()
            fileName = file.get("name")
        except HttpError as e:
            if self.verbose:
                sys.stdout.write("unable to access file ID  %s: %s\n" % (fileId, str(e)))
            else:
                logger.error("unable to access file ID  %s: %s\n" % (fileId, str(e)))
            return False
        fh = io.FileIO(fileName, mode='wb')
        downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024)
        done = False
        if self.verbose:
            sys.stdout.write("Downloading file %s, id=%s\n" % (fileName, fileId))
        while done is False:
            status, done = downloader.next_chunk()
            if status:
                if self.verbose:
                    sys.stdout.write("Download %d%%.\n" % int(status.progress() * 100))
        if self.verbose:
            sys.stdout.write("Download Complete!\n")
        return True
    
    def list_files_in_drive(self, query="", parentid=''):
        """Queries Google Drive for all files satisfying name contains string
        Returns:
                list of file resources
        """
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        try:
            if len(query) > 0:
                files= self.service.files().list(q=query).execute()
            else:
                files = self.service.files().list().execute()
        except HttpError as e:
            if self.verbose:
                sys.stdout.write("unable to list files  %s: %s\n" % (query, str(e)))
            else:
                logger.error("unable to list files  %s: %s\n" % (query, str(e)))
            return []
        if self.verbose:
            for file in files.get('files'):
                thisFile = self.service.files().get(fileId=file.get('id'), fields='id,parents,name,size,modifiedTime').execute()
                sys.stdout.write("%s(id='%s', size='%s', modified='%s'\n" % (thisFile.get('name'), 
                                                                             thisFile.get('id'), 
                                                                             thisFile.get('size'), 
                                                                             thisFile.get('modifiedTime')))
        return files.get('files')
    
    def upload_file_to_root(self, fileName=''):
        """Uploads the file to the root directory on the said Google Drive
        Returns:
                fileID, A string of the ID from the uploaded file
        """
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        return self.upload_file_to_folder(folderID=None, fileName=fileName)