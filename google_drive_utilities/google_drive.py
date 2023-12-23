'''
Created on Dec 31, 2018

@author: grovesr
'''
from __future__ import print_function
import sys
import os
import io
import re
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
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
        self.get_service(self.keyfile, self.tokenfile, self.scopes, verbose=self.verbose)
        self.root = self.get_root(verbose=self.verbose)
        return self.service
    
    def get_service(self, keyfile, tokenfile, scopes, verbose=False):
        """Get a service that commelse:
                    flow = InstalledAppFlow.from_client_secrets_file(
                            keyfile, scopes
                            )
                    credentials = flow.run_local_server(port=0)
                    # Save theunicates to a Google API.
        Returns:
          A service that is connected to the specified API.
        """
        if verbose:
            sys.stdout.write("Acquiring credentials...\n")
        try:
            credentials = None
            if os.path.exists(tokenfile):
                credentials = Credentials.from_authorized_user_file(tokenfile, scopes)
            # If there are no (valid) credentials available, let the user log in.
            if not credentials or not credentials.valid:
                if credentials and credentials.expired and credentials.refresh_token:
                    try:
                        credentials.refresh(Request())
                        if verbose:
                            sys.stdout.write("Credentials refreshed!\n")
                    except RefreshError as e:
                        msg = "Unable to refresh token '%s'\n run get_auth_token.pl script to generate one" % e.args[0]
                        if verbose:
                            sys.stderr(msg + "\n")
                        raise GoogleDriveException(msg)
        except Exception as e:
            msg = "problem getting credentials %s" % str(e)
            if verbose:
                sys.stderr.write(msg + "\n")
            raise GoogleDriveException(msg)

        # Build the service object for use with any API
        if verbose:
            sys.stdout.write("Acquiring service...\n")
        self.service = build(serviceName="drive", version="v3", credentials=credentials,
                                  cache_discovery=False)

        if verbose:
            sys.stdout.write("Service acquired!\n")
        return self.service

    def get_root(self, verbose=False):
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        try:
            rootdir = self.service.files().get(fileId='root').execute()
        except HttpError:
            msg = "unable to determine root directory"
            if verbose:
                sys.stdout.write("%s\n" % msg)
            raise GoogleDriveException(msg)
        return rootdir

    def create_folder_path(self, path=None, verbose=False):
        """Will create a new folderpath, including all missing folders
        Retruns:
            path string, id, and folder
        """
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        if path is None:
            raise GoogleDriveException("You need to specify a path in order to create it")
        pathlist = path.split('/')
        builtpath = ''
        # follow the path down from root and create directories as needed
        for dirname in pathlist:
            if len(dirname) == 0:
                continue
            parentpath = builtpath
            builtpath = builtpath + '/' + dirname
            existingpath = ''
            existingpaths, existingids, existingdirs = self.list_files_in_drive(pathquery=builtpath, verbose=False)
            for indx, existingpath in enumerate(existingpaths):
                isdir = self.service.files().get(fileId=existingids[indx], fields='id,name,mimeType').execute().get("mimeType") == 'application/vnd.google-apps.folder'
                if existingpath == builtpath and isdir:
                    # the path directory exists already
                    if verbose:
                        sys.stdout.write("%s already exists\n" % builtpath)
                    break
                if existingpath == builtpath and not isdir:
                    raise GoogleDriveException("The path [%s] contains a component that is currently a file and we would have to overwrite it as a directory" % builtpath)
                # otherwise create the folder
            if existingpath != builtpath:
                existingpaths, existingids, existingdirs = self.list_files_in_drive(pathquery=parentpath, verbose=False)
                if len(existingids) == 0:
                    raise GoogleDriveException("Unable to find parent id for [%s]. unable to create the folder path [%s]" % (parentpath, builtpath))
                if len(existingids) > 1:
                    raise GoogleDriveException("Unable to find unique parent id for [%s]. unable to create the folder path [%s]" % (parentpath, builtpath))
                folder_metadata = {
                'name' : dirname,
                'mimeType' : 'application/vnd.google-apps.folder',
                'parents'  : [existingids[0]],
                }
                try:
                    folder = self.service.files().create(body=folder_metadata, fields='id, name').execute()
                except HttpError as e:
                    msg = "[%s] unable to create folder path %s" % (builtpath, e.reason)
                    raise GoogleDriveException(msg)
        return path, folder.get("id"), folder

    def delete_file_path(self, path=None, trash=True, verbose=False):
        """Will delete the given file on the supplied GDrive,
        Retruns:
            True if sucessful
        """
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        try:
            pathlist, fileids, files = self.list_files_in_drive(pathquery=path, verbose=verbose)
            if len(fileids) > 1:
                raise GoogleDriveException("more than one file found (%s). You can only delete file paths that resolve to a single file" % path)
            if len(fileids) == 0:
                msg = "unable to find filepath %s to delete" % path
                raise GoogleDriveException(msg)
            if trash:
                body = {'trashed' : True }
                file = self.service.files().update(fileId=fileids[0], body=body).execute()
            else:
                file = self.service.files().get(fileId=fileids[0], fields='name').execute()
        except HttpError as e:
            msg = "unable to delete filepath %s. %s" % (path, e.reason)
            raise GoogleDriveException(msg)
        return pathlist[0], fileids[0], files[0]
    
    def delete_file_id(self, fileid=None, trash=True, verbose=False):
        """Will delete the given fileid on the supplied GDrive,
        Retruns:
            True if sucessful
        """
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        try:
            if trash:
                body = {'trashed' : True }
                file = self.service.files().update(fileId=fileid, body=body).execute()
            else:
                file = self.service.files().get(fileId=fileid, fields='name').execute()
            if file is None:
                msg = "unable to find fileid=%s to delete" % fileid
                raise GoogleDriveException(msg)
            ids, path = self.get_path(fileid=fileid)
            self.service.files().delete(fileId=fileid).execute()
        except HttpError as e:
            msg = "unable to delete fileid=%s: %s" % (fileid, e.reason)
            raise GoogleDriveException(msg)
        return path, fileid, file

    def upload_file_to_path(self, filename='', parentpath='', verbose=False, allowduplicate=False):
        """Uploads the file to the specified folder id on the said Google Drive
        Returns:
                file resource
        """
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        # get parentpath ids
        if parentpath != '/':
            pathlist, parentids, parents = self.list_files_in_drive(pathquery=parentpath)
            if len(parentids) == 0:
                raise GoogleDriveException("Unable to find path %s" % parentpath)
            if len(parentids) > 1:
                raise GoogleDriveException("There are %d folders at the path '%s' we don't know where to upload the file." % (len(parentids), parentpath))
        else:
            parentpath = ''
            rootdir = self.get_root(verbose)
            parentids = [rootdir.get('id')]
        existingpaths, existingfiles, files = self.list_files_in_drive(pathquery="%s/%s" %(parentpath, filename))
        if len(existingfiles) > 0 and not allowduplicate:
            msg = "file %s/%s already exists" % (parentpath, filename)
            raise GoogleDriveException(msg)
        file_metadata = {
              'name' : os.path.basename(filename),
              'parents': parentids
        }
        try:
            media = MediaFileUpload(filename, resumable=True)
        except FileNotFoundError :
            msg="Unable to find file '%s' to upload" % filename
            if verbose:
                sys.stdout.write("%s\n" % msg)
            raise GoogleDriveException(msg)
                
        try:
            file = self.service.files().create(body=file_metadata, media_body=media, fields='name,id,size,parents').execute()
        except HttpError as e:
            msg = "unable to upload file %s: %s" % (filename, e.reason)
            if verbose:
                sys.stdout.write("%s\n" % msg)
            raise GoogleDriveException(msg)

        return parentpath + '/' + file_metadata['name'], file.get('id'), file

    def download_file(self, fileid, fileName, verbose=False):
        """Downloads the fileId file
        Returns:
                media object
        """
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        if fileid is None or fileName is None:
            return None

        request = self.service.files().get_media(fileId=fileid)
        try:
            file = self.service.files().get(fileId=fileid, fields='id,name').execute()
            fileName = file.get("name")
        except HttpError as e:
            msg = "unable to access file ID %s %s" % (fileid, e.reason)
            if verbose:
                sys.stdout.write("%s\n" % msg)
            raise GoogleDriveException(msg)
        indx = 1
        name, ext = os.path.splitext(fileName)
        newname = name
        while os.path.isfile(newname + ext):
            newname = "%s(%d)" % (name, indx)
            indx = indx +1
        fh = io.FileIO(newname + ext, mode='wb')
        downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024)
        done = False
        if verbose:
            sys.stdout.write("Downloading file %s, id=%s\n" % (newname + ext, fileid))
        while done is False:
            try:
                status, done = downloader.next_chunk()
            except HttpError as e:
                msg = "failed to download file %s: %s" %(fh.name, e.reason)
                fh.close()
                os.remove(fh.name)
                raise GoogleDriveException(msg)
            if status:
                if verbose:
                    sys.stdout.write("Download %d%%.\n" % int(status.progress() * 100))
        if verbose:
            sys.stdout.write("Download Complete!\n")
        return newname + ext, fileid, file

    def list_files_in_drive(self, query=None, pathquery=None, fields="files(id,name,size,modifiedTime)", includetrashed=False, verbose=False):
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
                files= self.service.files().list(q=query, fields=fields).execute()
        except HttpError as e:
            msg = "unable to list files from query '%s': %s" %(query, e.reason)
            if verbose:
                sys.stdout.write("s\n" % msg)
            raise GoogleDriveException(msg)
        fileobjects = files.get('files')
        files = []
        paths = []
        ids = []
        for file in fileobjects:
            pathlist, pathstring = self.get_path(file.get('id'))
            if pathquery is None or pathquery == pathstring: 
                ids.append(pathlist[-1])
                paths.append(pathstring)
                files.append(file)
        return paths, ids, files
    
    def filter_filepath_in_drive(self, pathquery=None, fields="files(id,name,size,modifiedTime)", includetrashed=False, verbose=False):
        """Queries Google Drive for all files satisfying query
        Returns:
                list of file resources
        """
        if pathquery is None:
            raise GoogleDriveException("You must specify a pathquery")
        if self.service is None:
            raise GoogleDriveException("GoogleDrive object not initialized yet")
        m=re.match("(^/.*?)/?([^/]*?$)",pathquery)
        if m.groups() is None:
            raise GoogleDriveException("unable to parse the filepath [%s] with regex %s" % pathquery)
        parentpath = m.groups()[0]
        wildcard = m.groups()[1]
        if len(wildcard) == 0:
            wildcard = '.*'
        if parentpath == '/':
            try:
                query = "'root' in parents"
                if not includetrashed:
                    query = query + " and not trashed"
                files= self.service.files().list(q=query, fields=fields).execute()
            except HttpError as e:
                msg = "unable to list files from query '%s': %s" %(query, e.reason)
                if verbose:
                    sys.stdout.write("s\n" % msg)
                raise GoogleDriveException(msg)
        else:
            parentpaths, parentids, parents = self.list_files_in_drive(pathquery=parentpath, fields=fields)
            if len(parentids) > 1:
                raise GoogleDriveException("Found more than one parent path, parent path must reslolve to a single folder")
            if len(parentids) == 0:
                raise GoogleDriveException("unable to find the parent path %s" % parentpath)
            parentid = parentids[0]
            query = "'%s' in parents" % parentid
            if not includetrashed:
                query = query + " and not trashed"
            try:
                if len(query) > 0:
                    files= self.service.files().list(q=query, fields=fields).execute()
            except HttpError as e:
                msg = "unable to list files from query '%s': %s" %(query, e.reason)
                if verbose:
                    sys.stdout.write("s\n" % msg)
                raise GoogleDriveException(msg)
        fileobjects = files.get('files')
        files = []
        paths = []
        ids = []
        for file in fileobjects:
            pathlist, pathstring = self.get_path(file.get('id'))
            if parentpath == '/':
                sep = ''
            else:
                sep = '/'
            if re.match(parentpath + sep + wildcard,pathstring): 
                ids.append(pathlist[-1])
                paths.append(pathstring)
                files.append(file)
        return paths, ids, files

    def get_path(self, fileid=None, verbose=False):
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
            if verbose:
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
