# encoding: utf-8
'''
gdrive_helper -- is a CLI program that is used to access Google Drive service accounts and perform operations

@author:     Rob Groves

@copyright:  2023. All rights reserved.

@license:    license

@contact:    robgroves0@gmail.com
@deffield    updated: Updated
'''
import sys
import os
import json
import logging
sys.path.insert(0, os.path.expanduser("~/git/google-drive-utilities/google_drive_utilities"))
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from google_drive import GoogleDrive
from google_drive import GoogleDriveException
from logging.handlers import SMTPHandler

__version__ = 0.1
__date__ = '2024-01-01'
__updated__ = '2024-01-01'
DEBUG = 0
TESTRUN = 0
PROFILE = 0

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg
    
class ImproperlyConfigured(Exception):
    '''Generic exception to raise and log configuration errors.'''
    def __init__(self, msg):
        super(ImproperlyConfigured).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

def get_secret(secrets={}, setting=''):
    """
    get the secret setting or return explicit exception
    """
    try:
        return secrets[setting]
    except KeyError:
        error_msg = "Set the {0} environment variable in the secret file".format(setting)
        logger.error(error_msg)
        raise ImproperlyConfigured(error_msg)

def setup_logging(settings):
    
    logfile = get_secret(settings, "logfile")
    adminemail = get_secret(settings, "email")
    testlog = get_secret(settings, "testlog")
    verbose = get_secret(settings, "verbose")
    privatedir = get_secret(settings, "privatedir")
    logging.basicConfig(filename=logfile,
                        format='%(levelname)s - %(asctime)s - %(filename)s - %(message)s',
                        level=logging.INFO)
    logger = logging.getLogger(__name__)
    logging.getLogger('googleapiclient').setLevel(logging.ERROR)
    logging.getLogger('google').setLevel(logging.ERROR)
    logging.getLogger('google_auth_oauthlib').setLevel(logging.ERROR)

    database_secretfile = privatedir + "/" + get_secret(settings,"database_secretfile")
    try:
        with open(database_secretfile) as f:
            secrets=json.loads(f.read())
    except FileNotFoundError as e:
        if verbose:
            sys.stderr.write("Secrets %s not found\n" % database_secretfile)
            sys.stderr.write(e.strerror + ":\n")
            sys.stderr.write(database_secretfile + "\n")
        else:
            logger.error("Secrets %s not found" % database_secretfile)
    emailSubject = "gdrive_helper.py google_drive API access problem!!!"
    emailHost = get_secret(secrets, "EMAIL_HOST")
    emailUser = get_secret(secrets, "EMAIL_USER")
    emailPort = get_secret(secrets, "EMAIL_PORT")
    emailUseTLS = get_secret(secrets, "EMAIL_USE_TLS")
    emailPassword = get_secret(secrets, "EMAIL_PASS")
    emailFromUser = get_secret(secrets, "EMAIL_FROM_USER")
    if adminemail == "":
        if verbose:
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
    return logger

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created on %s.
  Copyright 2018. All rights reserved.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE

example call: python3 gdrive_helper.py settings.json -q="name contains 'Getting'"
''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument(dest="settingsfile", help="settings file containing connection information [default: %(default)s]", default="./settings.json")
        parser.add_argument("-q", "--query", dest="query", help="query to use when listing Google Drive [default: %(default)s]", default = None)
        parser.add_argument("--filterfilepath", dest="filterfilepath", help="use regex to filter files in a folder path from Google Drive [default: %(default)s]", default = None)
        parser.add_argument("--downloadfiles", dest="downloadfiles", action='store_true', help="download specified in the query or querypath from Google Drive [default: %(default)s]", default = False)
        parser.add_argument("--deletefilepath", dest="deletefilepath", help="delete queried file (includes path to file) from Google Drive [default: %(default)s]", default = None)
        parser.add_argument("--deletefileid", dest="deletefileid", help="delete queried file id from Google Drive [default: %(default)s]", default = None)
        parser.add_argument("--createfolderpath", dest="createfolderpath", help="create a folder path in Google Drive.", default = None)
        parser.add_argument("--uploadfile", dest="uploadfile", help="upload file in Google Drive under parentpath if supplied else under root [default: %(default)s]", default = None)
        parser.add_argument("--parentpath", dest="parentpath", help="parent directory path to use when creating file or directory [default: %(default)s]", default = "/")
        parser.add_argument("--allowduplicate", dest="allowduplicate", action='store_true', help="upload duplicate file if it already exists [default: %(default)s]", default = False)

        # Process arguments
        args = parser.parse_args()
        query = args.query
        deletefilepath = args.deletefilepath
        filterfilepath = args.filterfilepath
        deletefileid = args.deletefileid
        downloadfiles = args.downloadfiles
        createfolderpath = args.createfolderpath
        uploadfile = args.uploadfile
        parentpath = args.parentpath
        allowduplicate = args.allowduplicate
        settingsfile = args.settingsfile
        settings = {}
        
        if len(settingsfile) > 0:
            try:
                with open(settingsfile) as f:
                    settings=json.loads(f.read())
            except FileNotFoundError as e:
                sys.stderr.write(str(e) + ":\n")
                sys.stderr.write(settingsfile + "\n")
                return 2
        privatedir = get_secret(settings, "privatedir")
        keyfile = "%s/%s" % (privatedir, get_secret(settings, "google_keyfile"))
        tokenfile = "%s/%s" % (privatedir, get_secret(settings, "google_tokenfile"))
        scopes = get_secret(settings, "scopes")
        verbose = get_secret(settings, "verbose")
        logger = setup_logging(settings)
        if filterfilepath is not None and query is not None:
            msg = "You can't supply both a query and a filterfilepath at the same time"
            if verbose:
                sys.stderr.write("%s\n" % msg)
            else:
                logger.error(msg)
            return 2
        if downloadfiles and query is None and filterfilepath is None:
            msg = "You need to supply a query or filterfilepath in order to download files"
            if verbose:
                sys.stderr.write("%s\n" % msg)
            else:
                logger.error(msg)
            return 2
        if deletefilepath is not None and (query is not None or filterfilepath is not None):
            msg = "To delete a file do not include a query or filterfilpath, just inlcude the file with its path"
            if verbose:
                sys.stderr.write("%s\n" % msg)
            else:
                logger.error(msg)
            return 2
        if deletefilepath is not None and deletefileid is not None:
            msg = "You can't delete a filepath and a fileid at the same time, choose one or the other"
            if verbose:
                sys.stderr.write("%s\n" % msg)
            else:
                logger.error(msg)
            return 2
        try:
            gdrive = GoogleDrive(keyfile, tokenfile, scopes, verbose=DEBUG)
            paths = []
            ids = []
            files = []
            if deletefilepath is None and deletefileid is None and createfolderpath is None and uploadfile is None:
                # get the files from a query if you need them
                if query is not None:
                    # allow general queries to retrieve trashed files
                    paths, ids, files = gdrive.list_files_in_drive(query=query, includetrashed=True, verbose=DEBUG)
                if filterfilepath is not None:
                    paths, ids, files = gdrive.filter_filepath_in_drive(pathquery=filterfilepath, includetrashed=False, verbose=DEBUG)
                if not downloadfiles:
                    # list files if verbose
                    if verbose:
                        for indx in range(len(files)):
                            dirslash = ''
                            if files[indx].get('mimeType') == 'application/vnd.google-apps.folder':
                                dirslash = '/'
                            sys.stdout.write("%s%s (id=%s, size='%s', modified='%s')\n" % (paths[indx],
                                                                                    dirslash,
                                                                                  files[indx].get('id'),
                                                                                  files[indx].get('size'),
                                                                                  files[indx].get('modifiedTime')))
            if downloadfiles and len(ids) > 0:
                downloadpaths = []
                downloadids = []
                downloadfiles = []
                for indx in range(len(ids)):
                    try:
                        path, id, file = gdrive.download_file(ids[indx], paths[indx].split('/')[-1], verbose=DEBUG)
                        downloadpaths.append(path)
                        downloadids.append(id)
                        downloadfiles.append(file)
                        msg = "downloaded file %s" % file
                        if verbose:
                            sys.stdout.write("%s\n" % msg)
                        else:
                            logger.info(msg)
                    except GoogleDriveException as e:
                        msg = str(e)
                        if verbose:
                            sys.stderr.write("%s\n" % msg)
                        else:
                            logger.error(msg)
                        pass
                paths = downloadpaths
                ids = downloadids
                files = downloadfiles
            if deletefilepath is not None:
                try:
                    path, id, file = gdrive.delete_file_path(path=deletefilepath, trash=True, verbose=DEBUG)
                    paths.append(path)
                    ids.append(id)
                    files.append(file)
                    msg = "deleted file %s" % path
                    if verbose:
                        sys.stdout.write("%s\n" % msg)
                    else:
                        logger.info(msg)
                except GoogleDriveException as e:
                    msg = str(e)
                    if verbose:
                        sys.stderr.write("%s\n" % msg)
                    else:
                        logger.error(msg)
                    return 2
            if deletefileid is not None:
                try:
                    path, id, file = gdrive.delete_file_id(fileid=deletefileid, trash=True, verbose=DEBUG)
                    paths.append(path)
                    ids.append(id)
                    files.append(file)
                    msg = "deleted file %s" % path
                    if verbose:
                        sys.stdout.write("%s\n" % msg)
                    else:
                        logger.info(msg)
                except GoogleDriveException as e:
                    msg = str(e)
                    if verbose:
                        sys.stderr.write("%s\n" % msg)
                    else:
                        logger.error(msg)
                    return 2
            if createfolderpath is not None:
                try:
                    path, id, file = gdrive.create_folder_path(createfolderpath, verbose=DEBUG)
                    paths.append(path)
                    ids.append(id)
                    files.append(file)
                    msg = "created folder path %s" % path
                    if verbose:
                        sys.stdout.write("%s\n" % msg)
                    else:
                        logger.info(msg)
                except GoogleDriveException as e:
                    msg = str(e)
                    if verbose:
                        sys.stderr.write("%s\n" % msg)
                    else:
                        logger.error(msg)
                    return 2
            if uploadfile:
                try:
                    path, id, file = gdrive.upload_file_to_path(uploadfile, parentpath, verbose=DEBUG, allowduplicate= allowduplicate)
                    paths.append(path)
                    ids.append(id)
                    files.append(file)
                    msg = "uploaded file %s to %s (id='%s')" % (uploadfile, path, id)
                    if verbose:
                        sys.stdout.write("%s\n" % msg)
                    else:
                        logger.info(msg)
                except GoogleDriveException as e:
                    msg = str(e)
                    if verbose:
                        sys.stderr.write("%s\n" % msg)
                    else:
                        logger.error(msg)
                    return 2
        except GoogleDriveException as e:
            msg = ("Problem accessing Google Drive API: %s" % str(e))
            if verbose:
                sys.stderr.write("%s\n" % msg)
            else:
                logger.error(msg)
            return 2

    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception as e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help\n")
    return 0
if __name__ == "__main__":
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = 'gdrive_helper_profile.txt'
        cProfile.run('main()', profile_filename)
        #statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename)
        stats = p.strip_dirs().sort_stats(pstats.SortKey.CUMULATIVE)
        stats.print_stats()
        stats.dump_stats("gdrive_helper_profile_stats.txt")
        #statsfile.close()
        sys.exit(0)
    sys.exit(main())
