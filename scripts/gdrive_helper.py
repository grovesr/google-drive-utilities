# encoding: utf-8
'''
gdrive_list -- is a CLI program that is used to access Google Drive service accounts and perform operations

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

logger = logging.getLogger(__name__)

__version__ = 0.1
__date__ = '2019-01-01'
__updated__ = '2019-01-01'
DEBUG = 0
TESTRUN = 0

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
                        format='%(levelname)s:%(asctime)s %(message)s',
                        level=logging.INFO)
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
            logger.error("Secrets %s not found" % secretfile)
    emailSubject = "Database backup to Google Drive Information!!!"
    emailHost = get_secret(secrets, "EMAIL_HOST")
    emailUser = get_secret(secrets, "EMAIL_USER")
    emailPort = get_secret(secrets, "EMAIL_PORT")
    emailUseTLS = get_secret(secrets, "EMAIL_USE_TLS")
    emailPassword = get_secret(secrets, "EMAIL_PASS")
    emailFromUser = get_secret(secrets, "EMAIL_FROM_USER")
    if adminemail == "":
        if sverbose:
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

example call: gdrive_list.py settings.json -q="name contains 'Getting'"
''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-q", "--query", dest="query", help="query to use when listing Google Drive [default: %(default)s]", default = None)
        parser.add_argument("-p", "--pathquery", dest="pathquery", help="path query to use when listing Google Drive [default: %(default)s]", default = None)
        parser.add_argument("--deletefile", dest="deletefile", action='store_true', help="delete queried file from Google Drive [default: %(default)s]", default = False)
        parser.add_argument("--downloadfiles", dest="downloadfiles", action='store_true', help="download listed files from Google Drive [default: %(default)s]", default = False)
        parser.add_argument("--createdir", dest="createdir", help="create a directory listed in Google Drive under parentid if supplied else under root[default: %(default)s]", default = None)
        parser.add_argument("--uploadfile", dest="uploadfile", help="upload file in Google Drive under parentid if supplied else under root [default: %(default)s]", default = None)
        parser.add_argument("--parentid", dest="parentid", help="parent directory to use when creating file or directory [default: %(default)s]", default = "root")
        parser.add_argument(dest="settingsfile", help="settings file containing connection information [default: %(default)s]", default="./settings.json")

        # Process arguments
        args = parser.parse_args()
        query = args.query
        pathquery = args.pathquery
        deletefile = args.deletefile
        downloadfiles = args.downloadfiles
        createdir = args.createdir
        uploadfile = args.uploadfile
        parentid = args.parentid
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
        setup_logging(settings)
        if pathquery is not None and query is not None:
            msg = "You can't supply both a query and a pathquery at the same time"
            if verbose:
                sys.stderr.write("$s\n" % msg)
            else:
                logger.error(msg)
            return 2
        if downloadfiles and query is None and pathquery is None:
            msg = "You need to supply a query or pathquery in order to download files"
            if verbose:
                sys.stderr.write("\n")
            else:
                logger.error(msg)
            return 2
        if deletefile and query is None:
            msg = "You need to supply a query that returns a single file in order to delete it"
            if verbose:
                sys.stderr.write("\n")
            else:
                logger.error(msg)
            return 2
        try:
            gdrive = GoogleDrive(keyfile, tokenfile, scopes, verbose)
            # get the files from a query if you need them
            if pathquery is not None:
                paths, ids = gdrive.list_files_in_drive(pathquery=pathquery, includetrashed=False)
            if query is not None:
                paths, ids = gdrive.list_files_in_drive(query=pathquery, includetrashed=False)
            matchingids = []
            if pathquery and pathquery in paths:
                for indx, path in enumerate(paths):
                    if path == pathquery:
                        matchingids.append(indx)
            if downloadfiles and query is not None or pathquery is not None:
                for indx in range(len(matchingids)):
                    try:
                        gdrive.download_file(ids[indx], paths[indx].split('/')[-1])
                    except GoogleDriveException as e:
                        msg = str(e)
                        if verbose:
                            sys.stderr.write("%s\n" % msg)
                        else:
                            logger.error(msg)
                        pass
            if deletefile and len(ids) > 1:
                if verbose:
                    msg = "You need to supply a query that returns a single file in order to delete it.  Your query returned %d files" % len(files)
                    sys.stderr.write("%s\n" % msg)
                else:
                    logger.error(msg)
                return 2
            if deletefile and len(ids) == 1:
                gdrive.delete_file(ids[0])
            if createdir is not None:
                try:
                    gdrive.create_new_folder(createdir, parentid)
                except GoogleDriveException as e:
                    msg = str(e)
                    if verbose:
                        sys.stderr.write("%s\n" % msg)
                    else:
                        logger.error(msg)
                    return 2
            if uploadfile:
                gdrive.upload_file_to_folder(parentid, uploadfile)
            
        except GoogleDriveException as e:
            if verbose:
                msg = ("GoogleDrive not successfully initialized: %s" % str(e))
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
        return 2
    return 0
if __name__ == "__main__":
    sys.exit(main())
