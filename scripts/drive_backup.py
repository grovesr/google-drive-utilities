# encoding: utf-8
'''
database_backup.drive_backup -- is a CLI program used to automate backing up gzipped directories to Gogle Drive

@author:     Rob Groves

@copyright:  2018. All rights reserved.

@license:    license

@contact:    robgroves0@gmail.com
@deffield    updated: Updated
'''

import sys
import os
import json
import logging
import glob
import re
sys.path.insert(0, os.path.expanduser("~/git/google-drive-utilities/google_drive_utilities"))
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from google_drive import GoogleDrive
from google_drive import GoogleDriveException
from logging.handlers import SMTPHandler
import time
from datetime import datetime
from subprocess import run, PIPE, CalledProcessError
from pathlib import Path
from hashlib import md5

logger = logging.getLogger(__name__)

__version__ = 0.1
__date__ = '2024-01-01'
__updated__ = '2024-01-01'
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
    emailSubject = "drive_backup.py google_drive API Information!!!"
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
        smtpHandler.setLevel(logging.INFO)
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
  
Requires a json-formatted 'secretfile' containing various database and email information.
Below is a generic example of the contents of '.database_secret.json':

{
    "MYSQLDB1NAME_DB_USER":"db1_username",
    "MYSQLDB1NAME_DB_PASS":"db1_user_password",
    "MYSQLDB2NAME_DB_USER":"db2_username",
    "MYSQLDB2NAME_DB_PASS":"db2_user_password",
    "EMAIL_HOST":"smtp.gmail.com",
    "EMAIL_PORT":"587",
    "EMAIL_USER":"dummy@gmail.com",
    "EMAIL_USE_TLS":"True",
    "EMAIL_PASS":"emailpassword",
    "EMAIL_FROM_USER":"dummy@gmail.com"
    }


requires a json formatted 'keyfile' also containing the Google Drive server-server credentials.
see https://developers.google.com/identity/protocols/OAuth2ServiceAccount for more information

USAGE
''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument(dest="settingsfile", help="settings file containing connection information [default: %(default)s]", default="./settings.json")
        parser.add_argument("-b", "--backupfolder", dest="backupfolder", help="Folder under My Drive to upload the zippped file [default: %(default)s]", default="/Backup")
        parser.add_argument("-k", "--keepfiles", dest="keepfiles", type=int, help="Keep this number of unique files in drive. delete older files if necessary. [default: %(default)s]", default=1)
        parser.add_argument("-d", "--debug", dest="DEBUG", action="store_true", help="print out debuggung info [default: %(default)s]", default=False)
        parser.add_argument("-e", "--excludefolder", dest="excludefolders", action="append", help="exclude this directory from the gzipped directory [default: %(default)s]", default=None)
        parser.add_argument(dest="directories", help="space separated list of directories to zip & upload to drive", nargs='+')

        # Process arguments
        args = parser.parse_args()
        settingsfile = args.settingsfile
        backupfolder = args.backupfolder
        keepfiles = args.keepfiles
        excludefolders = args.excludefolders
        DEBUG = args.DEBUG
        directories = args.directories
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
        excludestring = '_excl_'
        if excludefolders is not None:
            for excludefolder in excludefolders:
                excludestring = excludestring + re.sub('[\*\[\]\-/]', '_', excludefolder)[1:]
        else:
            excludestring = excludestring + "none"
        directories = args.directories
        try:
            gdrive = GoogleDrive(keyfile, tokenfile, scopes, verbose=DEBUG)
            backupfolderpath, backupfolderid, backupfolderfile = gdrive.create_folder_path(backupfolder)
            successful = []
            exists = []
            for directory in directories:
                directory = os.path.expanduser(directory)
                path = Path(directory)
                parentname = str(path.parent.absolute())
                dirname = path.parts[-1]
                if os.path.exists(directory):
                    md5args = ['find',directory]
                    if excludefolders is not None:
                        for excludefolder in excludefolders:
                            md5args.extend(["-not", "-path", "%s/*" % excludefolder])
                    md5args.extend(['-type', 'f'])
                    md5command = ' '.join(md5args)
                    md5command = "%s | sort | xargs -n 1 md5sum | md5sum" % md5command
                    if verbose:
                        sys.stdout.write("Checking md5sum of %s\n" % (directory))
                        sys.stdout.write("using command: %s\n" % md5command)
                    try:
                        p1 = run(md5args, stdout=PIPE, check=True)
                        p2 = run(['sort'], input=p1.stdout, stdout=PIPE, check=True)
                        p3 = run(['xargs', '-n', '1', 'md5sum'], input=p2.stdout, stdout=PIPE, check=True)
                        p4 = run(['md5sum'], input = p3.stdout, stdout=PIPE, check=True)
                        checksum = p4.stdout.strip().decode("utf-8")
                    except CalledProcessError as e:
                        if gdrive.verbose:
                            sys.stdout.write("unable to run md5sum on directory=%s Error='%s'\n" % (directory, e.stderr.decode()))
                        else:
                            logger.error("unable to run md5sum on  directory=%s Error='%s'" % (directory, e.stderr.decode()))
                        continue
                    
                    backuproot =  directory.replace(os.path.sep,'_')[1:] + excludestring.replace(re.sub('[\-/]','_',parentname), '').replace('__','_')
                    utcnow = datetime.utcnow().isoformat()
                    # check to see if this file already exists on Drive, if so check its checksum
                    oldpaths, oldids, oldfiles = gdrive.list_files_in_drive(query="modifiedTime < '%sZ' and name contains '%s'" % (utcnow, backuproot), verbose=DEBUG)
                    fileexists = False
                    for file in oldfiles:
                        # double check that the file is really a match
                        if backuproot in file.get("name"):
                            properties = file.get('properties', None)
                            if properties is not None:
                                oldchecksum = properties.get('checksum', None)
                                if oldchecksum is not None and verbose:
                                    sys.stdout.write("new checksum=%s, drive checksum=%s\n" % (checksum, oldchecksum))
                                    if oldchecksum == checksum:
                                        fileexists = True
                                        exists.append("filename=%s/%s already exists and is identical" % (backupfolder, file.get("name")))
                                        break
                    if not fileexists:
                        backupfile = "%s%s%s.%s.tgz" %('/tmp',os.path.sep, backuproot, datetime.now().isoformat().replace(':', '.'))
                        tarargs = ['tar']
                        if excludefolders is not None:
                            for excludefolder in excludefolders:
                                tarargs.extend(['--exclude', excludefolder.replace(parentname, '')[1:]])
                        # use ustar format to ensure we don't change checksum for changed file attributes that don't change file contents 
                        tarargs.extend(["--format", "ustar", "-czf", backupfile,"--directory", parentname, dirname])
                        tarcommand = ' '.join(tarargs)
                        if verbose:
                            sys.stdout.write("Taring %s to %s\n" % (directory, backupfile))
                            sys.stdout.write("using command: %s\n" % tarcommand)
                        try:
                            run(tarargs, stderr=PIPE, check=True)
                        except CalledProcessError as e:
                            # try again after a 5 second delay
                            time.sleep(5)
                            try:
                                run(tarargs, stderr=PIPE, check=True)
                            except CalledProcessError as e:
                                if gdrive.verbose:
                                    sys.stdout.write("unable to tar directory=%s Error='%s'\n" % (directory, e.stderr.decode()))
                                else:
                                    logger.error("unable to tar directory=%s Error='%s'" % (directory, e.stderr.decode()))
                                continue
                        uploadedpath, uploadedid, uploadedfile = gdrive.upload_file_to_path(filename=backupfile, parentpath=backupfolder, checksum=checksum, verbose=DEBUG) 
                        if uploadedid is not None:
                            successful.append(directory + (" (filename=%s, size=%s)" % (uploadedpath, uploadedfile.get("size"))))
                        for rmfile in glob.glob("%s*" % os.path.join('/tmp', backuproot)):
                            fileToRemove = os.path.join('/tmp', rmfile)
                            try:
                                run(["rm", fileToRemove], stderr=PIPE, check=True)
                                if verbose:
                                    sys.stdout.write("removing %s from filesystem\n" % fileToRemove)
                            except CalledProcessError as e:
                                if verbose:
                                    sys.stdout.write("unable to remove %s, Error='%s'\n" % (fileToRemove, e.stderr.decode()))
                                else:
                                    logger.error("unable to remove %s, Error='%s'" % (fileToRemove, e.stderr.decode()))
                                continue
                        oldfiles.reverse()
                        indx = 1
                        for file in oldfiles:
                            if indx >= keepfiles and backuproot in file.get('name'):
                                # double check the name comparison here
                                gdrive.delete_file_id(fileid=file.get('id'), verbose=DEBUG)
                                if verbose:
                                    pathlist, thispath = gdrive.get_path(file=file, verbose=DEBUG)
                                    sys.stdout.write("removing %s from Google Drive\n" % thispath)
                            if backuproot in file.get('name'):
                                indx = indx + 1
                else:
                    if verbose:
                        sys.stdout.write("directory %s doesn't exist. Ignoring\n" % directory)
                    else:
                        logger.info("directory %s doesn't exist. Ignoring" % directory)
            if verbose:
                if len(successful) > 0:
                    sys.stdout.write("Uploaded the following directories to Google Drive: %s\n" % str(successful))
                if len(exists) > 0:
                    sys.stdout.write("The following files already exist on Google Drive: %s\n" % str(exists))
            else:
                if len(successful) > 0:
                    logger.info("Uploaded the following directories to Google Drive: %s" % str(successful))
                if len(exists) > 0:
                    logger.info("The following files already exist on Google Drive: %s" % str(exists))
            return 0
        except GoogleDriveException as e:
            msg = "Problem accessing Google Drive API: %s" % str(e)
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
        return 2


if __name__ == "__main__":
    if TESTRUN:
        import doctest
        doctest.testmod()
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