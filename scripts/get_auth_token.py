# encoding: utf-8
'''
get_auth_token -- is a CLI program that is used to trade a Google Auth secret for a token

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
from pathlib import Path
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from subprocess import run, PIPE, CalledProcessError, DEVNULL
from logging.handlers import SMTPHandler

__version__ = 0.1
__date__ = '2024-01-01'
__updated__ = '2024-01-01'
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
    emailSubject = "get_auth_token.py google_drive API access problem!!!"
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

example call: python3 get_auth_token.py settings.json
''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument(dest="settingsfile", help="settings file containing connection information [default: %(default)s]", default="./settings.json")

        # Process arguments
        args = parser.parse_args()
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
        ftpbatch = "%s/%s" %(privatedir, 'ftpbatch.txt')
        logger = setup_logging(settings)
        try:
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
                    if verbose:
                        sys.stderr.write(msg + "\n")
                    raise GoogleDriveException(msg)
            with tokenpath.open(mode="w") as f:
                f.write(credentials.to_json())
            tokenpath.chmod(0o644)   
            sftpargs = ['sftp', '-P', '7822', 'robsapps@robsapps.a2hosted.com']
            try:
                with open(ftpbatch, 'r') as batchfile:
                    run(sftpargs, 
                        stdin=batchfile, stdout=DEVNULL, stderr=PIPE, check=True)
            except CalledProcessError as e:
                raise (e)
        except GoogleDriveException as e:
            if verbose:
                msg = ("Unable to generate access token: %s" % str(e))
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
