#!/home/grovesr/.virtualenvs/google_drive/bin/python3
# encoding: utf-8
'''
gdrive_list -- is a CLI program that is used to list the contents of Google Drive service accounts

@author:     Rob Groves

@copyright:  2018. All rights reserved.

@license:    license

@contact:    robgroves0@gmail.com
@deffield    updated: Updated
'''
import sys
import os
sys.path.insert(0, "/home/grovesr/git/google-drive-utilities/google_drive_utilities")
sys.path.insert(0, "/home/grovesr/git/database-backup/database_backup")
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from google_drive import GoogleDrive

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
        parser.add_argument("--delete", dest="delete", action='store_true', help="delete listed files from Google Drive [default: %(default)s]", default = False)
        parser.add_argument("--download", dest="download", action='store_true', help="download listed files from Google Drive [default: %(default)s]", default = False)
        parser.add_argument(dest="settings", help="settings file containing connection information [default: %(default)s]", default="./settings.json")

        # Process arguments
        args = parser.parse_args()
        gdrive = GoogleDrive(args.settings)
        if gdrive.service is not None:
            if args.query is not None:
                files = gdrive.list_files_in_drive(query=args.query)
            else:
                files = gdrive.list_files_in_drive()
            if args.download:
                for file in files:
                     gdrive.download_file(file.get("id"), file.get("name"))
            if args.delete:
                for file in files:
                     gdrive.delete_file(file.get("id"))
        else:
            sys.stderr.write("GoogleDrive not successfully initialized\n")
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
