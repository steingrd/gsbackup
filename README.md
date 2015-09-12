gsbackup
========

`gsbackup` is a simple Python script that lets you incrementally upload a local directory to Google Storage.

Prerequisities
--------------

To use `gsbackup` you need a Google Developer account with a project with a Google Storage bucket. You will also need a service account credential that has access to this bucket and the client id and secret for this account.

Usage
-----

    $ ./gsbackup.py 
    gsbackup.py --config FILE COMMAND

    Commands:
        --initial             Create initial database by scanning source directory
        --refresh             Refresh database by scanning source directory       
        --upload              Upload to Google Storage, abort with Ctrl-C         
        --list-not-uploaded   List files that have not been uploaded

