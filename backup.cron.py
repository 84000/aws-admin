#! /usr/bin/python
from subprocess import call
from operator import itemgetter, attrgetter
import glob, os, smtplib
notify=""

def main():
    #no message to email by default
    global notify

    check_backup_log()
 
    try: 
        sync_s3()
    except:
        notify+="S3 sync failed\n"
    else:
        prune_old_backups(5)

    #send email if there's anything to notify
    if (notify):
        email_notify(notify)

def sort_files_by_last_modified(files):
    """ Given a list of files, return them sorted by the last
         modified times. """
    fileData = {}
    for fname in files:
        fileData[fname] = os.stat(fname).st_mtime

    fileData = sorted(fileData.items(), key = itemgetter(1))
    return fileData 

def delete_oldest_files(sorted_files, keep = 3):
    """ Given a list of files sorted by last modified time and a number to 
        keep, delete the oldest ones. """
    delete = len(sorted_files) - keep
    for x in range(0, delete):
        print "Deleting: " + sorted_files[x][0]
        os.remove(sorted_files[x][0])

# sync backups to s3
def sync_s3():
    copybackups = call (['/usr/bin/aws', 's3', 'sync', '/home/existdb/exist-backup', 's3://us-east-1-84000.co-backup/84000-translate.org/eXist-backup'])
    copyresources = call (['/usr/bin/aws', 's3', 'sync', '/var/www/html/translator-resources', 's3://us-east-1-84000.co-backup/84000-translate.org/translator-resources'])
    """non-zero above means there was an error"""
    """return true if success, false if error"""
    return not(bool(copybackups or copyresources))


# keep X number of backup and log files, delete others
def prune_old_backups(keep):
    # Find all backup files matching the path.
    file_paths = glob.glob('/home/existdb/exist-backup/full*.zip')

    # Sort the files according to the last modification time.
    sorted_files = sort_files_by_last_modified(file_paths)

    delete_oldest_files(sorted_files, keep)


    # Find all log files matching the path.
    file_paths = glob.glob('/home/existdb/exist-backup/report*.log')

    # Sort the files according to the last modification time.
    sorted_files = sort_files_by_last_modified(file_paths)

    delete_oldest_files(sorted_files, keep)

def check_backup_log():
    global notify
    # Find and sort log files
    file_paths = glob.glob('/home/existdb/exist-backup/report*.log')
    sorted_files = sort_files_by_last_modified(file_paths)

    #check the newest log for errors
    newest=(len(sorted_files))-1
    logname = sorted_files[newest][0]
    greplog = not (call (['grep', '-i', 'fail', logname]))
    if greplog:
        notify += "There is an error reported in log file "+logname+"\n"   


#email message containing notification text
def email_notify(notification):
    sender = 'dave@scheuneman.com'
    receivers = ['dave@scheuneman.com','dominic.latham@84000.co']

    message = "From: dave@scheuneman.com\nTo: dave@scheuneman.com,dominic.latham@84000.co\nSubject: 84000 collab-server cron message\n"
    message += "backup.cron.py on collaboration generated this message:\n"+notification+"\n"

    smtpObj = smtplib.SMTP('localhost')
    smtpObj.sendmail(sender, receivers, message)

#Run Me
main()
