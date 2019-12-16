#!/usr/bin/python
import sys
import smtplib
import argparse
from email.mime.text import MIMEText

import Onapp

arp = argparse.ArgumentParser(prog='UserGroupBilling', description='Generate billing data in CSV for all VMs in a user group');
garp= arp.add_mutually_exclusive_group();
garp.add_argument("-g", "--group", help="User group ID", default=0);
garp.add_argument("-u", "--user", help="User ID", default=0);
arp.add_argument("-m", "--month", help="Billing month, default previous", default=0);
arp.add_argument("-e", "--email", help="Target email address", default='kallen@ccsius.com')
arp.add_argument("-l", "--list", help="List users and groups", action="store_true");
args = arp.parse_args();

GROUP_ID=int(args.group)
USER_ID=int(args.user)
MONTH=int(args.month)
EMAIL=args.email
LIST_ONLY=args.list

if LIST_ONLY:
    print("User Groups: ")
    for group in Onapp.Job('ListUserGroups').run():
        print("{:>3}. {}".format(group['id'], group['label']))
    print("\nUsers: ")
    for user in Onapp.Job('ListUsers').run():
        print("{:>3}. {}".format(user['id'], user['login']))
    quit();

if GROUP_ID == 0 and USER_ID == 0:
    print("You must specify either a user ID with -u or a user group ID with -g")
    quit();

if MONTH == 0:
    MONTH = Onapp.now.month - 1;

if USER_ID == 0:
    USER_GROUP_DATA = Onapp.Job('GetUserGroupDetails', user_group_id=GROUP_ID).run()
    SUBJECT=USER_GROUP_DATA['label']
    users = [user['id'] for user in Onapp.Job('ListUsersInGroup', user_group_id=GROUP_ID).run()]
if GROUP_ID == 0:
    SUBJECT = Onapp.dsql('SELECT login FROM users WHERE id={}'.format(USER_ID))
    users = [USER_ID]

vms = [];
for user in users:
    vms += Onapp.Job('ListUserVMs', user_id=user).run()

csvbilling = ''

for vm in vms:
    cost = sum([ b['total_cost'] for b in Onapp.Job('GetVMBillingStatsByMonth', vm_id=vm['id'], month=MONTH).run()])
    csvbilling += '{},{},{}\n'.format(vm['id'], vm['label'], cost)


# Create a text/plain message
msg = MIMEText(csvbilling)
me = 'root@localhost'
you = EMAIL

msg['Subject'] = 'Monthly Billing {} for {}/{}'.format(SUBJECT, MONTH, Onapp.now.year)
msg['From'] = me
msg['To'] = you

s = smtplib.SMTP('localhost')
print(s.sendmail(me, [you], msg.as_string()))
s.quit()
