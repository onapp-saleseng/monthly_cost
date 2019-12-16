#!/usr/bin/python
import os
import re
import ast
import ssl
import sys
import json
import time
import shlex
import base64
import inspect
import argparse
import datetime
import subprocess
#import MySQLdb as SQL
import multiprocessing.pool
from subprocess import Popen
from urllib import urlencode
from random import shuffle, sample, choice
from multiprocessing import Process, Pool, TimeoutError
from urllib2 import Request, urlopen, URLError, build_opener, HTTPHandler, HTTPError

# All of the config options are here.
now = datetime.datetime.now();
LOG_FILE = 'onapp.{}-{}-{}.log'.format(now.year, now.month, now.day)
API_TARGET = 'http://127.0.0.1'
SSH_OPTIONS="-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=30"
ONAPP_ROOT = '/onapp'
ONAPP_CONF_DIR="{}/interface/config".format(ONAPP_ROOT);
ONAPP_CONF_FILE="{}/on_app.yml".format(ONAPP_CONF_DIR);
DB_CONF_FILE="{}/database.yml".format(ONAPP_CONF_DIR);

# arp = argparse.ArgumentParser(prog='Onapp', description='Python library for use with Onapp');
# garp= arp.add_mutually_exclusive_group();
# garp.add_argument("-v", "--verbose", help="Verbose output/logging", action="store_true");
# garp.add_argument("-q", "--quiet", help="Quiet output", action="store_true");
#
# arp.add_argument("-w", "--workers", metavar='N', help="Number of worker processes for starting jobs. Default: 4", default=4);
# arp.add_argument("-u", "--user", metavar='U', help="User ID which has API key and permissions. Default is 1 (admin).", default=1)
#
# args = arp.parse_args();
VERBOSE = False;
quiet = False;
workers = 4
USER_ID = 1

## These classes were stolen from the internet,
## However they're for non-daemonizing the processes
## Because occasionally one needs to spawn some children for a second.
##### I actually may have fixed this, so it may not be necessary anymore. Testing required.
class NoDaemonProcess(Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)

class NoDaePool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess

## This function also stolen
def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def is_ip(s):
    a = s.split('.')
    if len(a) != 4:
        return False
    for x in a:
        if not x.isdigit():
            return False
        i = int(x)
        if i < 0 or i > 255:
            return False
    return True

# functions that I've made, and are used around.
def avg(l,round_to=False):
    if type(l) is not list: raise TypeError('List required')
    try: sum(l)
    except TypeError: raise TypeError('List contains non-number value')
    try:
        if round_to: return round(sum(l)/len(l), round_to)
        else: return sum(l)/len(l)
    except ZeroDivisionError: return False;

def logger(s):
    if not VERBOSE: return;
    l = open(LOG_FILE, "a");
    text = '[{}] - {}\n'.format(str(datetime.datetime.now()),s)
    l.write(text)
    l.flush();
    l.close();
    # if VERBOSE: print text.rstrip();

def runCmd(cmd, shell=False, shlexy=True):
    if shlexy and type(cmd) is str:
        cmd = shlex.split(cmd)
    stdout, stderr = subprocess.Popen(cmd, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate();
    if stderr and 'Warning: Permanently added' not in stderr:
        logger("Command {} failed, stderr: {}".format(cmd, stderr.strip()))
        return False;
    return stdout.strip();

# try to import MySQLdb, attempt to install if not or raise an error.
try:
    import MySQLdb as SQL
except ImportError:
    print "MySQL not detected, attempting to install automatically..."
    runCmd(['yum','-q','-y','install','MySQL-python'])
    try:
        import MySQLdb as SQL
        print "Imported MySQL properly."
    except:
        print "Couldn't install/import MySQL. Please run `sudo yum -y install MySQL-python`."
        raise

def pullDBConfig(f):
    confDict = {};
    conf = open(f).read().split('\n');
    curLabel = False;
    for line in conf:
        if ':' not in line: continue;
        if line.startswith('  '):
            tmp = line.strip().split(':');
            confDict[curLabel][tmp[0].strip()] = tmp[1].strip();
        else:
            tmp = line.strip().split(':');
            if tmp[1] == '':
                curLabel = tmp[0].strip()
                confDict[curLabel] = {};
            else: confDict[tmp[0].strip()] = tmp[1].strip();
    logger("Gathered database configuration.");
    return confDict;

DB_CONFIG = pullDBConfig(DB_CONF_FILE);


def pullOAConfig(f):
    confDict = {}
    conf = open(f).read().split('\n');
    for line in conf:
        if ':' not in line: continue;
        tmp = line.strip().split(':');
        if tmp[1].strip() == '' : continue;
        confDict[tmp[0].strip()] = tmp[1].strip().strip('"').strip("'");
    logger("Gathered OnApp configuration.");
    return confDict;

ONAPP_CONFIG= pullOAConfig(ONAPP_CONF_FILE);

def dbConn(conf=None):
    if conf is None:
        conf = DB_CONFIG[DB_CONFIG['onapp_daemon']];
    return SQL.connect(host=conf['host'], user=conf['username'], passwd=conf['password'], db=conf['database'])

def pullAPIKey():
    db = dbConn();
    cur = db.cursor();
    cur.execute("SELECT api_key FROM users WHERE id={}".format(USER_ID));
    res = cur.fetchone()[0];
    cur.close();
    db.close();
    if res == None:
        print('!! API Key is not in database, please ensure the admin user as an API key generated !!');
        sys.exit();
    logger("Pulled API key from database.");
    return res

def pullAPIEmail():
    db = dbConn();
    cur = db.cursor();
    cur.execute("SELECT email FROM users WHERE id={}".format(USER_ID));
    res = cur.fetchone()[0];
    cur.close();
    db.close();
    if res == None:
        print('!! Admin email was not able to be pulled. !!');
        sys.exit();
    logger("Pulled API Email from database.");
    return res

API_AUTH = base64.encodestring("{}:{}".format(pullAPIEmail(), pullAPIKey())).replace('\n', '');


def __runJob__(j):
    return j.run();

def __runTimedJob__(j):
    return j.timedRun();

def runAll(j):
    jobData = [ job.run() for job in j ];
    return jobData;

def runParallel(j, timed=False):
    if [job.getAction() for job in j if job.getAction() is not 'VMStatus']:
        if len(j) > 1: logger('Starting parallel jobs: {}'.format([job.getAction() for job in j]))
        if VERBOSE: print "Starting parallel jobs: {}".format([job.getAction() for job in j])
    poolHandler = NoDaePool(workers)
    try:
        if timed: jobData = poolHandler.map(__runTimedJob__, j)
        else:     jobData = poolHandler.map(__runJob__, j)
    except:
        poolHandler.close();
        raise;
    if len(j) > 1: logger('Finished parallel jobs. Returned data: {}'.format(jobData))
    poolHandler.close();
    return jobData

def runStaggeredJobs(j, delay, tout=3600):
    if VERBOSE: logger('Starting Staggered Jobs with {}s delay: {}'.format(delay, j))
    poolHandler = NoDaePool(workers);
    jobData = [];
    handlers = [];
    for job in j:
        if timed: handlers.append(poolHandler.map_async(__runJob__, [job]));
        else:     handlers.append(poolHandler.map_async(__runTimedJob__, [job]));
        time.sleep(delay)
    if VERBOSE: logger('Mapped jobs, looking for results.');
    for n, h in enumerate(handlers):
        try:
            jobData.append(h.get(timeout=tout)[0]);
        except TimeoutError:
            if not quiet: print('Job {} timed out after {} seconds, skipping.'.format(j[n], tout));
            pass;
        except:
            poolHandler.close();
            raise;
        time.sleep(1);
    poolHandler.close();
    #checkJobOutput(j, jobData)
    if VERBOSE: logger('Finished staggered jobs.');
    return jobData;

def filterAPIOutput(data):
    if type(data) is not list:
        return data
    max_keys = 0
    for entry in data:
        ek = entry.keys()
        if len(ek) > max_keys: max_keys = len(ek);
    if max_keys == 1:
        return [ e[data[0].keys()[0]] for e in data ]
    return data;


def dictifyStr(s):
    d = {};
    if type(s) == str: stmp = shlex.split(s);
    else: return s;
    for ii in stmp:
        val = ii.split('=');
        d[val[0]] = val[1];
    return d;

dictify = dictifyStr;



def ListHVsInZone(data):
    checkKeys(data, ['hv_zone_id'])
    url = '/settings/hypervisor_zones/{}/hypervisors.json'.format(data['hv_zone_id'])
    r = apiCall(url, method='GET')
    return r;


def ListHVs(data):
    url = '/settings/hypervisors.json'
    r = apiCall(url)
    return r;



def ListVMBackups(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/backups.json'.format(data['vm_id']);
    r = apiCall(url)
    return r;

def ListNormalBackups(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/backups/images.json'.format(data['vm_id']);
    r = apiCall(url)
    return r;

def ListIncrementalBackups(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/backups/files.json'.format(data['vm_id']);
    r = apiCall(url)
    return r;

def ListVMDiskBackups(data):
    reqKeys = [ 'vm_id', 'disk_id' ]
    checkKeys(data, reqKeys)
    url = '/virtual_machines/{}/disks/{}/backups.json'.format(data['vm_id'], data['disk_id'])
    r = apiCall(url)
    return r;

def CreateIncrementalBackup(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/backups.json'.format(data['vm_id'])
    if 'note' in data.keys():
        r = apiCall(url, data={"backup:":{"note":data['note']}}, method='POST')
    else:
        r = apiCall(url, method='POST')
    if len(r) == 1: return r;
    elif type(r) is dict and len(r.keys()) == 1: return r['backup']
    else: return r[0];

def CreateDiskBackup(data):
    checkKeys(data, ['disk_id'])
    url = '/settings/disks/{}/backups.json'.format(data['disk_id'])
    if 'note' in data.keys():
        r = apiCall(url, data={"backup:":{"note":data['note']}}, method='POST')
    else:
        r = apiCall(url, method='POST')
    return r;

def DeleteBackup(data):
    checkKeys(data, ['backup_id'])
    url = '/backups/{}.json'.format(data['backup_id'])
    r = apiCall(url, method='DELETE')
    return r;

def RestoreBackup(data):
    checkKeys(data, ['backup_id'])
    url = '/backups/{}/restore.json'.format(data['backup_id'])
    r = apiCall(url, method='POST')
    return r;

def DetailBackup(data):
    checkKeys(data, ['backup_id'])
    url = '/backups/{}.json'.format(data['backup_id'])
    r = apiCall(url)
    return r;


def ListVMs(data=None, short=True):
    url = '/virtual_machines/per_page/all.json'
    if short: url += '?short'
    r = apiCall(url)
    return r

def ListShortVMs(data):
    r = ListVMs(data, short=True);
    return r;

def DetailVM(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}.json'.format(data['vm_id'])
    r = apiCall(url);
    return r

def AllVMStatuses(data):
    url = '/virtual_machines/status.json'
    r = apiCall(url);
    return r

def VMStatus(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/status.json'.format(data['vm_id'])
    r = apiCall(url);
    return r

def CreateVM(data):
    reqKeys = [
      'memory', 'cpus', 'cpu_shares',
      'hostname', 'label', 'primary_disk_size',
      'required_virtual_machine_build',
      'required_ip_address_assignment',
      'template_id']
    checkKeys(data, reqKeys)
    url = '/virtual_machines.json'
    r = apiCall(url, data={"virtual_machine":data}, method='POST')
    return r

def BuildVM(data):
    reqKeys = [ 'vm_id', 'template_id' ]
    checkKeys(data, reqKeys)
    url = '/virtual_machines/{}/build.json'.format(data['vm_id'])
    datanoid = dict(data)
    del datanoid['vm_id'];
    r = apiCall(url, data={"virtual_machine":datanoid}, method='POST')
    return r;

def EditVM(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}.json'.format(data['vm_id'])
    datanoid = dict(data)
    del datanoid['vm_id'];
    r = apiCall(url, data={"virtual_machine":datanoid}, method='POST')
    return r;

def UnlockVM(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/unlock.json'.format(data['vm_id'])
    r = apiCall(url, method='POST')
    return r;

def MigrateVM(data):
    reqKeys = [ 'vm_id', 'destination' ]
    checkKeys(data, reqKeys)
    url = '/virtual_machines/{}/migration.json'.format(data['vm_id'])
    datanoid = dict(data)
    del datanoid['vm_id']
    r = apiCall(url, data={"virtual_machine":datanoid}, method='POST')
    return r;

def DeleteVM(data):
    checkKeys(data, ['vm_id'])
    dk = data.keys()
    url = '/virtual_machines/{}.json'.format(data['vm_id'])
    if 'convert_last_backup' in dk or 'destroy_all_backups' in dk:
        url += '?'
        if 'convert_last_backup' in dk:
            url += 'convert_last_backup={}'.format(data['convert_last_backup'])
            if 'destroy_all_backups' in dk:
                url += '&'
        if 'destroy_all_backups' in dk:
            url += 'destroy_all_backups={}'.format(data['destroy_all_backups'])
    r = apiCall(url, method='DELETE')
    return r;

def StartVM(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/startup.json'.format(data['vm_id'])
    if 'recovery' in data.keys() and data['recovery'] == True:
        r = apiCall(url, data={"mode":"recovery"}, method='POST')
    else:
        r = apiCall(url, method='POST')
    return r;

def RebootVM(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/reboot.json'.format(data['vm_id'])
    r = apiCall(url, method='POST')
    return r;

def ShutdownVM(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/shutdown.json'.format(data['vm_id'])
    r = apiCall(url, method='POST')
    return r;

def StopVM(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/stop.json'.format(data['vm_id'])
    r = apiCall(url, method='POST')
    return r;

def GetVMBillingStats(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/vm_stats.json'.format(data['vm_id'])
    if 'start' in data.keys() and 'end' in data.keys():
        url += '?period[startdate]={}'.format(data['start'])
        url += '&period[enddate]={}'.format(data['end'])
        if 'use_local_time' in data.keys():
            url += '&period[use_local_time]={}'.format(data['use_local_time'])
    r = apiCall(url)
    return r;

def GetVMBillingStatsByMonth(data):
    checkKeys(data, ['vm_id', 'month'])
    if 'year' not in data.keys():
        data['year'] = now.year;
    start=datetime.datetime(year=data['year'], month=data['month'], day=1, hour=0, minute=0, second=0);
    if(data['month']==12):
        end=datetime.datetime(year=data['year'], month=12, day=31, hour=23, minute=59, second=59);
    else:
        end=datetime.datetime(year=data['year'], month=int(data['month'])+1, day=1) - datetime.timedelta(seconds=1);
    return GetVMBillingStats({'vm_id':data['vm_id'],
        'start' : start.strftime('%Y-%m-%d+%H:%M:%S'),
        'end' : end.strftime('%Y-%m-%d+%H:%M:%S'),
        'use_local_time' : 1 });


def ListVMDisks(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/disks.json'.format(data['vm_id'])
    r = apiCall(url)
    return r;

def DetailVMDisk(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/disks/{}.json'.format(data['vm_id'], data['disk_id'])
    r = apiCall(url);
    return r;

def EditDisk(data):
    checkKeys(data, ['disk_id'])
    url = '/settings/disks/{}.json'.format(data['disk_id'])
    datanoid = dict(data)
    del datanoid['disk_id']
    r = apiCall(url, data={"disk":datanoid}, method='PUT')
    return r;

def GetDiskIOPS(data):
    checkKeys(data, ['disk_id'])
    url = '/settings/disks/{}/usage.json'.format(data['disk_id'])
    r = apiCall(url)
    return r;

def ListNetworks(data=None):
    url = '/settings/networks.json'
    r = apiCall(url);
    return r;

def ListUsers(data=None):
    url = '/users.json'
    r = apiCall(url);
    return r;

def ListUsersInGroup(data):
    checkKeys(data, ['user_group_id'])
    url = '/user_groups/{}/users.json'.format(data['user_group_id'])
    r = apiCall(url);
    return r;

def ListUserVMs(data):
    checkKeys(data, ['user_id'])
    url = '/users/{}/virtual_machines.json'.format(data['user_id'])
    r = apiCall(url);
    return r;

def ListUserGroups(data=None):
    url = '/user_groups.json'
    r = apiCall(url);
    return r;

def GetUserGroupDetails(data):
    checkKeys(data, ['user_group_id'])
    url = '/user_groups/{}.json'.format(data['user_group_id'])
    r = apiCall(url);
    return r;


def HealthcheckVM(data):
    d={}
    if type(data) is dict:
        ks = data.keys();
        if "ip_address" in ks:
            d['ip_address'] = data['ip_address']
        if "identifier" in ks:
            d['identifier'] = data['identifier']
    elif type(data) is str:
        if is_ip(data):
            d = {'ip_address', data}
        else:
            d = {'identifier', data}
    d["cmd"] = "loadavg"
    loadavg = runOnVM(d)
    if not loadavg:
        return False
    else:
        return True

def runOnVM(data, raiseErrors=False):
    ks = data.keys();
    if "cmd" not in ks:
        raise KeyError('Function runOnVM requires cmd key in data')
    if "ip_address" not in ks and "identifier" in ks:
        vm_ip_addr = dsql("SELECT ip_address FROM virtual_machines WHERE identifier={}".format(data['identifier']))
    elif "ip_address" in ks:
        vm_ip_addr = data['ip_address']
    cur_cmd_full = 'ssh {} root@{} "{}"'.format(SSH_OPTIONS, vm_ip_addr, data['cmd'])
    cur_cmd = ['su', 'onapp', cur_cmd_full]
    p = Popen(cur_cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE);
    stdo, stde = p.communicate();
    if raiseErrors and len(stde): raise OnappException(cur_cmd, 'runOnVM', stde)
    elif len(stde): return stde
    return stdo;

def dRunQuery(q, unlist=True):
    db = dbConn();
    cur = db.cursor();
    cur.execute(q)
    res = cur.fetchall();
    cur.close();
    db.close();
    if len(res) == 1 and unlist:
        if len(res[0]) == 1: return res[0][0];
        else: return res[0]
    return res;

dsql = dRunQuery;

def dRunPrettyQuery(q, unlist=True):
    if VERBOSE: logger("Running pretty query:{}".format(' '.join(q.split())))
    db = dbConn();
    cur = db.cursor();
    cur.execute(q)
    res = cur.fetchall();
    num_fields = len(cur.description)
    field_names = [i[0] for i in cur.description]
    cur.close();
    db.close();
    if num_fields == 1 and len(res) == 1 and unlist:
        return res[0][0]
    if num_fields == 1 and len(res) == 1 and not unlist:
        return {field_names[0] : res[0][0]}
    output = [];
    for n, r in enumerate(res):
        o = {}
        for nn, fld in enumerate(field_names):
            if type(res[n][nn]) is datetime.datetime:
                o[fld] = str(res[n][nn])
            else:
                o[fld] = res[n][nn];
        output.append(o)
    if len(output) == 1 and unlist: return output[0]
    if len(output) == 0: return False;
    return output;

dpsql = dRunPrettyQuery;

def dGetDiskID(disk):
    #{'datastore': 'k7blx48pwsvmya', 'uuid': 'l61uni90j7zgxw', 'size': '5'}
    res = dsql('SELECT id FROM disks WHERE identifier=\'{}\''.format(disk['uuid']))
    if res == None:
        print('Disk {} was not found in database!'.format(str(disk)))
        raise OnappException('dbcall', 'getDiskID, {}'.format(uuid or disk_id))
    return res;

def dGetDiskSize(uuid=None, disk_id=None):
    res = None;
    if uuid: res = dsql('SELECT disk_size FROM disks WHERE identifier=\'{}\''.format(uuid))
    if disk_id: res = dsql('SELECT disk_size FROM disks WHERE id=\'{}\''.format(disk_id))
    if res == None:
        print('Could not retrieve size for disk {}'.format(uuid or disk_id))
        raise OnappException('dbcall', 'getDiskSize, {}'.format(uuid or disk_id))
    return res;

def dGetVMFromBackupID(bkpid):
    ttypeid = dsql('SELECT target_type, target_id FROM backups WHERE id={}'.format(bkpid))
    if ttypeid == None:
        print('Getting backup {} data failed.'.format(bkpid))
        raise OnappException('dbcall', 'getVMFromBackupID1, {}'.format(bkpid))
    if ttypeid[0] == 'Disk':
        tid = dsql('SELECT virtual_machine_id FROM disks WHERE id={}'.format(ttypeid[1]))
    else:
        tid = dsql('SELECT identifier FROM virtual_machines WHERE id={}'.format(ttypeid[1]))
    if tid == None:
        return "FAIL";
        print('Getting VM ID from Backup ID {} failed.'.format(bkpid))
        raise OnappException('dbcall', 'getVMFromBackupID2, {}'.format(bkpid))
    return tid;

def dListHVZones():
    zones = []
    res = dpsql("SELECT id, label FROM packs WHERE type='HypervisorGroup'", unlist=False)
    if res == None:
        print('Could not list HV Zones.')
        raise OnappException('dbcall', 'listHVZones')
    return res;

def dListHVsFromZone(zone):
    res = dpsql("SELECT id, label, ip_address, mac FROM hypervisors \
            WHERE hypervisor_group_id={}".format(zone), unlist=False)
    if res == None:
        print('Could not list HVs from zone {}'.format(zone))
        raise OnappException('dbcall', 'listHVsFromZone')
    return res;

# def dDetailVM(vm):
#     req = dsql(''.format(vm));

class Job(object):
    def __init__(self, action, data=None, **kwdata):
        self.action = action;
        if not data: self.data = {};
        else: self.data = data;
        self.data.update(kwdata)

    def __repr__(self):
        if self.action == 'batchRunnerJob':
            return "<Onapp.Job batch-controlled action:{}>".format(self.data['func'])
        return "<Onapp.Job action:{}>".format(self.action);

    def __str__(self):
        if self.action == 'batchRunnerJob':
            return 'Onapp Job object, batch-controlled action: {}|data: {}'.format(self.action, self.data)
        return 'Onapp Job object, action: {}|data: {}'.format(self.action, self.data);

    def addData(self, **kwdata):
        for key, value in kwdata.iteritems():
            self.data[key] = value;

    def delData(self, key):
        del self.data[key]

    def clearData(self):
        self.data = {};

    def getAction(self):
        if self.action == 'batchRunnerJob':
            return self.data['func'];
        else:
            return self.action;

    def run(self):
        if callable(self.action):
            if self.action in [ runAll , runParallel, runStaggeredJobs ]: raise ValueError('{} is not valid for a Job action.'.format(self.action))
            data = self.action(self.data)
        elif self.action not in globals().keys():
            raise OnappException('{}.run{}'.format(self.action), self.data, 'Function Onapp.{} does not exist.'.format(self.action))
        if type(self.action) is str:
            if self.action in ['runAll' , 'runParallel' , 'runStaggeredJobs' ]:
                raise ValueError('{} is not valid for a Job action.'.format(self.action))
            try:
                data = globals()[self.action](self.data);
            except HTTPError: raise;
            except OnappException as err:
                print "Error in job {} : {}".format(self.action, sys.exc_info()[0]);
                return False;
            except:
                e = sys.exc_info()
                print "!!!!! ERROR OCCURRED INSIDE JOB {} : {} line {} !!!!!\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!".format(self.action, e, e[2].tb_lineno)
                print str(self.data)
                return False;
        if type(data) is dict and len(data.keys()) == 1:
            return data.values()[0];
        else:
            return data;

    def timedRun(self):
        beginTime = datetime.now();
        if callable(self.action):
            if self.action in [ runAll , runParallel, runStaggeredJobs ]: raise ValueError('{} is not valid for a Job action.'.format(self.action))
            data = self.action(self.data)
        elif self.action not in globals().keys():
            raise OnappException('{}.run'.format(self.action), self.data, 'Function Onapp.{} does not exist.'.format(self.action))
        if type(self.action) is str:
            if self.action in ['runAll' , 'runParallel' , 'runStaggeredJobs' ]:
                raise ValueError('{} is not valid for a Job action.'.format(self.action))
            try:
                data = globals()[self.action](self.data);
            except OnappException as err:
                print "Error in job {}"
                if not CONTINUE_MODE: raise
            except:
                print "!!!!! ERROR OCCURRED INSIDE JOB {} : {} !!!!!\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!".format(self.action, sys.exc_info())
                raise
        timeTook = datetime.now() - beginTime;
        if type(data) is dict and len(data.keys()) == 1:
            return (data.values()[0], timeTook);
        else:
            return (data, timeTook);


def checkKeys(data, reqKeys):
    dk = data.keys();
    caller = inspect.stack()[1][3];
    for k in reqKeys:
        if k not in dk: raise KeyError('{} requires data key {}'.format(caller, k))

def apiCall(r, data=None, method='GET', target=API_TARGET, auth=API_AUTH):
    req = Request("{}{}".format(target, r), json.dumps(data))
    if auth: req.add_header("Authorization", "Basic {}".format(auth))
    req.add_header("Accept", "application/json")
    req.add_header("Content-type", "application/json")
    if method: req.get_method = lambda: method;
    try:
        if target.startswith('https://'):
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            ssl_context.load_default_certs();
            response = urlopen(req, context=ssl_context)
        else:
            response = urlopen(req)
        status = response.getcode();
    except HTTPError as err:
        caller = inspect.stack()[1][3];
        print caller,"called erroneous API request: {}{}, error: {}".format(target, r, err)
        status = response.getcode()
        # if r.endswith('status.json'): raise;
        # else: return False;
        raise;
    try:
        status;
    except NameError:
        status = response.getcode();
    if VERBOSE and 'status.json' not in r: logger('API Call executed - {}{}, Status code: {}'.format(API_TARGET, r, status));
    apiResponse = response.read().replace('null', 'None').replace('true', 'True').replace('false', 'False')
    if apiResponse != '':
        pyResponse = ast.literal_eval(apiResponse)
        retData = filterAPIOutput(pyResponse)
    else:
        retData = False;
    if status in [200, 201, 204]:
        return retData;
    else:
        raise OnappException('apiCall', "{}: Unknown HTTP Status code".format(status), caller)

def storageAPICall(target, r, data=None, method=None):
    req = Request("http://{}:8080{}".format(target, r), data)
    if method: req.get_method = lambda: method;
    response = urlopen(req)
    status = response.getcode()
    caller = inspect.stack()[1][3];
    # print 'API Call executed - {}{}, Status code: {}'.format(target, r, status);
    return ast.literal_eval(response.read().replace('null', 'None').replace('true', 'True').replace('false', 'False'));

stapi = storageAPICall


class OnappException(Exception):
    def __init__(self, d, f, reason=False):
        self.data = d;
        self.func = f;
        self.reason = reason;
        print('OnappError, Action: {}, Data: {}'.format(f, d))
        if self.reason is not False: print('Reason: {}'.format(reason))
