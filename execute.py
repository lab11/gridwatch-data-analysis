#!/usr/bin/env python3
import os
import sys
import signal
import argparse
import subprocess
import json

parser = argparse.ArgumentParser(description = 'Create a dataproc cluster')
parser.add_argument('script')
parser.add_argument('-n','--cluster-name', type=str, required=False) #name of the cluster so you can create multiple
parser.add_argument('-m','--master-machine-type', type=str, required=False)
parser.add_argument('-w','--worker-machine-type', type=str, required=False)
parser.add_argument('-j','--num-workers', type=int, required=False)
parser.add_argument('-z','--zone', type=int, required=False)
parser.add_argument('-g','--get', required=False, action='store_true')
args = parser.parse_args()


#gather cluster variables
cluster_name = 'powerwatch-analysis'
num_workers = 4
worker_machine_type = 'n1-standard-4'
master_machine_type = 'n1-standard-4'
bucket = 'powerwatch-analysis'
zone = 'us-west1-b'

if(args.zone):
    zone = args.zone

if(args.num_workers):
    num_workers = args.num_workers

if(args.worker_machine_type):
    worker_machine_type = args.worker_machine_type

if(args.master_machine_type):
    master_machine_type = args.master_machine_type

if(args.cluster_name):
    cluster_name = args.cluster_name
    bucket = args.cluster_name

config = None
with open('config.json') as data:
    config = json.load(data)

result_name = args.script.split('.')[0]
folder_name = args.script.split('/')
if(len(folder_name) > 1):
    s = "/"
    folder_name = s.join(folder_name[:-1])
else:
    folder_name = ""


if(args.get == False):
    #create the cloud storage bucket if it doesn't already exist
    print("Creating storage bucket...")
    try:
        subprocess.check_output(['gsutil', 'mb', 'gs://' + bucket], stderr=subprocess.STDOUT)
    except Exception as e:
        if type(e) is subprocess.CalledProcessError and str(e.output, 'utf-8').find('409') != -1:
            print("Storage bucket already exists. Proceeding...")
            print()
        else:
            print(e.output);
            raise e
    
    #list the existing clusters
    output = subprocess.check_output(['gcloud', 'dataproc', 'clusters', 'list'])
    cluster_exists = str(output,'utf-8').find(cluster_name) != -1;
    
    if(cluster_exists == False):
        #create the cluster
        print("Creating cluster");
        subprocess.check_call(['gcloud', 'beta', 'dataproc','clusters','create',
                                cluster_name,
                                '--zone=' + zone,
                                '--image-version=1.4',
                                '--max-idle=30m',
                                '--num-workers=' + str(num_workers),
                                '--worker-machine-type=' + worker_machine_type,
                                '--master-machine-type=' + master_machine_type,
                                '--worker-boot-disk-size=250G',
                                '--bucket=' + bucket])
    
        print()
    else:
        print("Cluster already exists. Proceeding...")
        print()
    
    #run the analysis script script with the output argument, and the driver
    #first copy the postgres driver to the bucket
    print("Setting up driver");
    subprocess.check_call(['gsutil', 'cp', './postgres-driver/org.postgresql.jar','gs://' + bucket + '/org.postgresql.jar'])
    print()
        
    first_line = None
    #setup the handler to kill the job if you control C
    def signal_handler(sig, frame):
        #strip out the job number
        job = first_line.split(' ')[1]
        job = job[1:-1]
        print()
        subprocess.check_call(['gcloud', 'dataproc', 'jobs','kill',job])
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    
    print("Starting job");
    try:
        child = subprocess.Popen(['gcloud', 'dataproc', 'jobs','submit','pyspark',
                              args.script,
                              '--cluster=' + cluster_name,
                              '--jars=gs://' + bucket + '/org.postgresql.jar',
                              '--properties=spark.driver.extraClassPath=gs://' + bucket + '/org.postgresql.jar',
                              '--',
                              'gs://' + bucket + '/' + result_name,
                              config['user'],
                              config['password'],
                              'gs://' + bucket], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        print("Error executing analysis script")
        sys.exit(1)
    
    first_line = str(child.stderr.readline(),'utf-8')
    print(first_line.rstrip())
    while True:
        line = child.stderr.readline()
        if not line:
            break
        else:
            print(str(line,'utf-8').rstrip())
    
    print()
    
    #now copy the results to the local folder
    print("Job complete. Copying results");
    try:
        subprocess.check_call(['gsutil', 'cp', '-r', 'gs://' + bucket + '/' + result_name, './' + folder_name])
    except:
        print("Error retrieving results")
else:
    #just copy the most recent results that came out of this script
    print("Copying results from the last job executed on this script and cluster.");
    try:
        subprocess.check_call(['gsutil', 'cp', '-r', 'gs://' + bucket + '/' + result_name, './' + folder_name])
    except:
        print("Error retrieving results")
