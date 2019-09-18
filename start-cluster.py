#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import json

parser = argparse.ArgumentParser(description = 'Create a dataproc cluster')
parser.add_argument('-n','--cluster-name', type=str, required=False) #name of the cluster so you can create multiple
parser.add_argument('-m','--master-machine-type', type=str, required=False)
parser.add_argument('-w','--worker-machine-type', type=str, required=False)
parser.add_argument('-j','--num-workers', type=int, required=False)
parser.add_argument('-z','--zone', type=int, required=False)
args = parser.parse_args()

#gather cluster variables
cluster_name = 'powerwatch-analysis'
num_workers = 4
worker_machine_type = 'n1-standard-4'
master_machine_type = 'n1-standard-4'
bucket = 'powerwatch-analysis'
zone = 'us-west1-b'
region = 'us-west1'

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

#copy the postgres driver to the bucket
print("Setting up postgres driver");
subprocess.check_call(['gsutil', 'cp', './postgres-driver/org.postgresql.jar','gs://' + bucket + '/org.postgresql.jar'])
print()

#copy the livy initialization to the bucket
print("Setting up livy initialization");
subprocess.check_call(['gsutil', 'cp', './dataproc-initialization-actions/livy/livy.sh','gs://' + bucket + '/livy.sh'])
print()

#list the existing clusters
output = subprocess.check_output(['gcloud', 'dataproc', 'clusters', 'list'])
cluster_exists = str(output,'utf-8').find(cluster_name) != -1;

if(cluster_exists == False):
    #create the cluster
    print("Creating cluster");
    subprocess.check_call(['gcloud', 'beta', 'dataproc','clusters','create',
                            cluster_name,
                            '--optional-components=ANACONDA,JUPYTER',
                            '--enable-component-gateway',
                            '--initialization-actions=' + 'gs://' + bucket + '/livy.sh',
                            '--zone=' + zone,
                            '--region=' + region,
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

#port forward from the running livy server on the master to this computer
#this allows a local python notebook to send spark commands to the server
print("Setting up SSH forwarding to cluster master for Livy")
subprocess.check_call(['gcloud', 'compute', 'ssh',cluster_name + '-m',
                        '--zone=' + zone,
                        '--',
                        '-N',
                        '-f',
                        '-L',
                        '8998:localhost:8998'])
