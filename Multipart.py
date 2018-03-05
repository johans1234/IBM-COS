##########################################################################################
# Author: Sascha Johannes     Date: March 2018
#
# This script splits a file into parts and uploads these parts via
# S3 multipart upload into IBM Cloud Object Storage	 
#
# Step 1 - SPLIT FILE:
# Split File Into Parts (temporary saved in ../temp_folder)
#
# Step 2 - MULTIPART UPLOAD:
# Phase 1 = IBM COS - Initiate IBM COS Multipart Upload 
# Phase 2 = IBM COS - Upload Parts
# Phase 3 = IBM COS - Complete Upload 
# Phase 4 = System Clean-Up (delete temp_folder)
#
# Prereqs:
# - Prereq to run the script is a connection to an existing COS service instance (incl. authorization);
#   the authorization method of AWS CLI is easy to install and easy to configure
#	(1. install AWS CLI from the AWS internet page; 2. run AWS CONFIGURE and enter the credentials for your COS bucket) 
# - the imported modules must be available on your system
# - this script has to be in the same folder than the file you want to upload
#
# Preparations:
# - check/adapt IBM COS URL (endpoint)
# - enter Bucket-Name (bucket_name)
#
# Notes:	
# - this script was written and tested on Windows 10 with Pythons 3.6.4
# - in a production environment, some more error handling would be required; this script is
#   just a simple to use sample to demonstrate the advantages and general process of multipart-upload  
# - to demonstrate the time savings, upload a midsize file (e.g. 30MB) with this script and do the 
#   same via a "normal" PUT script/command and compare the run times  
# - I am an absolute beginner in the area of "scripting", so please excuse any uncommon or awkward programming style ;o)  
#
##########################################################################################
import ibm_boto3
import sys
import os
import glob
import threading

endpoint = 'https://s3.eu-geo.objectstorage.softlayer.net'				# IBM COS endpoint URL
bucket_name = 'PLEASE ENTER YOUR BUCKET NAME'							# name of the IBM COS target bucket 

if bucket_name == 'PLEASE ENTER YOUR BUCKET NAME':						# check if bucket_name has been changed 
	print()
	print('-- please enter a valid bucket name in line 43 of the script --')
	sys.exit()

###########################################
######## Step 1 - SPLIT FILE ##############
###########################################
kilobytes = 1024
megabytes = kilobytes * 1000
chunksize = int(6 * megabytes)      									# minimum part size is 6MB   
script_dir = os.path.dirname(os.path.realpath("__file__")) 				# script folder

def split(file, chunksize=chunksize): 
	partnum = 0
	old_parts = (os.path.join(script_dir, 'temp_folder\part*'))			# folder with old parts
	input = open(file, 'rb')                       						# use binary mode on Windows
	
	if not os.path.exists(os.path.join(script_dir, "temp_folder")):     # if temp_folder does not exist
		os.mkdir('temp_folder')                 						# create temp_folder to save parts
		
	else:																# if temp_folder already exists
		for parts in os.listdir("temp_folder/"):						# deletes old parts if existing
			os.remove(os.path.join("temp_folder/", parts)) 				# deletes old parts if existing
			
	while 1:                                       						# save parts in temp_folder
		chunk = input.read(chunksize)              						# get next part <= chunksize
		if not chunk: break
		partnum  = partnum+1
		filename = os.path.join("temp_folder/", ('part%04d' % partnum))
		fileobj  = open(filename, 'wb')
		fileobj.write(chunk)
		fileobj.close()                            						# or simply open(  ).write(  )
	input.close(  )
	assert partnum <= 9999          	              					# join sort fails if 5 digits
	return partnum

##### MAIN ########	
os.system('cls')									
print()
v = 0
directory_list = glob.glob('*')											# read all filenames within script videofolder 
while v == 0:
	print('File to be uploaded?')										
	print("or 'exit'")													
	print()															
	file = input('Filename: ') 											# input Filename 
	if file not in directory_list and file != 'exit':					# check if entered file does exist
		os.system('cls')												
		print()															
		print('-- File must exist in the current folder --')		
		print()
	elif file == 'exit':												# terminate script if 'exit' has been entered
			sys.exit()	
	else:																# continue with split process
		v=1
		
try:
	parts = split(file, chunksize)										# run split function
except:
	print (sys.exc_type, sys.exc_value)									# error message in case of a problem during split
else:
	print('----------------')	
	print ('Split finished:', parts, 'parts are created')				# output number of created parts
		
###########################################
####### Step 2 - MULTIPART UPLOAD #########
###########################################

client = ibm_boto3.client('s3', endpoint_url=endpoint)					# create IBM COS client instance

### PHASE1: INITIATE ###
def initiate_mu():
	response = client.create_multipart_upload(
		Bucket='1158019-3',
		Key=file
	)
	upload_id = response["UploadId"]
	return upload_id

### PHASE2: UPLOAD ###
upload_id=initiate_mu()
etag_dic = {}
new_parts = glob.glob(os.path.join(script_dir, 'temp_folder\part*'))	# creates a list of the parts
parts_counter = len(new_parts) 											# count number of parts

def upload(num):
	filename = 'temp_folder\part%04d' % num
	print('upload started for: part%04d' % num)
	upload_part = open(os.path.join(script_dir, filename), 'rb')		# open the upload_part 
	response = client.upload_part(										# S3 command to upload one part of the file
		Body=upload_part,												# S3 command to upload one part of the file
		Bucket=bucket_name,												# S3 command to upload one part of the file
		Key=file,														# S3 command to upload one part of the file
		PartNumber=num,													# S3 command to upload one part of the file
		UploadId=upload_id)												# S3 command to upload one part of the file
	etag_dic[num] = response['ETag']   									# writes the returned ETag-id into a list; needed in phase 3
	upload_part.close()													# close upload_part 
	print('upload finished for: part%04d' % num)
	
print('----------------')		
threads = []															
for t in range(1,parts_counter+1):										# create threads according number of parts
	thread=threading.Thread(target=upload, args=(t,))					# create threads according number of parts
	threads += [thread]													# create threads according number of parts
	thread.start()														# create threads according number of parts

print('----------------')	
	
for x in threads:														# wait until all threads are finished
	x.join()															# wait until all threads are finished


### PHASE3: COMPLETE UPLOAD ###	

parts_list=[]
for m in range(0,parts_counter):
	parts_list.append({'ETag':etag_dic[m+1],'PartNumber':m+1})    		# creates the list of the uploaded parts and their corresponding ETag-id

response = client.complete_multipart_upload(   							#S3 command to complete the multipart upload process
		Bucket='1158019-3',
		Key=file,
		MultipartUpload={'Parts': parts_list},
		UploadId=upload_id
)

### PHASE4: SYSTEM CLEAN-UP ###

for parts in os.listdir('temp_folder/'):								# delete parts
	os.remove(os.path.join('temp_folder/', parts)) 						# delete parts 
os.rmdir('temp_folder/')												# delete temp_folder

print('----------------')
print('All temp data has been deleted from local disk')
print()
print('----- END OF SCRIPT -----')	

############ END OF SCRIPT ##################

