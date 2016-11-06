#!/usr/bin/python
#title           :utils.py
#description     :Script with functions shared by all .py scripts
#author          :Simon Grondin - Diego Alvarez
#date            :2016-06-05
#python_version  :3.5  
#==============================================================================

import re

#Begin Diego Alvarez
import logging
import sys
import os
import datetime
import config
#End Diego Alvarez

NEWLINE_REPLACEMENT = " " # Default
SUBARRAY_SEPARATOR = ";" # Default
FIELD_SEPARATOR = "," # Default
newline_regex = re.compile(r"[\n\r]")

# Sanitizes all the elements in a list
def prepare_fields(li):
	for i, x in enumerate(li):
		updated = str(x) if x is not None else ""
		updated = newline_regex.sub(NEWLINE_REPLACEMENT, updated)
		li[i] = updated
	return li

# Turns a cursor into a list ready to write
def to_prepared_list(li):
	return prepare_fields(list(li))

# Turns a cursor into a csv string
def to_csv(li):
	return FIELD_SEPARATOR.join(to_prepared_list(li))

# Completely drains a cursor to return a csv string
def drain_cursor_to_csv(cursor):
	result = []
	f = cursor.fetchone()
	while f:
		result.append(to_csv(f))
		f = cursor.fetchone()
	return "\n".join(result)

# Returns the length of the longest element at a position.
# Rewinds the cursor before returning
def size_of_subarray(cursor, pos):
	size = 0
	cursor.scroll(0, "absolute")
	f = cursor.fetchone()
	while f:
		size = max(size, len(f[pos].split(SUBARRAY_SEPARATOR)))
		f = cursor.fetchone()
	cursor.scroll(0, "absolute")
	return size

# Turns the subarray created by array_to_string(array_agg(...)) into a python list
def break_subarray(li, pos, size):
	sub = li[pos].split(SUBARRAY_SEPARATOR)
	padding = ["" for i in xrange(0, size - len(sub))]
	return li[:pos] + sub + li[pos+1:] + padding

# Prepares headers dynamically based on the number of columns
def make_headers(name, pos, nb):
	return FIELD_SEPARATOR.join([name+str(i) for i in xrange(pos, nb)])

# Prepares placeholders (ex: {0}) dynamically to fill in with format()
def make_placeholders(pos, nb):
	return FIELD_SEPARATOR.join(["{"+str(i)+"}" for i in xrange(pos, pos+nb)])

#Begin Diego Alvarez
def create_logger(p_source):
        # Create a logger

	try:
        	os.makedirs(config.log_file_destination)
	except OSError as err:
	  pass
	#utils.log(logger,str(err)) 

        now = datetime.datetime.now()
        LOG_FILENAME=config.log_file_destination+"log_"+p_source+"_"+now.strftime("%Y%m%d_%H%M%S")+".out"

        my_logger = logging.getLogger('MyLogger')
	my_logger.setLevel(logging.DEBUG)

	# Create file handler
	fhandler = logging.FileHandler(LOG_FILENAME)
	fhandler.setLevel(logging.DEBUG)

	# create a logging format
	#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	formatter = logging.Formatter('%(asctime)s - %(message)s')
	fhandler.setFormatter(formatter)

	# add the handlers to the logger
	my_logger.addHandler(fhandler)
        
        return my_logger;

def log(p_logger, p_message):
  	p_logger.debug(p_message)


#def find_file_in_list(p_filelist, p_filename):
#   
#    filename = None
#    for item in p_filelist:
#	if p_filename in item:
#           filename=item
#	   break
#    return filename

def find_file_in_list(p_filelist, p_regex):
   
	filename = None
       
	for item in p_filelist:
		if re.match(p_regex, item):
				filename=item
				break
	return filename


    
def get_file_name(p_filename):
 #Get filename without the extension
 filename, file_extension = os.path.splitext(p_filename)
 short_filename=filename.split()

 return short_filename[0]

def get_individual_info(p_filename, p_info):
   
   info= None

   with open(p_filename,'rb') as f:
	
     for line in f:
	  columns=line.split()       	 
          if columns[0] == p_info:
             info =  columns[2]
             break
   return info

  

#End Diego Alvarez
