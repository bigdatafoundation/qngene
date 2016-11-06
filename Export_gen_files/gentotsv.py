#!/usr/bin/python
#title           :gentotsv.py
#description     :Script to process impute files .gz and create a csv file
#author          :Diego Alvarez
#date            :2016-06-05
#python_version  :3.5  
#==============================================================================

import gzip
import os
import fnmatch
import csv
import sys
import getopt
import time
import linecache
import utils 
import config
from multiprocessing import Pool, Process
import multiprocessing
from functools import partial


      
def script_usage():
  print 'gentotsv.py  -h<help> -t<threads> -s<sourcedir> -d<destinationdir> -f<samplefile>' 
  print '---------'
  print 'If no parameters are passed, default values are taken from <config.py>'
  print 'Default #threads =  #processor cores'
  print '----------------'
  
  return


def get_gen_file_columns(p_source_dir,p_source_file):
 with gzip.open(p_source_dir+p_source_file,'rb') as genfile:
                      
   utils.log(logger,"GEN file: "+ p_source_file)

   columns=genfile.readline().split()
   totalcolumns = len(columns)

   utils.log(logger,"Columns in GEN file: "+str(totalcolumns))

                                
 genfile.close()  
 return totalcolumns
 
def create_sample_file(p_source_dir,p_destination_dir, p_source_file, p_file_type):
  
   utils.log(logger,"Begin - create_sample_file -")
     
   samplecountlines = 0
   source_file = utils.get_file_name(str(p_source_file))
     
   with open(p_destination_dir+"SAM_"+source_file+p_file_type, 'wb') as xfile:

    utils.log(logger,"Reading file SAMPLE: " + p_source_file)
           
    csvwriter = csv.writer(xfile,delimiter='\t',quotechar='"', quoting=csv.QUOTE_MINIMAL) 
                 
    with open(p_source_dir+p_source_file,'rb') as samplefile:
                
     INDfilelist = []

     for line in samplefile:
                
          samplecountlines=samplecountlines+1
          
          if samplecountlines <= 2:
           
            seq=str(samplecountlines * (-1)).split()
            columns=line.split()
            csvwriter.writerow(seq+columns)     
                   
          #Start counting individuals
          if samplecountlines > 2:
                     
              seq=str(samplecountlines-2).split()
              columns=line.split()                    
            
              col01= columns[0:2] #to create the file ID
              csvwriter.writerow(seq+columns)
                                       
           #Create empty INDIVIDUAL file
              INDfilename = create_individuals_file(p_destination_dir, seq[0]+"_"+col01[0]+"_"+col01[1], p_file_type)
              #Create list with Individuals file             
              INDfilelist.append(INDfilename)         
         
                 
    samplefile.close() 
   xfile.close()

   utils.log(logger,"SAMPLE file lines: "+ str(samplecountlines))
   utils.log(logger,"End - create_sample_file -")
   return INDfilelist

def create_individuals_sample_files(p_source_dir,p_destination_dir, p_source_file, p_file_type):
   utils.log(logger,"Begin - create_individuals_sample_files -")
     
   samplecountlines = 0
   source_file = utils.get_file_name(str(p_source_file))
   INDfilelist = []

   with open(p_source_dir+p_source_file,'rb') as samplefile:
     for line in samplefile:

       samplecountlines = samplecountlines + 1
       columns = line.split()
      
       if samplecountlines == 1:
         headerline = columns[:]

       elif samplecountlines == 2:
         
         datatypeline = columns[:]
       else:
         individualline = samplecountlines - 2
         with open(p_destination_dir+"SAM_"+str(individualline)+"_"+str(columns[0])+"_"+str(columns[1])+p_file_type, 'wb') as xfile:
         
                  csvwriter = csv.writer(xfile,delimiter='\t',quotechar='"', quoting=csv.QUOTE_MINIMAL) 

                  for i in range(0, len(columns)):
                    
                      csvwriter.writerow([headerline[i]]+[datatypeline[i]]+[columns[i]])
                 
                 
                  #Create empty INDIVIDUAL file
                  INDfilename = create_individuals_file(p_destination_dir, str(individualline)+"_"+columns[0]+"_"+columns[1], p_file_type)
                  #Create list with Individuals file             
                  INDfilelist.append(INDfilename)

     
         xfile.close()
   samplefile.close()

   utils.log(logger,"SAMPLE file lines: "+ str(samplecountlines))
   utils.log(logger,"End - create_individuals_sample_files -")
   
   return INDfilelist


def create_snp_file(p_source_dir,p_destination_dir, p_source_file_type, p_dest_file_type):
  
  utils.log(logger,"Begin - Create SNP file -")

  filename = p_destination_dir+"SNP"+p_dest_file_type
  open(filename, 'w').close()
  
  for file_list in sorted(os.listdir(p_source_dir)):
     if fnmatch.fnmatch(file_list,'*'+p_source_file_type):
       with gzip.open(p_source_dir+file_list,'rb') as genfile:

         sequence=0
         gencountlines=0

         utils.log(logger,"Reading file GEN: " + file_list)

         with open(filename,'ab') as SNPfile:
               csvwriter = csv.writer(SNPfile,delimiter='\t',quotechar='"', quoting=csv.QUOTE_MINIMAL)
                 
               #readlines() Loads full .gen file into memory and split in lines. To many threads 
               # or very big files can cause memory overflow.     
               #for line in genfile.readlines(): 
               for line in genfile: #Read file line by line
                   gencountlines=gencountlines+1

                   columns=line.split()
                   col05=columns[0:5]

  
                   source_file = utils.get_file_name(file_list)

                   sequence=sequence+1
                   seq=str(sequence).split()  

                   csvwriter.writerow([source_file]+seq+col05)
                             
          
         SNPfile.close()
       genfile.close()

  utils.log(logger,"End - Create SNP file -")
  return     
                      
 
def create_individuals_file(p_destination_dir, p_filename, p_file_type):

  filename = p_destination_dir+"IND_"+p_filename+p_file_type

  open(filename, 'w').close()

  return filename    
      
def convert_cols_to_lines(p_source_dir,p_source_file,p_destination_dir,p_dest_file_list, p_individualsposlist, p_gen_column):

           utils.log(logger,"Begin - convert_gen_cols_to_ind_lines - ")           
           positionindex = p_individualsposlist.index(p_gen_column)
           regex =  r"^{0}.*{1}$".format(p_destination_dir+"IND_"+str(positionindex+1)+"_",destination_file_type)
           
           p_indfilename = utils.find_file_in_list(p_dest_file_list,regex)
           source_file = utils.get_file_name(str(p_source_file))

           try:
          
              col = int(p_gen_column)

           except:
              e = sys.exc_info()[0]
              utils.log(logger,e)


           #Open individuals file  
           with open(p_indfilename,'a') as indfile:         
            
            
             utils.log(logger,"Writing IND .tsv file: "+ p_indfilename)
            
             csvwriter = csv.writer(indfile,delimiter='\t',quotechar='"', quoting=csv.QUOTE_MINIMAL)
             sequence = 0 
          
             with gzip.open(p_source_dir+p_source_file,'rb') as genfile:
                for line in genfile: #reads line by line .gen file.

                 #readlines() loads full .gen file into memory and split in lines. To many threads 
                 # or very big files can cause memory overflow. 
                 #for line in genfile.readlines():
          
                   sequence=sequence+1

                   seq=str(sequence).split() 
                   columns=line.split()
                        
                   csvwriter.writerow([source_file]+seq+columns[col:col+3])
              
                indfile.close()
                                 
                utils.log(logger,"Lines in source file: "+ str(sequence))

             genfile.close()
             utils.log(logger,"End - convert_gen_cols_to_ind_lines - ")           
             return     

def update_individuals_file(p_source_dir,p_source_file_type,p_destination_dir,p_dest_file_list):
    
    utils.log(logger,"Begin - update_individuals_file -")     
    for file_list in sorted(os.listdir(p_source_dir)): 
     
     if fnmatch.fnmatch(file_list,'*'+p_source_file_type):

         if  __name__ =='__main__':

                #with gzip.open(p_source_dir+file_list,'rb') as genfile:
                #read only first line
                genfile = gzip.open(p_source_dir+file_list,'rb')
                columns=genfile.readline().split()
                genfile_columns = len(columns)
                genfile.close()
                 
                utils.log(logger, "numthreads: "+str(numthreads))
                pool = Pool(int(numthreads))
                                 
                utils.log(logger,"Reading GEN file: "+ file_list)

                index =5
                individualpos = 0
                individualsposlist = []

                #create list with all individuals position
                while(index < genfile_columns):
                           
                           individualsposlist.append(index)
                           index = index + 3
                        
                func = partial(convert_cols_to_lines,p_source_dir,file_list,p_destination_dir,p_dest_file_list,individualsposlist)
                pool.map_async(func,individualsposlist).get(9999999)

    utils.log(logger,"End - update_individuals_file -")     
    return
     

###########################################################################################################
###############################################Main function###############################################     
###########################################################################################################



try:

     print 'ARGV      :', sys.argv[1:]
     opts, args = getopt.getopt(sys.argv[1:], 'ht:s:d:f:', ['help=','threads=','sourcedir=','destinationdir=','samplefile='])
     print 'OPTIONS   :', opts

     #Initialization
     help=0

     samplecount=0
     samplecounttotal=0
     gencount=0
     gencounttotal=0
     poscount=0 

     #Get default values
     source_dir = config.source_dir_oxford
     source_file_type = config.source_file_type_oxford
     destination_dir = config.destination_dir_oxford
     destination_file_type =config.destination_file_type_oxford
     sample_file = config.sample_file_oxford
     sample_file_format = config.sample_file_format_oxford
     numthreads = multiprocessing.cpu_count()

          
     #Pass the script name to Log
     logger=utils.create_logger("gentotsv")
     
     start_time = time.time()
     print "Start time: "+time.ctime()
     utils.log(logger, "Start time: "+time.ctime())



     for opt,arg in opts:

           if opt=='-h':
              help = 1
              script_usage()


           elif opt=='-t':

              global numtreads
              numthreads = arg

           elif opt=='-s':
     
              source_dir = arg

           elif opt=='-d':
     
              destination_dir = arg
     
           elif opt=='-f':

              sample_file = arg
          
     if help == 0:

          print "Number of threads: "+str(numthreads)
          utils.log(logger, "Number of threads: "+str(numthreads))     
          print "Sample file format: "+sample_file_format
          utils.log(logger, "Sample file format: "+sample_file_format)          
          print "Source directory: "+source_dir
          utils.log(logger, "Source directory: "+source_dir)
          print "Destination directory: "+destination_dir
          utils.log(logger, "Destination directory: "+destination_dir)
          print "Sample file name: "+sample_file
          utils.log(logger, "Sample file name: "+sample_file)

          if not os.path.exists(source_dir):
             utils.log(logger, "EXCEPTION - Source directory "+source_dir+" does not exist")   
             sys.exit("EXCEPTION - Source directory "+source_dir+" does not exist")

          #Create destination directory
          try:
             os.makedirs(destination_dir)
          except OSError as err:
             pass       


          if os.path.isfile(source_dir+sample_file):
            
             #----------------------     
             #Convert SAMPLE to TSV
             # 1 file = 1 individual
             #----------------------
             INDfilelist = create_individuals_sample_files(source_dir,destination_dir,sample_file,destination_file_type)
             
             # 2 threads. Parallel processing.
             if __name__=='__main__':
               
                 #----------------------     
                 #Create SNP file with 1st 5 columns of GEN file
                 #----------------------
                 #create_snp_file(source_dir,destination_dir,source_file_type, destination_file_type)
                  p1 = Process(target=create_snp_file, args=(source_dir,destination_dir,source_file_type, destination_file_type))
                  p1.start()

                 #----------------------     
                 #Convert GEN to TSV
                 # 1 file = 1 individual
                 #---------------------- 
                 #update_individuals_file(source_dir,source_file_type,destination_dir,INDfilelist)
                  p2 = Process(target=update_individuals_file, args=(source_dir,source_file_type,destination_dir,INDfilelist))
                  p2.start()
                        
                  p1.join()
                  p2.join()

                     
          else:
               utils.log(logger," EXCEPTION - Sample File: " + sample_file + " does not exists")
               sys.exit("Sample File: " + sample_file + " does not exists")
            

     print time.ctime()
     utils.log(logger, "End time: "+time.ctime())

except getopt.GetoptError as err:
  print str(err)
  utils.log(logger,str(err)) 
  sys.exit(2)
except:
  exc_type, exc_obj, tb = sys.exc_info()
  f = tb.tb_frame
  lineno = tb.tb_lineno
  filename = f.f_code.co_filename
  linecache.checkcache(filename)
  line = linecache.getline(filename, lineno, f.f_globals)
  print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)
  utils.log(logger,'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))
  sys.exit(2)
 



