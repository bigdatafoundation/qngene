#!/usr/bin/python
#title           :config.py
#description     :Config file
#author          :Diego Alvarez
#date            :2016-06-05
#python_version  :3.5  
#==============================================================================
import os

##############################
#Replace with your own values
##############################

#Default values to be used in gentotsv.py
source_dir_oxford = os.path.dirname(os.path.abspath(__file__))+"/Data_sample/"
source_file_type_oxford = ".gz"
destination_dir_oxford = os.path.dirname(os.path.abspath(__file__))+"/output/"
destination_file_type_oxford = ".tsv"
sample_file_oxford = "ADVANCE_5.0_6.0.sample"
sample_file_format_oxford = "IMPUTE2"


#To be used in utils.py
log_file_destination = os.path.dirname(os.path.abspath(__file__))+"/qngene_logs/"

#To be used in export.py
source_converted_oxford = os.path.dirname(os.path.abspath(__file__))+"/output/"
imputed_genotype_version = "2i"
