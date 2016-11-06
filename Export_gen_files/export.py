#!/usr/bin/python
#title           :export.py
#description     :Script generate tsv from Advance, dbSNP and .gen/sample files
#author          :Simon Grondin - Diego Alvarez
#date            :2016-06-05
#python_version  :3.5  
#==============================================================================


import sys
import os
from time import sleep
import psycopg2
import utils
import config
import re


 
# Constants
# utils.NEWLINE_REPLACEMENT = " " # Can be uncommented and edited
# utils.SUBARRAY_SEPARATOR = ";" # Can be uncommented and edited



if len(sys.argv) != 7:
     print "Passed arguments were: " + str(sys.argv)
     print "Arguments must be: export.py outputDir dbHost dbName dbUser dbPassword importGenotypeFlag"
     exit(0)


logger=utils.create_logger("export") 

output_dir = "./" + sys.argv[1]
print output_dir
output_dir = output_dir if output_dir[-1] == "/" else output_dir + "/"
connection_string = "dbname={1} host={0} user={2} password={3}".format(*sys.argv[2:])

importGenotypesFlag = None
sourcesDirectory = None


importGenotypesFlag = sys.argv[6]
if importGenotypesFlag == "Y":
  sourcesDirectory = config.source_converted_oxford
  file_type_oxford = config.destination_file_type_oxford

  if sourcesDirectory[-1]=="/":
     sourcesDirectory = sourcesDirectory
  else: 
     sourcesDirectory=sourcesDirectory + "/"
  
  sourcesFileList = sorted(os.listdir(sourcesDirectory))
          

print "Connecting to " + connection_string
conn = psycopg2.connect(connection_string)
cursor = conn.cursor()
print "Connected\n"
sleep(2)


#########################
### Individuals_##### ###
#########################
print "Exporting Individuals"

# array_agg + array_to_string = Like SUM(), but for text fields. Concatenates strings.
cursor.execute("""
	SELECT
		I.id, I.dob, I.sex, I.ethnic_code, I.centre_name, I.region_name,
		I.country_name, I.comments, COALESCE(B.batches, '')
	FROM "public"."person" I
	LEFT JOIN (
		SELECT B.person_id, array_to_string(array_agg(B.batch_id::text), '{0}') AS batches
		FROM "public"."batches" B
		GROUP BY B.person_id
		ORDER BY B.person_id ASC
	) B ON I.id = B.person_id
	ORDER BY I.id ASC
""".format(utils.SUBARRAY_SEPARATOR))
if importGenotypesFlag != "Y":
     print "Writing " + str(cursor.rowcount) + " files\n"


# This section will need to create the headers and the rows dynamically because
# the number of columns is not known ahead of time! See utils.py for more info.
# 8 is where the batches subarray is located
subarrayPos = 8
nbBatches = utils.size_of_subarray(cursor, subarrayPos)
batchPlaceholders = utils.make_placeholders(subarrayPos, nbBatches)
headerPlaceholders = utils.make_headers("batch", 0, nbBatches)
f = cursor.fetchone()
current_id = -1
migratedindividuals=0

utils.log(logger, "Writing <<Individuals>> files")
utils.log(logger, "------------------------------------")

while f:
     current_id = f[0]

     if importGenotypesFlag == "Y":

                regex =  r"^SAM_[0-9]+_{0}_.*{1}$".format(current_id,file_type_oxford)
                filename =  utils.find_file_in_list(sourcesFileList, regex)
                familyID = ''
                fatherID = ''
                motherID = ''

                if filename == None:
                        utils.log(logger, "ID: "+str(current_id) +" -Not found in SAMPLE file")
                else:

                  os.makedirs(output_dir + str(current_id))

                  with open("{0}{1}/Individuals_{1}.tsv".format(output_dir, current_id), "w") as file:
                    file.write("individualId,familyId,paternalId,maternalId,dateOfBirth,gender,ethnicCode,centreName,region,country,notes,missingCallFreq,{0}\n".format(headerPlaceholders))
                    familyID=utils.get_individual_info(sourcesDirectory+filename, "ID_2")
                    fatherID=utils.get_individual_info(sourcesDirectory+filename, "father")                    
                    motherID=utils.get_individual_info(sourcesDirectory+filename, "mother")
                    sex=utils.get_individual_info(sourcesDirectory+filename, "sex")
                    missingCallFreq=utils.get_individual_info(sourcesDirectory+filename, "missing")
                    migratedindividuals = migratedindividuals + 1
               
                    #According to Impute2
                    if sex=='1':
                       sex="m"
                    elif sex=='2':
                       sex="f"
                    
                    li = utils.to_prepared_list(f + (familyID,fatherID,motherID,missingCallFreq))
                    li = utils.break_subarray(li, subarrayPos, nbBatches)     
                            
                    if sex != li[2]:
                         utils.log(logger, "Difference - Sample SEX: "+sex+ " vs. Advance SEX: "+li[2])

                             
                    file.write(("""{0},{11},{12},{13},{1},{2},{3},{4},{5},{6},{7},{14},"""+batchPlaceholders+"""\n""").format(*li))

          
     else:
          os.makedirs(output_dir + str(current_id))
          with open("{0}{1}/Individuals_{1}.tsv".format(output_dir, current_id), "w") as file:
               file.write("individualId,familyId,paternalId,maternalId,dateOfBirth,gender,ethnicCode,centreName,region,country,notes,missingCallFreq,{0}\n".format(headerPlaceholders))
               migratedindividuals = migratedindividuals + 1
               li = utils.to_prepared_list(f)
               li = utils.break_subarray(li, subarrayPos, nbBatches)     

               file.write(("""{0},,,,{1},{2},{3},{4},{5},{6},{7},,"""+batchPlaceholders+"""\n""").format(*li))
               
     f = cursor.fetchone()

################################
### Individuals_#####_Visits ###
################################
print "Exporting Individuals Visits"
cursor.execute("""
     SELECT V.person_id, V.id, V.visit_date, V.form_id, V.fasting, V.descr
     FROM "public"."visit" AS V
     ORDER BY V.person_id ASC, V.id ASC
""")
if importGenotypesFlag != "Y":
     print "Writing " + str(cursor.rowcount) + " files\n"
f = cursor.fetchone()
current_id = -1

utils.log(logger, "Writing <<Individuals_Visits>> files")
utils.log(logger, "------------------------------------")

while f:
     if f[0] != current_id: # We only close the file when we switch to a new Person
          try: # Ugly, but Python doesn't let me create ad hoc mocks like JS
               file.close()
          except:
               pass

          current_id = f[0]
          createdfile=0

          if importGenotypesFlag == "Y":

               regex =  r"^SAM_[0-9]+_{0}_.*{1}$".format(current_id,file_type_oxford)
               filename =  utils.find_file_in_list(sourcesFileList, regex)
     
               if filename == None:
                     utils.log(logger, "ID: "+str(current_id) +" -Not found in SAMPLE file")
               else:
          
                    file = open("{0}{1}/Individuals_{1}_Visits.tsv".format(output_dir, current_id), "a")
                    file.write("visitId,visitDate,studyFormId,fasting,description\n")
                    createdfile=1
     
          else:
                    file = open("{0}{1}/Individuals_{1}_Visits.tsv".format(output_dir, current_id), "a")
                    file.write("visitId,visitDate,studyFormId,fasting,description\n")
                    createdfile=1

     if createdfile==1:
          li = utils.to_prepared_list(f)
          file.write("""{1},{2},{3},{4},{5}\n""".format(*li))
     
     f = cursor.fetchone()

####################################
### Individuals_#####_Phenotypes ###
####################################
print "Exporting Individuals Phenotypes"
cursor.execute("""
     SELECT V.person_id, V.id, P.name, M.value, P.um, P.descr, V.visit_date
     FROM "public"."phenotype" AS P
     LEFT JOIN (
          SELECT * FROM "public"."measure" M
     ) AS M ON P.id = M.phenotype_id
     LEFT JOIN "public"."visit" V ON M.visit_id = V.id
     WHERE V.person_id IS NOT NULL -- There's 1 buggy row, exclude it
     ORDER BY V.person_id ASC
""")
if importGenotypesFlag != "Y":
     print "Writing " + str(cursor.rowcount) + " files\n"

f = cursor.fetchone()
current_id = -1

utils.log(logger, "Writing <<Individuals_Phenotypes>> files")
utils.log(logger, "----------------------------------------")

while f:
     if f[0] != current_id: # We only close the file when we switch to a new Person
          try: # Ugly, but Python doesn't let me create ad hoc mocks like JS
               file.close()
          except:
               pass

          current_id = f[0]
          createdfile=0

          if importGenotypesFlag == "Y":

               regex =  r"^SAM_[0-9]+_{0}_.*{1}$".format(current_id,file_type_oxford)#check if I I can use IND file
               filename =  utils.find_file_in_list(sourcesFileList, regex)
               
               if filename == None:
                             utils.log(logger, "ID: "+str(current_id) +" -Not found in SAMPLE file")
               else:

                    file = open("{0}{1}/Individuals_{1}_Phenotypes.tsv".format(output_dir, current_id), "a")
                    file.write("visitId,phenotypeType,phenotypeGroup,name,measureDataType,measure,units,description,diagnosisDate\n") 
                    createdfile=1
          else:
                    file = open("{0}{1}/Individuals_{1}_Phenotypes.tsv".format(output_dir, current_id), "a")
                    file.write("visitId,phenotypeType,phenotypeGroup,name,measureDataType,measure,units,description,diagnosisDate\n") 
                    createdfile=1
     
     if createdfile==1:
          li = utils.to_prepared_list(f)
          file.write("""{1},,,{2},,{3},{4},{5},{6}\n""".format(*li))
     
     f = cursor.fetchone()


###################################
### Individuals_#####_Genotypes ###
###################################
print "Exporting Individuals Genotypes"

mapping = {
     "1": "AA",
     "2": "BB",
     "3": "AB",
     "4": "00",
     "5": "A0", # Doesn't happen in practice
     "6": "B0", # Doesn't happen in practice
     "X": "00",
     "x": "00" # Saves a call to lower()
}

# See Simon_Grondin_PFE.pdf for more information about why there's 2 cursors and how they work together
# It's too much information to write it all here.

cursor1 = conn.cursor()
cursor2 = conn.cursor()
# The first query loads data from the "snp_genotype" table, joined with the "person" table
print "Executing SQL query 1/2..."
cursor1.execute("""
     SELECT G.person_id, G.genotype_version, P.sample, G.genotype
     FROM "public"."snp_genotype" G
     INNER JOIN "public"."person" P on G.person_id = P.id
     ORDER BY G.person_id ASC, G.genotype_version ASC LIMIT 4
""")

# The second query loads data from the "snp_annotation" table, joined with the "vcf" table
print "Executing SQL query 2/2..."
cursor2.execute("""
     SELECT
          A.genotype_index, A.annotation_version, A.rs,
          V.chromosome, V.position, (V.position + CHAR_LENGTH(ref)),
          V.ref, V.alt, A.allele_a, A.allele_b
     FROM "public"."snp_annotation" A
     LEFT JOIN "public"."vcf" V ON A.rs = V.rs
     ORDER BY A.annotation_version ASC, A.genotype_index ASC
""")
if importGenotypesFlag != "Y":
     print "Writing " + str(cursor1.rowcount) + " files\n"

f1 = cursor1.fetchone()
f2 = cursor2.fetchone()

utils.log(logger, "Writing <<Individuals_Genotypes>> files")
utils.log(logger, "------------------------------------")

if importGenotypesFlag != "Y": 
     while f1:
          current_id = f1[0]
          current_version = f1[1]
          filename = "{0}{1}/Individuals_{1}_v{2}_Genotypes.tsv".format(output_dir, current_id, current_version)
          print "Writing " + filename
          file = open(filename, "a")
          file.write("personId,genotypeVersion,sample,letter,rs,chromosome,start,end,ref,alt,allele_a,allele_b,genotypeLikelihoods0,genotypeLikelihoods1,genotypeLikelihoods2\n")

          buffer = []
          while f2 and f2[1] == current_version:
               li1 = utils.to_prepared_list(f1[:3])
               li2 = utils.to_prepared_list(f2[2:])
               letter = mapping[f1[3][f2[0]-1]]
               buffer.append("""{0},{1},{2},{3},{4},{5},{6},{7},{8},"{9}",{10},{11},,,\n""".format(*(li1+[letter]+li2)))
               f2 = cursor2.fetchone()

          file.write("".join(buffer))
          if f2 is None: # rewind
               cursor2.scroll(0, "absolute")
               f2 = cursor2.fetchone()

     
          file.close()
          f1 = cursor1.fetchone()


#GEN contains new data, different from Advance, I'm creating a new cursor with same Individuals as 
#indivuals files. It is a complement to the others 2 cursors.-

if importGenotypesFlag == "Y": 

     print " "
     print "Import Genotypes? : Y..."

     print "Executing SQL query 3..."
     cursor3 = conn.cursor()
     cursor3.execute("""
               SELECT distinct I.id
               FROM "public"."person" I
               LEFT JOIN (
                    SELECT B.person_id
                    FROM "public"."batches" B
                    GROUP BY B.person_id
                    ORDER BY B.person_id ASC
               ) B ON I.id = B.person_id
               ORDER BY I.id ASC
                          """)
     
     f3 = cursor3.fetchone()
 
     regex =  r"^SNP.*{0}$".format(file_type_oxford)
     SNPfilename =  utils.find_file_in_list(sourcesFileList, regex)

     if SNPfilename == None:
           utils.log(logger, "SNP file not found")
     else:
          while f3:
               current_id = f3[0]
               regex =  r"^IND_[0-9]+_{0}_.*{1}$".format(current_id,file_type_oxford)

               INDfilename =  utils.find_file_in_list(sourcesFileList, regex)

               if INDfilename == None:
                  utils.log(logger, "ID: "+str(current_id) +" -Not found in GEN file")
               else:
                                            

                    filename = "{0}{1}/Individuals_{1}_v{2}_Genotypes.tsv".format(output_dir, current_id, config.imputed_genotype_version)
                    print "Writing " + filename

                    INDfile = open(filename, "a")

                    INDfile.write("personId,genotypeVersion,sample,letter,rs,chromosome,start,end,ref,alt,allele_a,allele_b,genotypeLikelihoods0,genotypeLikelihoods1,genotypeLikelihoods2\n")

                    with open(sourcesDirectory+SNPfilename,'rb') as SFile,open(sourcesDirectory+INDfilename,'rb') as IFile:
                         for x, y in zip(SFile,IFile):

                              SNPcolumns=x.split()  
                              INDcolumns=y.split()

                              li =  utils.to_prepared_list((str(current_id),config.imputed_genotype_version,SNPcolumns[2],SNPcolumns[3],SNPcolumns[4],str(int(SNPcolumns[4])+len(SNPcolumns[5])),SNPcolumns[5],SNPcolumns[6],INDcolumns[2],INDcolumns[3],INDcolumns[4]))
                              INDfile.write("""{0},{1},,,{3},{2},{4},{5},{6},{7},{6},{7},{8},{9},{10}\n""".format(*li)) 
  
                    INDfile.close()
               f3 = cursor3.fetchone()


###############
### Batches ###
###############
print "Exporting Batches"
utils.log(logger, "Writing <<Batches>> file")
utils.log(logger, "------------------------")

with open(output_dir + "Batches.tsv", "w") as file:
     file.write("batchId,batchDate,batchType,description,studyName\n")

     # First do the batch table
     cursor.execute("""SELECT B.id, B.batch_date, B.description FROM "public"."batch" AS B""")
     f = cursor.fetchone()
     while f:
          li = utils.to_prepared_list(f)
          file.write("""{0},{1},ADVANCE-ON,{2},Genotyping Batch\n""".format(*li))
          f = cursor.fetchone()

     # Then do the samples table
     cursor.execute("""SELECT B.id, B.sample_date, B.description FROM "public"."samples" AS B""")
     f = cursor.fetchone()
     while f:
          li = utils.to_prepared_list(f)
          file.write("""S{0},{1},ADVANCE-ON,{2},Enrollment Batch\n""".format(*li)) # Note the "S" before the Sample ID
          f = cursor.fetchone()
print "-------------------------------"
print 'Migrated Individuals: '+str(migratedindividuals)
utils.log(logger, 'Migrated Individuals: '+str(migratedindividuals))
print "Done"



