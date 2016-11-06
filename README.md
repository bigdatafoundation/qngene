How to execute the scripts in "Export_gen_files"
-----------------------
gentotsv.py (Converts .gen and .sample into tsv) 
-----------------------
python gentotsv.py -> (it takes the default sourcedir, destinationdir and sample file from config.py)

or

gentotsv.py  -h[help] -t[threads] -s[sourcedir] -d[destinationdir] -f[samplefile] -> (all the parameters are optionals)

------------------------
export.py (Generates individuals, visits, genotypes and phenotypes files from Advance, dbSNP and .gen/.sample data sources)
------------------------
python export.py {destination_des_tsv} {ip_postgresql_server} {schema_postgresql} {owner_schema_postresql} {password_owner_schema}{import_genotypes_flag}

Ex:
python export.py "/Test/output" 127.0.0.1 advancedb prognomix pass12345 Y -> . Flag "import_genotypes_flag" to Y takes only individuals from Advance DB with imputed genotypes (from .sample file) and genotypes from .gen files. Flag "import_genotypes_flag" to N takes all the invidivuals and gentoypes from Advance DB). 


[![Stories in Ready](https://badge.waffle.io/bigdatafoundation/qngene.png?label=ready&title=Ready)](https://waffle.io/bigdatafoundation/qngene)
# qngene

[![Join the chat at https://gitter.im/bigdatafoundation/qngene](https://badges.gitter.im/bigdatafoundation/qngene.svg)](https://gitter.im/bigdatafoundation/qngene?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
Genetic Scalable Analytical Query Engine

Contact GitHub API Training Shop Blog About
© 2016 GitHub, Inc. Terms Privacy Security Status Help
