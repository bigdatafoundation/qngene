import com.opencsv._
import java.io._
import scala.util.Try
import scala.collection.JavaConversions._
import org.bdgenomics.formats.avro._
import org.slf4j.LoggerFactory

class DirectoryHandler(prefix: String, genotypeVersion: String, assemblyName: String,
  batchData: collection.mutable.HashMap[String,Batch], dateFormat: java.text.SimpleDateFormat) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  // Number of columns in the Individual.csv file. Everything past that number is a batch ID
  val NB_INDIVIDUAL_COLUMNS = 11
  // Extract the type of file
  val filePattern = """^Individuals_([0-9]+)(_(.*))?\.tsv$""".r
  // Extract the Genotype version number
  //val genotypePattern = """^Individuals_([0-9]+)_(v[0-9]+)_Genotypes\.tsv$""".r
  val genotypePattern = """^Individuals_([0-9]+)_(v[0-9]+i*)_Genotypes\.tsv$""".r

  // Run a function without having to care if it succeeds or fails
  def ignore(str:String, possibleFailure: () => Unit) =
    try {
      possibleFailure()
    } catch {
      case e : Throwable =>
        //logger.warn(str+" - "+e.getMessage) commented out because each warning is an object. No enough memory to support them all
	None
    }

  // This is the main function in this file
  def processIndividual(id: String) : Option[Individual] = {
    // List of files in the sub-folder
    val ls = new java.io.File(prefix + id).listFiles.map(file => file.getName).sorted
    // Structure to hold incomplete data
    val record = IndividualRecord()

    ls.foreach({ fileName =>
      filePattern.findAllIn(fileName).matchData foreach { m =>

        // Needs an interator or it'll run out of RAM
        val reader = new CSVIterator(new CSVReader(new FileReader(prefix + id + "/" + fileName)))
        reader.next() // Skip headers

        m.group(3) match {
          // The current file is for the Visits
          case ("Visits") =>
            reader.foreach { row =>
              val visit = Visit.newBuilder()
              visit.setVisitId(row(0))
              ignore(id+"-"+"setVisitDateEpoch",() => visit.setVisitDateEpoch(dateFormat.parse(row(1)).getTime))
              visit.setStudyId(row(2))
              visit.setIsFasting(Try(java.lang.Boolean.valueOf(row(3))).getOrElse(false))
              visit.setDescription(row(4))
              record.visits = visit :: record.visits // Each row in the csv adds a visit to the list
            }
          // The current file is for the Phenotypes
          case ("Phenotypes") =>
            reader.foreach { row =>
              val phenotype = Phenotype.newBuilder()
              phenotype.setPhenotypeType(row(1))
              phenotype.setPhenotypeGroup(row(2))
              phenotype.setName(row(3))
           
              //begin DMA
              //ignore(() => phenotype.setMeasureDataType(MeasureDataType.valueOf(row(4))))
	      if(row(4) == "") None// phenotype.setMeasureDataType(MeasureDataType.valueOf(Unknown))
	      else ignore(id+"-"+"setMeasureDataType",() => phenotype.setMeasureDataType(MeasureDataType.valueOf(row(4))))
	      //end DMA

              phenotype.setMeasure(row(5))
              phenotype.setMeasureUnits(row(6))
              phenotype.setDescription(row(7))
	      //begin DMA
	      if(row(8) =="") None
              else ignore(id+"-"+"setDiagnosisDateEpoch",() => phenotype.setDiagnosisDateEpoch(dateFormat.parse(row(8)).getTime))
             //end DMA
              record.phenotypes = phenotype :: record.phenotypes // Each row in the csv adds a phenotype to the list
            }
          // The current file is for the Individual
          case (null) =>
            reader.foreach { row =>

              val gender = if (Character.toLowerCase(row(5).charAt(0)) == 'm') Gender.Male
                else if (Character.toLowerCase(row(5).charAt(0)) == 'f') Gender.Female
                else Gender.Unknown
              val individual = Individual.newBuilder()
              individual.setGender(gender)
              individual.setIndividualId(row(0))
              individual.setFamilyId(row(1))
              individual.setFatherId(row(2))
              individual.setMotherId(row(3))
              ignore(id+"-"+"setBirthDateEpoch",() => individual.setBirthDateEpoch(dateFormat.parse(row(4)).getTime))
              individual.setEthnicCode(row(6))
              individual.setCentreName(row(7))
              individual.setRegion(row(8))
              individual.setCountry(row(9))
              individual.setNotes(row(10))

              //dma
              if(row(11) == "") None
	      else ignore(id+"-"+"setMissingCallFreq",() =>individual.setMissingCallFreq(java.lang.Float.parseFloat(row(11))))
              //dma 

              // Ignore the first NB_INDIVIDUAL_COLUMNS
              // Map the remaining columns (batch IDs) with the HashMap of batch data by ID
              // Flatten: List[Option[T]] -> List[T]
              // Flatten handles both when fields are null and when the batch ID can't be mapped
              val listBatches = row.toList.drop(NB_INDIVIDUAL_COLUMNS).map({batchId =>
                if (batchId == "") None else batchData.get(batchId)
              }).flatten
              individual.setBatches(listBatches)
              record.individual = Some(individual)
            }
          // The current file is for the Genotypes
          case (_) =>
            // Extract the genotype version number
            genotypePattern.findAllIn(fileName).matchData foreach { n =>
              // We only want to continue when it's the right version
              if (n.group(2) == genotypeVersion) reader.foreach { row =>
                val contig = Contig.newBuilder()
                contig.setContigName(row(5)) // chromosome
                contig.setAssembly(assemblyName)
                contig.setSpecies("human")

                // DBSNP data
                val variant = Variant.newBuilder()
                variant.setContig(contig.build())
                ignore(id+"-"+"setStart",() => variant.setStart(java.lang.Long.parseLong(row(6))))
                ignore(id+"-"+"setEnd",() => variant.setEnd(java.lang.Long.parseLong(row(7))))
                variant.setReferenceAllele(row(8))
                variant.setAlternateAllele(row(9))

                val alleles: List[GenotypeAllele] = List() // List of GenotypeAllele Ref, Alt, OtherAlt, NoCall
              
                val genotype = Genotype.newBuilder()

                genotype.setVariant(variant.build())
                // genotype.setSampleId(row(2)) // NULL for now. Leaving this commented, as documentation
                genotype.setAlleles(alleles)


		//begin dma
                //val likelihoods: List[java.lang.Float] = List(row(12).toFloat,row(13).toFloat,row(14).toFloat) // List of Likelihoods
		if (row(12) == "" && row(13) == "" && row(14) == "") None
		else {	
			try {
			      val likelihoods: List[java.lang.Float] = List(row(12).toFloat,row(13).toFloat,row(14).toFloat) // List of Likelihoods
			      genotype.setGenotypeLikelihoods(likelihoods)
			    } catch {
			      case e : Throwable =>
				logger.warn(row(5) + " - " +e.getMessage)
			    }
		}

                genotype.setVariantIdentifier(row(4))

                //end dma

                record.genotypes = genotype :: record.genotypes // Each row in the csv adds a genotype to the list
              }
            }
        }
      }
    })

    // Only generate an ADAM object when all the data is there
    if (record.isReady) {
      Some(record.toWritable)
    } else {
      None
    }

  }
}
