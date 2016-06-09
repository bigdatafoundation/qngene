import com.opencsv._
import java.io._
import scala.collection.JavaConversions._
import org.bdgenomics.formats.avro._
import scala.collection.parallel._

object Importer {

  def main(args: Array[String]) = {

    if (args.size != 3) {
      println("Passed arguments were: " + args.mkString(", "))
      println("Arguments must be: genotypeVersion(v1, v2) assemblyName(NCI142) nbThreads")
      sys.exit(0)
    }
    val genotypeVersion = args(0)
    val assemblyName = args(1)
    val nbThreads = args(2).toInt

    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val prefix = "./output/"
    val batchData = new collection.mutable.HashMap[String,Batch]()

    // Warm up Spark
    val sc = AppContext.sc

    // Load the batches first into a HashMap[String, Batch]
    val reader = new CSVIterator(new CSVReader(new FileReader(prefix + "Batches.tsv")))
    reader.next() // Skip headers
    reader.foreach { row =>
      val batch = Batch.newBuilder()
      batch.setBatchDateEpoch(dateFormat.parse(row(1)).getTime)
      batch.setBatchType(row(2))
      batch.setDescription(row(3))
      batchData += row(0) -> batch.build() // Add to the HashMap
    }

    // Then process everything else
    val handler = new DirectoryHandler(prefix, genotypeVersion, assemblyName, batchData, dateFormat)
    // List of contents in the main output folder, sorted
    val list = new java.io.File(prefix).listFiles.sortWith(_.getName < _.getName).toList
    // Only keep directories (this is to exclude the Batches file we've already processed)
    val parallelList = list.filter(f => f.isDirectory).par
    // Make the list parallel, with the number of threads passed by argument
    parallelList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(nbThreads))
    // Process each folder in parallel
    val nbWritten = parallelList.map({ file =>
      val id = file.getName
      handler.processIndividual(id) match {
        // The Handler returned a valid ADAM object
        case (Some(writable)) => {
          // Serialize the ADAM object
          val buffer = scala.collection.mutable.Buffer[Individual](writable)
          // Write it to the disk
          val parquetFile = "./parquet/individual_" + genotypeVersion + "_" + id
          val parquetWriter = new ParquetWriter[Individual](buffer, parquetFile, Individual.getClassSchema)
          parquetWriter.persistData
          Some()
        }
        // That folder didn't have all the required data, no ADAM object was created
        case (None) => None
      }
    }).flatten

    // Disconnect cleanly to avoid errors
    sc.stop

    println(nbWritten.size + " individuals loaded")

  }
}
