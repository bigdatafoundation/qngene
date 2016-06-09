import java.io.File
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.bdgenomics.formats.avro._
import org.slf4j.LoggerFactory
import parquet.avro.AvroParquetWriter

object AppContext {
  val conf = new SparkConf().setAppName("Adam-IBS").setMaster("local")
  val sc = new SparkContext(conf)
  // val sqc = new SQLContext(sc)
  // sqc.sql("SET spark.sql.parquet.binaryAsString=true")
  var logger = LoggerFactory.getLogger(this.getClass)
  logger.info("Load AppContext")
}

/**
 * Created by ikizema on 15-07-28.
 */
class ParquetWriter[T](data:scala.collection.mutable.Buffer[T], filename:String, avroSchemaInput:Schema) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val DATA_PATH = "./"
  private val fileName = filename
  private val avroSchema = avroSchemaInput
  private val conf = AppContext.conf
  private val sc = AppContext.sc
  // private val sqc = new SQLContext(sc)
  private val dataToPersist = data

  def persistData() {
    val parquetFilePath = initialiseParqurFile()
    writeToFile(parquetFilePath)
  }

  def writeToFile(parquetFilePath:Path) {
    val parquetWriter = new AvroParquetWriter[IndexedRecord](parquetFilePath, avroSchema)
    for (itemNb <- 0 to this.dataToPersist.length-1) {
      parquetWriter.write(this.dataToPersist(itemNb).asInstanceOf[IndexedRecord])
    }
    parquetWriter.close()
  }

  def initialiseParqurFile() : Path= {
    val parquetFilePath:Path = new Path(DATA_PATH+this.fileName+".parquet")
    deleteIfExist(parquetFilePath.getName());
    return parquetFilePath;
  }

  /**
   * This function delete the file on the disk if it exist.
   */
  def deleteIfExist(fileName:String) {
    var fileTemp = new File(DATA_PATH+fileName);
    if (fileTemp.exists()) {
      fileTemp.delete();
    }
  }

}
