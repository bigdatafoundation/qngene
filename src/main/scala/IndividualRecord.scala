import scala.collection.JavaConversions._
import org.bdgenomics.formats.avro._

// This file simply holds data until we've processed all the files in the sub-folder
// We can't predict which files will be there or what order they'll be coming in
// This file provides some safety
class IndividualRecord {
  var individual: Option[Individual.Builder] = _
  var visits: List[Visit.Builder] = _
  var phenotypes: List[Phenotype.Builder] = _
  var genotypes: List[Genotype.Builder] = _
  def isReady() = {
    !(this.individual.isEmpty || this.visits.length == 0 ||
    this.phenotypes.length == 0 || this.genotypes.size == 0)
  }
  def toWritable() = {
    val listPhenotypes = this.phenotypes.map { p => p.build() }
    val listGenotypes = this.genotypes.map { g => g.build() }
    val listVisits = this.visits.map { visit =>
      visit.setPhenotypes(listPhenotypes)
      visit.build()
    }
    this.individual.get.setVisits(listVisits)
    this.individual.get.setGenotypes(listGenotypes)
    this.individual.get.build
  }
}

object IndividualRecord {
  def apply (): IndividualRecord = {
    var ind = new IndividualRecord
    ind.individual = None
    ind.visits = List()
    ind.phenotypes = List()
    ind.genotypes = List()
    ind
  }
}
