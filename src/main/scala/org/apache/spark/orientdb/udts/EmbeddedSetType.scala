package org.apache.spark.orientdb.udts

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

@SQLUserDefinedType(udt = classOf[EmbeddedSetType])
case class EmbeddedSet(elements: Array[Any]) extends Serializable {
  override def hashCode(): Int = {
    var hashCode = 1
    val i = elements.iterator
    while (i.hasNext) {
      val obj = i.next()

      val elemValue = if (obj == null) 0 else obj.hashCode()
      hashCode = 31 * hashCode + elemValue
    }
    hashCode
  }

  override def equals(other: scala.Any): Boolean = other match {
    case that: EmbeddedSet => that.elements.sameElements(this.elements)
    case _ => false
  }

  override def toString: String = elements.mkString(", ")
}

class EmbeddedSetType extends UserDefinedType[EmbeddedSet] {

  override def sqlType: DataType = ArrayType(StringType)

  override def serialize(obj: EmbeddedSet): Any = {
    new GenericArrayData(obj.elements.map{elem =>
      val out = new ByteArrayOutputStream()
      val os = new ObjectOutputStream(out)
      os.writeObject(elem)
      UTF8String.fromBytes(out.toByteArray)
    })
  }

  override def deserialize(datum: Any): EmbeddedSet = {
    datum match {
      case values: ArrayData =>
        new EmbeddedSet(values.toArray[UTF8String](StringType).map{ elem =>
          val in = new ByteArrayInputStream(elem.getBytes)
          val is = new ObjectInputStream(in)
          is.readObject()
        })
      case other => sys.error(s"Cannot deserialize $other")
    }
  }

  override def userClass: Class[EmbeddedSet] = classOf[EmbeddedSet]
}

object EmbeddedSetType extends EmbeddedSetType