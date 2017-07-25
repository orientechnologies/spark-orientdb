package org.apache.spark.orientdb.udts

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

@SQLUserDefinedType(udt = classOf[EmbeddedListType])
case class EmbeddedList(elements: Array[Any]) extends Serializable {
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
    case that: EmbeddedList => that.elements.sameElements(this.elements)
    case _ => false
  }

  override def toString: String = elements.mkString(", ")
}

class EmbeddedListType extends UserDefinedType[EmbeddedList] {

  override def sqlType: DataType = ArrayType(StringType)

  override def serialize(obj: EmbeddedList): Any = {
    new GenericArrayData(obj.elements.map{elem =>
      val out = new ByteArrayOutputStream()
      val os = new ObjectOutputStream(out)
      os.writeObject(elem)
      UTF8String.fromBytes(out.toByteArray)
    })
  }

  override def deserialize(datum: Any): EmbeddedList = {
    datum match {
      case values: ArrayData =>
        new EmbeddedList(values.toArray[UTF8String](StringType).map{ elem =>
          val in = new ByteArrayInputStream(elem.getBytes)
          val is = new ObjectInputStream(in)
          is.readObject()
        })
      case other => sys.error(s"Cannot deserialize $other")
    }
  }

  override def userClass: Class[EmbeddedList] = classOf[EmbeddedList]
}

object EmbeddedListType extends EmbeddedListType