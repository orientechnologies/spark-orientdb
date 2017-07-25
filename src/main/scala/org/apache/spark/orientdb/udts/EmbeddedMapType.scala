package org.apache.spark.orientdb.udts

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.sql.catalyst.expressions.UnsafeMapData
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

@SQLUserDefinedType(udt = classOf[EmbeddedMapType])
case class EmbeddedMap(elements: Map[Any, Any]) extends Serializable {
  override def hashCode(): Int = 1

  override def equals(other: scala.Any): Boolean = other match {
    case that: EmbeddedMap => that.elements == this.elements
    case _ => false
  }

  override def toString: String = elements.mkString(", ")
}

class EmbeddedMapType extends UserDefinedType[EmbeddedMap] {

  override def sqlType: DataType = MapType(StringType, StringType)

  override def serialize(obj: EmbeddedMap): Any = {
    ArrayBasedMapData(obj.elements.keySet.map{ elem =>
      val outKey = new ByteArrayOutputStream()
      val osKey = new ObjectOutputStream(outKey)
      osKey.writeObject(elem)
      UTF8String.fromBytes(outKey.toByteArray)
    }.toArray,
      obj.elements.values.map{ elem =>
        val outValue = new ByteArrayOutputStream()
        val osValue = new ObjectOutputStream(outValue)
        osValue.writeObject(elem)
        UTF8String.fromBytes(outValue.toByteArray)
      }.toArray)
  }

  override def deserialize(datum: Any): EmbeddedMap = {
    datum match {
      case values: UnsafeMapData =>
        new EmbeddedMap(values.keyArray().toArray[UTF8String](StringType).map{ elem =>
          val in = new ByteArrayInputStream(elem.getBytes)
          val is = new ObjectInputStream(in)
          is.readObject()
        }.zip(values.valueArray().toArray[UTF8String](StringType).map{ elem =>
          val in = new ByteArrayInputStream(elem.getBytes)
          val is = new ObjectInputStream(in)
          is.readObject()
        }).toMap)
      case other => sys.error(s"Cannot deserialize $other")
    }
  }

  override def userClass: Class[EmbeddedMap] = classOf[EmbeddedMap]
}

object EmbeddedMapType extends EmbeddedMapType