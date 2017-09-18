package org.apache.spark.orientdb.udts

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.orientechnologies.orient.core.id.ORecordId
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

@SQLUserDefinedType(udt = classOf[LinkBagType])
case class LinkBag(elements: Array[_ <: ORecordId]) extends Serializable {
  override def hashCode(): Int = {
    val hashCode = 1
    val elemValue = if (elements == null) 0 else elements.hashCode()

    31 * hashCode + elemValue
  }

  override def equals(other: scala.Any): Boolean = other match {
    case that: LinkBag => that.elements.sameElements(this.elements)
    case _ => false
  }

  override def toString: String = elements.toString
}

class LinkBagType extends UserDefinedType[LinkBag] {

  override def sqlType: DataType = ArrayType(StringType)

  override def serialize(obj: LinkBag): Any = {
    new GenericArrayData(obj.elements.map{ elem =>
      val out = new ByteArrayOutputStream()
      val os = new ObjectOutputStream(out)
      os.writeObject(elem)
      UTF8String.fromBytes(out.toByteArray)
    })
  }

  override def deserialize(datum: Any): LinkBag = {
    datum match {
      case values: ArrayData =>
        new LinkBag(values.toArray[UTF8String](StringType).map{ elem =>
          val in = new ByteArrayInputStream(elem.getBytes)
          val is = new ObjectInputStream(in)
          is.readObject().asInstanceOf[ORecordId]
        })
      case other => sys.error(s"Cannot deserialize $other")
    }
  }

  override def userClass: Class[LinkBag] = classOf[LinkBag]
}

object LinkBagType extends LinkBagType