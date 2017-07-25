package org.apache.spark.orientdb.udts

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.orientechnologies.orient.core.record.ORecord
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

@SQLUserDefinedType(udt = classOf[LinkListType])
case class LinkList(elements: Array[_ <: ORecord]) extends Serializable {
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
    case that: LinkList => that.elements.sameElements(this.elements)
    case _ => false
  }

  override def toString: String = elements.mkString(", ")
}

class LinkListType extends UserDefinedType[LinkList] {

  override def sqlType: DataType = ArrayType(StringType)

  override def serialize(obj: LinkList): Any = {
    new GenericArrayData(obj.elements.map{ elem =>
      val out = new ByteArrayOutputStream()
      val os = new ObjectOutputStream(out)
      os.writeObject(elem)
      UTF8String.fromBytes(out.toByteArray)
    })
  }

  override def deserialize(datum: Any): LinkList = {
    datum match {
      case values: ArrayData =>
        new LinkList(values.toArray[UTF8String](StringType).map{ elem =>
          val in = new ByteArrayInputStream(elem.getBytes)
          val is = new ObjectInputStream(in)
          is.readObject().asInstanceOf[ORecord]
        })
      case other => sys.error(s"Cannot deserialize $other")
    }
  }

  override def userClass: Class[LinkList] = classOf[LinkList]
}

object LinkListType extends LinkListType