package software.amazon.awssdk.services.dynamodb

import scala.jdk.CollectionConverters._

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.util.SdkAutoConstructList
import software.amazon.awssdk.core.util.SdkAutoConstructMap
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

object MockUtil {

  def sAttr(v: String): AttributeValue = {
    AttributeValue.builder().s(v).build()
  }

  def nAttr[T](v: T)(implicit n: Numeric[T]): AttributeValue = {
    AttributeValue.builder().n(v.toString).build()
  }

  def boolAttr(v: Boolean): AttributeValue = {
    AttributeValue.builder().bool(v).build()
  }

  def bAttr(v: Array[Byte]): AttributeValue = {
    AttributeValue.builder().b(SdkBytes.fromByteArray(v)).build()
  }

  def ssAttr(v: List[String]): AttributeValue = {
     AttributeValue.builder().ss(v.asJava).build()
  }

  def nsAttr[T](v: List[T])(implicit n: Numeric[T]): AttributeValue = {
     AttributeValue.builder().ns(v.map(_.toString).asJava).build()
  }

  def bsAttr(v: List[Array[Byte]]): AttributeValue = {
    AttributeValue.builder().bs(v.map(SdkBytes.fromByteArray).asJava).build()
  }

  def lAttr(v: List[AttributeValue]): AttributeValue = {
    AttributeValue.builder().l(v.asJava).build()
  }

  def mAttr(v: Map[String,AttributeValue]): AttributeValue = {
    AttributeValue.builder().m(v.asJava).build()
  }

  def attrType(attr: AttributeValue): String = {
    if (attr.s != null) "S"
    else if (attr.n != null) "N"
    else if (attr.bool != null) "BOOL"
    else if (attr.b != null) "B"
    else if (!attr.ss.isInstanceOf[SdkAutoConstructList[?]]) "SS"
    else if (!attr.ns.isInstanceOf[SdkAutoConstructList[?]]) "NS"
    else if (!attr.bs.isInstanceOf[SdkAutoConstructList[?]]) "BS"
    else if (!attr.l.isInstanceOf[SdkAutoConstructList[?]]) "L"
    else if (!attr.m.isInstanceOf[SdkAutoConstructMap[?,?]]) "M"
    else throw new IllegalArgumentException(s"Unknown attribute type [${attr}]")
  }

  def attrToAny(attr: AttributeValue): Any = {
    attrType(attr) match {
      case "S" => attr.s
      case "N" => {
        val n = attr.n
        if (n.contains('.')) n.toDouble
        else n.toLong
      }
      case "BOOL" =>  attr.bool
      case "B" => attr.b.asByteArray
      case "SS" =>  attr.ss.asScala.toSet
      case "NS" => {
        val ns = attr.ns.asScala.toSet
        if (ns.exists(_.contains('.'))) ns.map(_.toDouble)
        else ns.map(_.toLong)
      }
      case "BS" => attr.bs.asScala.map(_.asByteArray).toSet
      case "L" =>  attr.l.asScala.toList
      case "M" =>  attr.m.asScala.toMap
      case _ => throw new IllegalArgumentException(s"Unknown attribute type [${attr}]")
    }
  }

  def attrCompare(a: AttributeValue, b: AttributeValue): Int = {
    if (attrType(a) != attrType(b)) {
      throw new IllegalArgumentException(
        s"Can only compare AttributeValues of same type [${attrType(a)},${attrType(b)}]"
      )
    }

    attrType(a) match {
      case "S" => sCompare(a.s, b.s)
      case "N" => {
        if (a.n.contains('.')) nCompare(a.n.toDouble, b.n.toDouble)
        else nCompare(a.n.toLong, b.n.toLong)
      }
      case "BOOL" => boolCompare(a.bool, b.bool)
      case "B" => bCompare(a.b.asByteArray, b.b.asByteArray)
      case "SS" =>  ssCompare(a.ss.asScala.toSet, b.ss.asScala.toSet)
      case "NS" => {
        val as = a.ns.asScala.toSet
        val bs = b.ns.asScala.toSet
        if (as.exists(_.contains('.'))) {
          nsCompare(as.map(_.toDouble), bs.map(_.toDouble))
        }
        else nsCompare(as.map(_.toLong), bs.map(_.toLong))
      }
      case "BS" => bsCompare(
        a.bs.asScala.map(_.asByteArray).toSet, b.bs.asScala.map(_.asByteArray).toSet
      )
      case "L" =>  lCompare(a.l.asScala.toList, b.l.asScala.toList)
      case _ => throw new IllegalArgumentException(s"Unknown attribute type [${a}]")
    }
  }

  def sCompare(a: String, b: String): Int = {
    a.compare(b)
  }

  def nCompare[T](a: T, b: T)(implicit n: Numeric[T]): Int = {
    n.compare(a, b)
  }

  def boolCompare(a: Boolean, b: Boolean): Int = {
    a.compare(b)
  }

  def bCompare(a: Array[Byte], b: Array[Byte]): Int = {
    a.zip(b).foldLeft(0)((acc, v) => if (acc == 0) v._1.compare(v._2) else acc) match {
      case 0 => a.size.compare(b.size)
      case v => v
    }
  }

  def ssCompare(a: Set[String], b: Set[String]): Int = {
    val aList = a.toSeq.sorted
    val bList = b.toSeq.sorted
    aList.zip(bList).foldLeft(0)((acc, v) => if (acc == 0) v._1.compare(v._2) else acc) match {
      case 0 => a.size.compare(b.size)
      case v => v
    }
  }

  def nsCompare[T](a: Set[T], b: Set[T])(implicit n: Numeric[T]): Int = {
    val aList = a.toSeq.sortWith(n.compare(_, _) < 0)
    val bList = b.toSeq.sortWith(n.compare(_, _) < 0)
    aList.zip(bList).foldLeft(0)((acc, v) => if (acc == 0) n.compare(v._1, v._2) else acc) match {
      case 0 => a.size.compare(b.size)
      case v => v
    }
  }

  def bsCompare(a: Set[Array[Byte]], b: Set[Array[Byte]]): Int = {
    val aList = a.toSeq.sortWith(bCompare(_, _) < 0)
    val bList = b.toSeq.sortWith(bCompare(_, _) < 0)
    aList.zip(bList).foldLeft(0)((acc, v) => if (acc == 0) bCompare(v._1, v._2) else acc) match {
      case 0 => a.size.compare(b.size)
      case v => v
    }
  }

  def lCompare(a: List[AttributeValue], b: List[AttributeValue]): Int = {
    a.zip(b).foldLeft(0)((acc, v) => if (acc == 0) attrCompare(v._1, v._2) else acc) match {
      case 0 => a.size.compare(b.size)
      case v => v
    }
  }
}
