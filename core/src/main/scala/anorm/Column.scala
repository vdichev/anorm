/*
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package anorm

import java.io.{ ByteArrayInputStream, InputStream }
import java.math.{ BigDecimal => JBigDec, BigInteger }
import java.util.{ Date, UUID }
import java.sql.Timestamp

import scala.language.reflectiveCalls
import scala.util.{ Failure, Success => TrySuccess, Try }

import resource.managed

import scala.language.reflectiveCalls

/** Column mapping */
@annotation.implicitNotFound(
  "No column extractor found for the type ${A}: `anorm.Column[${A}]` required; See https://github.com/playframework/anorm/blob/master/docs/manual/working/scalaGuide/main/sql/ScalaAnorm.md#column-parsers")
trait Column[A] extends ((Any, MetaDataItem) => MayErr[SqlRequestError, A]) {
  def validate(meta: MetaDataItem): MayErr[SqlRequestError, Unit]
}

/** Column companion, providing default conversions. */
object Column extends JodaColumn with JavaTimeColumn {

  @deprecated(message = "Use [[nonNull]]", since = "2.5.1")
  def nonNull1[A](transformer: Column[A]): Column[A] = nonNull[A](transformer)

  /**
   * Helper function to implement column conversion.
   *
   * @param transformer Function converting raw value of column
   * @tparam Output type
   */
  def nonNull[A](transformer: Column[A]): Column[A] = new Column[A] {
    def apply(value: Any, meta: MetaDataItem) = {
      if (value != null) transformer(value, meta)
      else MayErr(Left[SqlRequestError, A](
        UnexpectedNullableFound(meta.column.toString)))
    }

    def validate(meta: MetaDataItem) = if (meta.nullable) {
      MayErr(Left(UnexpectedNullableFound(meta.column.toString)))
    } else transformer.validate(meta)
  }

  @inline private[anorm] def className(that: Any): String =
    if (that == null) "<null>" else that.getClass.getName

  implicit val columnToString: Column[String] = nonNull {
    new Column[String] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case string: String => Right(string)
          case clob: java.sql.Clob => Right(
            clob.getSubString(1, clob.length.asInstanceOf[Int]))

          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to String for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.lang.String" | "java.sql.Clob" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to String for column $meta.column"))
      }
    }
  }

  /**
   * Column conversion to bytes array.
   *
   * {{{
   * import anorm.SqlParser.scalar
   * import anorm.Column.columnToByteArray
   *
   * val bytes: Array[Byte] = SQL("SELECT bin FROM tbl").
   *   as(scalar[Array[Byte]].single)
   * }}}
   */
  implicit val columnToByteArray: Column[Array[Byte]] = nonNull {
    new Column[Array[Byte]] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case bytes: Array[Byte] => Right(bytes)
          case stream: InputStream => streamBytes(stream)
          case string: String => Right(string.getBytes)
          case blob: java.sql.Blob => streamBytes(blob.getBinaryStream)
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to bytes array for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.lang.String" | "java.sql.Blob" | "byte[]" | "[B" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to bytes array for column $meta.column"))
      }
    }
  }

  /**
   * Column conversion to character.
   *
   * {{{
   * import anorm.SqlParser.scalar
   * import anorm.Column.columnToChar
   *
   * val c: Char = SQL("SELECT char FROM tbl").as(scalar[Char].single)
   * }}}
   */
  implicit val columnToChar: Column[Char] = nonNull {
    new Column[Char] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case string: String => Right(string.charAt(0))
          case clob: java.sql.Clob => Right(clob.getSubString(1, 1).charAt(0))
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Char for column $column"))
        }
      }
      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.lang.String" | "java.sql.Clob" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Char for column $meta.column"))
      }
    }
  }

  implicit val columnToInt: Column[Int] = nonNull {
    new Column[Int] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case bi: BigInteger => Right(bi.intValue)
          case bd: JBigDec => Right(bd.intValue)
          case l: Long => Right(l.toInt)
          case i: Int => Right(i)
          case s: Short => Right(s.toInt)
          case b: Byte => Right(b.toInt)
          case bool: Boolean => Right(if (!bool) 0 else 1)
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Int for column $column"))
        }
      }
      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.math.BigInteger" | "java.math.BigDecimal" | "java.lang.Long" | "java.lang.Integer" | "java.lang.Short" | "java.lang.Byte" | "java.lang.Boolean" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Int for column $meta.column"))
      }
    }
  }

  /**
   * Column conversion to bytes array.
   *
   * {{{
   * import anorm.SqlParser.scalar
   * import anorm.Column.columnToInputStream
   *
   * val bytes: InputStream = SQL("SELECT bin FROM tbl").
   *   as(scalar[InputStream].single)
   * }}}
   */
  implicit val columnToInputStream: Column[InputStream] = nonNull {
    new Column[InputStream] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case bytes: Array[Byte] => Right(new ByteArrayInputStream(bytes))
          case stream: InputStream => Right(stream)
          case string: String => Right(new ByteArrayInputStream(string.getBytes))
          case blob: java.sql.Blob => Right(blob.getBinaryStream)
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to input stream for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.lang.String" | "java.sql.Blob" | "byte[]" | "[B" | "java.io.InputStream" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to input stream for column $meta.column"))
      }
    }
  }

  implicit val columnToFloat: Column[Float] = nonNull {
    new Column[Float] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case f: Float => Right(f)
          case bi: BigInteger => Right(bi.floatValue)
          case i: Int => Right(i.toFloat)
          case s: Short => Right(s.toFloat)
          case b: Byte => Right(b.toFloat)
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Float for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.math.BigInteger" | "java.lang.Integer" | "java.lang.Short" | "java.lang.Byte" | "java.lang.Float" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Float for column $meta.column"))
      }
    }
  }

  implicit val columnToDouble: Column[Double] = nonNull {
    new Column[Double] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case bg: JBigDec => Right(bg.doubleValue)
          case d: Double => Right(d)
          case f: Float => Right(new JBigDec(f.toString).doubleValue)
          case bi: BigInteger => Right(bi.doubleValue)
          case i: Int => Right(i.toDouble)
          case s: Short => Right(s.toDouble)
          case b: Byte => Right(b.toDouble)
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Double for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.math.BigDecimal" | "java.math.BigInteger" | "java.lang.Integer" | "java.lang.Short" | "java.lang.Byte" | "java.lang.Float" | "java.lang.Double" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Float for column $meta.column"))
      }
    }
  }

  implicit val columnToShort: Column[Short] = nonNull {
    new Column[Short] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case b: Byte => Right(b.toShort)
          case s: Short => Right(s)
          case bool: Boolean => Right(if (!bool) 0.toShort else 1.toShort)
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Short for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.lang.Byte" | "java.sql.Short" | "java.lang.Boolean" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Short for column $meta.column"))
      }
    }
  }

  implicit val columnToByte: Column[Byte] = nonNull {
    new Column[Byte] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case b: Byte => Right(b)
          case s: Short => Right(s.toByte)
          case bool: Boolean => Right(if (!bool) 0.toByte else 1.toByte)
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Byte for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.lang.Byte" | "java.sql.Short" | "java.lang.Boolean" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Byte for column $meta.column"))
      }
    }
  }

  implicit val columnToBoolean: Column[Boolean] = nonNull {
    new Column[Boolean] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case bool: Boolean => Right(bool)
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Boolean for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.lang.Boolean" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Boolean for column $meta.column"))
      }
    }
  }

  private[anorm] def timestamp[T](ts: Timestamp)(f: Timestamp => T): Either[SqlRequestError, T] = Right(if (ts == null) null.asInstanceOf[T] else f(ts))

  implicit val columnToLong: Column[Long] = nonNull {
    new Column[Long] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case bi: BigInteger => Right(bi.longValue)
          case bd: JBigDec => Right(bd.longValue)
          case int: Int => Right(int: Long)
          case long: Long => Right(long)
          case s: Short => Right(s.toLong)
          case b: Byte => Right(b.toLong)
          case bool: Boolean => Right(if (!bool) 0L else 1L)
          case date: Date => Right(date.getTime)
          case TimestampWrapper1(ts) => timestamp(ts)(_.getTime)
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Long for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.math.BigInteger" | "java.math.BigDecimal" | "java.lang.Long" | "java.lang.Integer" | "java.lang.Short" | "java.lang.Byte" | "java.lang.Boolean" | "java.util.Date" | "java.sql.Timestamp" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Long for column $meta.column"))
      }
    }
  }

  // Used to convert Java or Scala big integer
  private def anyToBigInteger(value: Any, meta: MetaDataItem): Either[SqlRequestError, BigInteger] = {
    import meta._
    value match {
      case bi: BigInteger => Right(bi)
      case bd: JBigDec => Right(bd.toBigInteger)
      case long: Long => Right(BigInteger.valueOf(long))
      case int: Int => Right(BigInteger.valueOf(int))
      case s: Short => Right(BigInteger.valueOf(s))
      case b: Byte => Right(BigInteger.valueOf(b))
      case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to BigInteger for column $column"))
    }
  }

  private def validateBigInteger(meta: MetaDataItem): Either[SqlRequestError, Unit] = meta.clazz match {
    case "java.math.BigInteger" | "java.math.BigDecimal" | "java.lang.Long" | "java.lang.Integer" | "java.lang.Short" | "java.lang.Byte" => Right(())
    case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to BigInteger for column $meta.column"))
  }

  /**
   * Column conversion to Java big integer.
   *
   * {{{
   * import anorm.SqlParser.scalar
   * import anorm.Column.columnToScalaBigInteger
   *
   * val c: BigInteger =
   *   SQL("SELECT COUNT(*) FROM tbl").as(scalar[BigInteger].single)
   * }}}
   */
  implicit val columnToBigInteger: Column[BigInteger] = nonNull {
    new Column[BigInteger] {
      def apply(value: Any, meta: MetaDataItem) = anyToBigInteger(value, meta)

      def validate(meta: MetaDataItem) = validateBigInteger(meta)
    }
  }

  /**
   * Column conversion to big integer.
   *
   * {{{
   * import anorm.SqlParser.scalar
   * import anorm.Column.columnToBigInt
   *
   * val c: BigInt =
   *   SQL("SELECT COUNT(*) FROM tbl").as(scalar[BigInt].single)
   * }}}
   */
  implicit val columnToBigInt: Column[BigInt] = nonNull {
    new Column[BigInt] {
      def apply(value: Any, meta: MetaDataItem) = MayErr(anyToBigInteger(value, meta).right.map(BigInt(_)))

      def validate(meta: MetaDataItem) = validateBigInteger(meta)
    }
  }

  implicit val columnToUUID: Column[UUID] = nonNull {
    new Column[UUID] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case d: UUID => Right(d)
          case s: String => Try { UUID.fromString(s) } match {
            case TrySuccess(v) => Right(v)
            case Failure(ex) => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to UUID for column $column"))
          }
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to UUID for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.util.UUID" | "java.lang.String" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to UUID for column $meta.column"))
      }
    }
  }

  // Used to convert Java or Scala big decimal
  private def anyToBigDecimal(value: Any, meta: MetaDataItem): Either[SqlRequestError, JBigDec] = {
    import meta._
    value match {
      case bd: JBigDec => Right(bd)
      case bi: BigInteger => Right(new JBigDec(bi))
      case d: Double => Right(JBigDec.valueOf(d))
      case f: Float => Right(JBigDec.valueOf(f))
      case l: Long => Right(JBigDec.valueOf(l))
      case i: Int => Right(JBigDec.valueOf(i))
      case s: Short => Right(JBigDec.valueOf(s))
      case b: Byte => Right(JBigDec.valueOf(b))
      case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to BigDecimal for column $column"))
    }
  }

  private def validateBigDecimal(meta: MetaDataItem): Either[SqlRequestError, Unit] = meta.clazz match {
    case "java.math.BigInteger" | "java.math.BigDecimal" | "java.lang.Long" | "java.lang.Integer" | "java.lang.Short" | "java.lang.Byte" | "java.lang.Double" | "java.lang.Float" => Right(())
    case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to BigDecimal for column $meta.column"))
  }

  /**
   * Column conversion to Java big decimal.
   *
   * {{{
   * import java.math.{ BigDecimal => JBigDecimal }
   * import anorm.SqlParser.scalar
   * import anorm.Column.columnToJavaBigDecimal
   *
   * val c: JBigDecimal =
   *   SQL("SELECT COUNT(*) FROM tbl").as(scalar[JBigDecimal].single)
   * }}}
   */
  implicit val columnToJavaBigDecimal: Column[JBigDec] = nonNull {
    new Column[JBigDec] {
      def apply(value: Any, meta: MetaDataItem) = anyToBigDecimal(value, meta)

      def validate(meta: MetaDataItem) = validateBigDecimal(meta)
    }
  }

  /**
   * Column conversion to big decimal.
   *
   * {{{
   * import anorm.SqlParser.scalar
   * import anorm.Column.columnToScalaBigDecimal
   *
   * val c: BigDecimal =
   *   SQL("SELECT COUNT(*) FROM tbl").as(scalar[BigDecimal].single)
   * }}}
   */
  implicit val columnToScalaBigDecimal: Column[BigDecimal] = nonNull {
    new Column[BigDecimal] {
      def apply(value: Any, meta: MetaDataItem) = anyToBigDecimal(value, meta).right.map(BigDecimal(_))

      def validate(meta: MetaDataItem) = validateBigDecimal(meta)
    }
  }

  /**
   * Parses column as Java Date.
   * Time zone offset is the one of default JVM time zone
   * (see [[java.util.TimeZone#getDefault TimeZone.getDefault]]).
   *
   * {{{
   * import java.util.Date
   *
   * val d: Date = SQL("SELECT last_mod FROM tbl").as(scalar[Date].single)
   * }}}
   */
  implicit val columnToDate: Column[Date] = nonNull {
    new Column[Date] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case date: Date => Right(date)
          case time: Long => Right(new Date(time))
          case TimestampWrapper1(ts) => timestamp(ts)(t => new Date(t.getTime))
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Date for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.util.Date" | "java.lang.Long" | "java.sql.Timestamp" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Date for column $meta.column"))
      }
    }
  }

  implicit def columnToOption[T](implicit transformer: Column[T]): Column[Option[T]] = new Column[Option[T]] {
    def apply(value: Any, meta: MetaDataItem) = {
      if (value != null) transformer(value, meta).map(Some(_))
      else MayErr(Right[SqlRequestError, Option[T]](None))
    }
    def validate(meta: MetaDataItem) = transformer.validate(meta.copy(nullable = true))
  }

  /**
   * Parses column as array.
   *
   * val a: Array[String] =
   *   SQL"SELECT str_arr FROM tbl".as(scalar[Array[String]])
   * }}}
   */
  implicit def columnToArray[T](implicit transformer: Column[T], t: scala.reflect.ClassTag[T]): Column[Array[T]] = Column.nonNull {
    new Column[Array[T]] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._

        @inline def typeNotMatch(value: Any, target: String, cause: Any) = TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to $target for column $column: $cause")

        @annotation.tailrec
        def transf(a: Array[_], p: Array[T]): Either[SqlRequestError, Array[T]] =
          a.headOption match {
            case Some(r) => transformer(r, meta).toEither match {
              case Right(v) => transf(a.tail, p :+ v)
              case Left(cause) => Left(typeNotMatch(value, "array", cause))
            }
            case _ => Right(p)
          }

        @annotation.tailrec
        def jiter(i: java.util.Iterator[_], p: Array[T]): Either[SqlRequestError, Array[T]] = if (!i.hasNext) Right(p)
        else transformer(i.next, meta).toEither match {
          case Right(v) => jiter(i, p :+ v)
          case Left(cause) => Left(typeNotMatch(value, "list", cause))
        }

        value match {
          case sql: java.sql.Array => try {
            transf(sql.getArray.asInstanceOf[Array[_]], Array.empty[T])
          } catch {
            case cause: Throwable => Left(typeNotMatch(value, "array", cause))
          }

          case arr: Array[_] => try {
            transf(arr, Array.empty[T])
          } catch {
            case cause: Throwable => Left(typeNotMatch(value, "list", cause))
          }

          case it: java.lang.Iterable[_] => try {
            jiter(it.iterator, Array.empty[T])
          } catch {
            case cause: Throwable => Left(typeNotMatch(value, "list", cause))
          }

          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to array for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.sql.Array" | "java.lang.Iterable" => Right(())
        case arr if arr contains "[" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to array for column $meta.column"))
      }
    }
  }

  /**
   * Parses column as list.
   *
   * val a: List[String] =
   *   SQL"SELECT str_arr FROM tbl".as(scalar[List[String]])
   * }}}
   */
  implicit def columnToList[T](implicit transformer: Column[T], t: scala.reflect.ClassTag[T]): Column[List[T]] = Column.nonNull {
    new Column[List[T]] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._

        @inline def typeNotMatch(value: Any, target: String, cause: Any) = TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to $target for column $column: $cause")

        @annotation.tailrec
        def transf(a: Array[_], p: List[T]): Either[SqlRequestError, List[T]] =
          a.headOption match {
            case Some(r) => transformer(r, meta).toEither match {
              case Right(v) => transf(a.tail, p :+ v)
              case Left(cause) => Left(typeNotMatch(value, "list", cause))
            }
            case _ => Right(p)
          }

        @annotation.tailrec
        def jiter(i: java.util.Iterator[_], p: List[T]): Either[SqlRequestError, List[T]] = if (!i.hasNext) Right(p)
        else transformer(i.next, meta).toEither match {
          case Right(v) => jiter(i, p :+ v)
          case Left(cause) => Left(typeNotMatch(value, "list", cause))
        }

        value match {
          case sql: java.sql.Array => try {
            transf(sql.getArray.asInstanceOf[Array[_]], Nil)
          } catch {
            case cause: Throwable => Left(typeNotMatch(value, "list", cause))
          }

          case arr: Array[_] => try {
            transf(arr, Nil)
          } catch {
            case cause: Throwable => Left(typeNotMatch(value, "list", cause))
          }

          case it: java.lang.Iterable[_] => try {
            jiter(it.iterator, Nil)
          } catch {
            case cause: Throwable => Left(typeNotMatch(value, "list", cause))
          }

          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to list for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.sql.Array" | "java.lang.Iterable" => Right(())
        case arr if arr contains "[" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to list for column $meta.column"))
      }
    }
  }

  @inline private def streamBytes(in: InputStream): Either[SqlRequestError, Array[Byte]] = managed(in).acquireFor(streamToBytes(_)).fold({ errs =>
    Left(SqlMappingError(errs.headOption.
      fold("Fails to read binary stream")(_.getMessage)))
  }, Right(_))

  @annotation.tailrec
  private def streamToBytes(in: InputStream, bytes: Array[Byte] = Array(), buffer: Array[Byte] = Array.ofDim(1024)): Array[Byte] = {
    val count = in.read(buffer)

    if (count == -1) bytes
    else streamToBytes(in, bytes ++ buffer.take(count), buffer)
  }
}

sealed trait JodaColumn {
  import org.joda.time.{ DateTime, LocalDate, LocalDateTime, Instant }
  import Column.{ nonNull, className, timestamp => Ts }

  /**
   * Parses column as Joda local date.
   * Time zone is the one of default JVM time zone
   * (see [[org.joda.time.DateTimeZone#getDefault DateTimeZone.getDefault]]).
   *
   * {{{
   * import org.joda.time.LocalDate
   *
   * val i: LocalDate = SQL("SELECT last_mod FROM tbl").
   *   as(scalar[LocalDate].single)
   * }}}
   */
  implicit val columnToJodaLocalDate: Column[LocalDate] = nonNull {
    new Column[LocalDate] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._

        value match {
          case date: java.util.Date => Right(new LocalDate(date.getTime))
          case time: Long => Right(new LocalDate(time))
          case TimestampWrapper1(ts) => Ts(ts)(t => new LocalDate(t.getTime))
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Joda LocalDate for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.util.Date" | "java.lang.Long" | "java.sql.Timestamp" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Joda LocalDate for column $meta.column"))
      }
    }
  }

  /**
   * Parses column as Joda local date/time.
   * Time zone is the one of default JVM time zone
   * (see [[org.joda.time.DateTimeZone#getDefault DateTimeZone.getDefault]]).
   *
   * {{{
   * import org.joda.time.LocalDateTime
   *
   * val i: LocalDateTime = SQL("SELECT last_mod FROM tbl").
   *   as(scalar[LocalDateTime].single)
   * }}}
   */
  implicit val columnToJodaLocalDateTime: Column[LocalDateTime] = nonNull {
    new Column[LocalDateTime] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._

        value match {
          case date: java.util.Date => Right(new LocalDateTime(date.getTime))
          case time: Long => Right(new LocalDateTime(time))
          case TimestampWrapper1(ts) => Ts(ts)(t => new LocalDateTime(t.getTime))
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Joda LocalDateTime for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.util.Date" | "java.lang.Long" | "java.sql.Timestamp" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Joda LocalDateTime for column $meta.column"))
      }
    }
  }

  /**
   * Parses column as joda DateTime
   *
   * {{{
   * import org.joda.time.DateTime
   *
   * val d: Date = SQL("SELECT last_mod FROM tbl").as(scalar[DateTime].single)
   * }}}
   */
  implicit val columnToJodaDateTime: Column[DateTime] = nonNull {
    new Column[DateTime] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case date: Date => Right(new DateTime(date.getTime))
          case time: Long => Right(new DateTime(time))
          case TimestampWrapper1(ts) =>
            Option(ts).fold(Right(null.asInstanceOf[DateTime]))(t =>
              Right(new DateTime(t.getTime)))

          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to DateTime for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.util.Date" | "java.lang.Long" | "java.sql.Timestamp" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to DateTime for column $meta.column"))
      }
    }
  }

  /**
   * Parses column as joda Instant
   *
   * {{{
   * import org.joda.time.Instant
   *
   * val d: Date = SQL("SELECT last_mod FROM tbl").as(scalar[Instant].single)
   * }}}
   */
  implicit val columnToJodaInstant: Column[Instant] = nonNull {
    new Column[Instant] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case date: Date => Right(new Instant(date.getTime))
          case time: Long => Right(new Instant(time))
          case TimestampWrapper1(ts) => Ts(ts)(t => new Instant(t.getTime))
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Instant for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.util.Date" | "java.lang.Long" | "java.sql.Timestamp" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Instant for column $meta.column"))
      }
    }
  }
}

sealed trait JavaTimeColumn {
  import java.time.{ ZonedDateTime, ZoneId, LocalDate, LocalDateTime, Instant }
  import Column.{ nonNull, className, timestamp => Ts }

  /**
   * Parses column as Java8 instant.
   * Time zone offset is the one of default JVM time zone
   * (see [[java.time.ZoneId#systemDefault ZoneId.systemDefault]]).
   *
   * {{{
   * import java.time.Instant
   * import anorm.Java8._
   *
   * val i: Instant = SQL("SELECT last_mod FROM tbl").as(scalar[Instant].single)
   * }}}
   */
  implicit val columnToInstant: Column[Instant] = nonNull {
    new Column[Instant] {
      def apply(value: Any, meta: MetaDataItem) = {
        import meta._
        value match {
          case date: java.util.Date => Right(Instant ofEpochMilli date.getTime)
          case time: Long => Right(Instant ofEpochMilli time)
          case TimestampWrapper1(ts) => Ts(ts)(Instant ofEpochMilli _.getTime)
          case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Java8 Instant for column $column"))
        }
      }

      def validate(meta: MetaDataItem) = meta.clazz match {
        case "java.util.Date" | "java.lang.Long" | "java.sql.Timestamp" => Right(())
        case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Java8 Instant for column $meta.column"))
      }
    }
  }

  /**
   * Parses column as Java8 local date/time.
   * Time zone offset is the one of default JVM time zone
   * (see [[java.time.ZoneId#systemDefault ZoneId.systemDefault]]).
   *
   * {{{
   * import java.time.LocalDateTime
   * import anorm.Java8._
   *
   * val i: LocalDateTime = SQL("SELECT last_mod FROM tbl").
   *   as(scalar[LocalDateTime].single)
   * }}}
   */
  implicit val columnToLocalDateTime: Column[LocalDateTime] = {
    @inline def dateTime(ts: Long) = LocalDateTime.ofInstant(
      Instant.ofEpochMilli(ts), ZoneId.systemDefault)

    nonNull {
      new Column[LocalDateTime] {
        def apply(value: Any, meta: MetaDataItem) = {
          import meta._
          value match {
            case date: java.util.Date => Right(dateTime(date.getTime))
            case time: Long => Right(dateTime(time))
            case TimestampWrapper1(ts) => Ts(ts)(t => dateTime(t.getTime))

            case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Java8 LocalDateTime for column $column"))
          }
        }

        def validate(meta: MetaDataItem) = meta.clazz match {
          case "java.util.Date" | "java.lang.Long" | "java.sql.Timestamp" => Right(())
          case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Java8 LocalDateTime for column $meta.column"))
        }
      }
    }
  }

  /**
   * Parses column as Java8 local date.
   * Time zone offset is the one of default JVM time zone
   * (see [[java.time.ZoneId#systemDefault ZoneId.systemDefault]]).
   *
   * {{{
   * import java.time.LocalDateTime
   * import anorm.Java8._
   *
   * val i: LocalDateTime = SQL("SELECT last_mod FROM tbl").
   *   as(scalar[LocalDateTime].single)
   * }}}
   */
  implicit val columnToLocalDate: Column[LocalDate] = {
    @inline def localDate(ts: Long) = LocalDateTime.ofInstant(
      Instant.ofEpochMilli(ts), ZoneId.systemDefault).toLocalDate

    nonNull {
      new Column[LocalDate] {
        def apply(value: Any, meta: MetaDataItem) = {
          import meta._
          value match {
            case date: java.util.Date => Right(localDate(date.getTime))
            case time: Long => Right(localDate(time))
            case TimestampWrapper1(ts) => Ts(ts)(t => localDate(t.getTime))
            case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Java8 LocalDate for column $column"))
          }
        }

        def validate(meta: MetaDataItem) = meta.clazz match {
          case "java.util.Date" | "java.lang.Long" | "java.sql.Timestamp" => Right(())
          case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Java8 LocalDate for column $meta.column"))
        }
      }
    }
  }

  /**
   * Parses column as Java8 zoned date/time.
   * Time zone offset is the one of default JVM time zone
   * (see [[java.time.ZoneId#systemDefault ZoneId.systemDefault]]).
   *
   * {{{
   * import java.time.ZonedDateTime
   * import anorm.Java8._
   *
   * val i: ZonedDateTime = SQL("SELECT last_mod FROM tbl").
   *   as(scalar[ZonedDateTime].single)
   * }}}
   */
  implicit val columnToZonedDateTime: Column[ZonedDateTime] = {
    @inline def dateTime(ts: Long) = ZonedDateTime.ofInstant(
      Instant.ofEpochMilli(ts), ZoneId.systemDefault)

    nonNull {
      new Column[ZonedDateTime] {
        def apply(value: Any, meta: MetaDataItem) = {
          import meta._
          value match {
            case date: java.util.Date => Right(dateTime(date.getTime))
            case time: Long => Right(dateTime(time))
            case TimestampWrapper1(ts) => Ts(ts)(t => dateTime(t.getTime))
            case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: ${className(value)} to Java8 ZonedDateTime for column $column"))
          }
        }

        def validate(meta: MetaDataItem) = meta.clazz match {
          case "java.util.Date" | "java.lang.Long" | "java.sql.Timestamp" => Right(())
          case _ => Left(TypeDoesNotMatch(s"Cannot convert: ${meta.clazz} to Java8 ZonedDateTime for column $meta.column"))
        }
      }
    }
  }
}
