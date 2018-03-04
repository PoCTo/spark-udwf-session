package com.nuvola_tech.spark

import java.util.UUID

import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.catalyst.expressions.{Add, AggregateWindowFunction, AttributeReference, Expression, If, IsNotNull, LessThanOrEqual, Literal, ScalaUDF, Subtract}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String



object MyUDWF {
  val defaultMaxSessionLengthms = 3600 * 1000
  case class SessionUDWF(timestamp:Expression,
                         sessionWindow:Expression = Literal(defaultMaxSessionLengthms)) extends AggregateWindowFunction {
    self: Product =>

    override def children: Seq[Expression] = Seq(timestamp)
    override def dataType: DataType = IntegerType

    protected val zero_long = Literal( 0L )
    protected val zero_int = Literal( 0 )
    protected val one_int = Literal( 1 )
    protected val nullString = Literal(null:String)

    protected val currentSession = AttributeReference("currentSession", IntegerType, nullable = true)()
    protected val previousTs =    AttributeReference("lastTs", LongType, nullable = false)()

    override val aggBufferAttributes: Seq[AttributeReference] =  currentSession  :: previousTs :: Nil

    protected val assignSession =  If(LessThanOrEqual(Subtract(timestamp, aggBufferAttributes(1)), sessionWindow),
      aggBufferAttributes(0), // if
      Add(aggBufferAttributes(0), one_int))

    override val initialValues: Seq[Expression] =  zero_int :: zero_long :: Nil
    override val updateExpressions: Seq[Expression] =
        assignSession ::
        timestamp ::
        Nil

    override val evaluateExpression: Expression = aggBufferAttributes(0)
    override def prettyName: String = "makeSession"
  }

  def calculateSession(ts:Column): Column = withExpr { SessionUDWF(ts.expr, Literal(defaultMaxSessionLengthms)) }
  def calculateSession(ts:Column, sessionWindow:Column): Column = withExpr { SessionUDWF(ts.expr, sessionWindow.expr) }

  private def withExpr(expr: Expression): Column = new Column(expr)
}
