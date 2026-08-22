/*
rule = UserDefinedAggregateFunctionDetect
 */
package fix

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

class MyAvgUDAF extends UserDefinedAggregateFunction {// assert: UserDefinedAggregateFunctionDetect
  def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil)
  def bufferSchema: StructType = StructType(StructField("sum", DoubleType) :: StructField("count", LongType) :: Nil)
  def dataType: DataType = DoubleType
  def deterministic: Boolean = true
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 0L
  }
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) + input.getDouble(0)
    buffer(1) = buffer.getLong(1) + 1
  }
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }
  def evaluate(buffer: Row): Any = buffer.getDouble(0) / buffer.getLong(1)
}

class PlainClass {
  def noop(): Unit = ()
}
