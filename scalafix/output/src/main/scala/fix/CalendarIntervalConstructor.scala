package fix

import org.apache.spark.unsafe.types.CalendarInterval

object CalendarIntervalConstructor {
  // Rewritten: 3.x added a days field between months and microseconds.
  val oneMonthAnHour = new CalendarInterval(1, 0, 3600L * 1000 * 1000)
  def of(months: Int, micros: Long) = new org.apache.spark.unsafe.types.CalendarInterval(months, 0, micros)
}
