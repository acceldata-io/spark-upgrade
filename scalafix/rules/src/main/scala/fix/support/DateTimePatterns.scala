package fix.support

/**
 * What Spark 3.5.5 does differently from 2.4 with a datetime pattern: 2.4 used
 * SimpleDateFormat, 3.x DateTimeFormatter through
 * DateTimeFormatterHelper.convertIncompatiblePattern. A port of pyspark-rules'
 * `datetime_rules.check_pattern`, whose every claim was run on 3.5.5
 * (36 patterns through to_timestamp and date_format), so the Scala and Java rules judge a pattern as PySpark does.
 */
object DateTimePatterns {

  // Letters SimpleDateFormat (2.4) accepted that Spark 3.x refuses. `e`, `c`,
  // `A`, `B`, `n`, `N`, `p`, `q`, `Q` are refused too, but SimpleDateFormat
  // never accepted them either, so a 2.4 job cannot have relied on them.
  private val weekBased: Seq[(Char, String)] = Seq(
    'Y' -> "the week-based year -- almost always meant as `y`, the calendar year",
    'w' -> "week of the week-based year -- use weekofyear() instead",
    'W' -> "week of month -- no 3.x pattern letter means this",
    'u' -> "day number of the week in 2.4 (1 = Monday) -- use dayofweek() or `E`"
  )
  private val parsingOnly: Seq[(Char, String)] = Seq(
    'E' -> "the day-of-week name",
    'F' -> "the week of month (2.4) / aligned day-of-week-in-month (3.x)"
  )
  // Five-letter text forms are "narrow" in DateTimeFormatter, full in 2.4:
  // Spark refuses them rather than change the output silently. Seven `y`s overflow.
  private val tooLong = Seq("GGGGG", "MMMMM", "LLLLL", "EEEEE", "yyyyyyy")
  // DateTimeFormatter's own letter-count limits, for letters SimpleDateFormat
  // padded to any width.
  private val maxRun: Map[Char, Int] =
    Map('a' -> 1, 'h' -> 2, 'H' -> 2, 'k' -> 2, 'K' -> 2, 'm' -> 2, 's' -> 2, 'd' -> 2, 'D' -> 3, 'F' -> 1, 'z' -> 4, 'Z' -> 5)

  private val run = """([A-Za-z])\1*|[^A-Za-z]""".r

  /** Runs of one repeated letter outside quoted text, the way both formatters
   *  tokenise: `yyyy-MM-dd'T'HH` -> yyyy, -, MM, -, dd, HH. */
  private def runs(pattern: String): Seq[String] =
    pattern.split("'", -1).toSeq.zipWithIndex.collect { case (part, i) if i % 2 == 0 => part }
      .flatMap(part => run.findAllIn(part).toSeq)

  /** Why Spark 3.5.5 treats `pattern` differently from 2.4, or None. `parsing`
   *  is true for to_date/to_timestamp/unix_timestamp, false for
   *  date_format/from_unixtime. */
  def check(pattern: String, parsing: Boolean): Option[String] = {
    val rs = runs(pattern)
    val letters = rs.filter(_.head.isLetter).map(_.head).toSet
    weekBased.collectFirst { case (c, what) if letters(c) =>
      s"uses '$c', $what. Spark 3.0+ rejects week-based pattern letters outright, so the call fails"
    }.orElse(if (!parsing) None else parsingOnly.collectFirst { case (c, what) if letters(c) =>
      s"uses '$c' ($what) to parse. Spark 3.0+ can format with it but refuses to parse with it"
    }).orElse(tooLong.find(style => rs.exists(_.contains(style))).map { style =>
      s"uses '$style'. Spark 3.0+ refuses this length (its meaning changed in DateTimeFormatter)"
    }).orElse(rs.collectFirst { case r if maxRun.get(r.head).exists(r.length > _) =>
      s"repeats '${r.head}' ${r.length} times. 2.4's SimpleDateFormat padded to any width; Spark 3.x's " +
        s"DateTimeFormatter allows at most ${maxRun(r.head)}, so the pattern is rejected"
    }).orElse(Seq("#", "{", "}").find(rs.contains).map { c =>
      s"contains '$c', reserved by Spark 3.x's DateTimeFormatter, so the pattern is rejected"
    }).orElse(if (rs.contains("[") || rs.contains("]"))
      Some("contains '[' or ']', literal text in 2.4 but an optional section in Spark 3.x") else None
    ).orElse(if (!parsing && letters('F'))
      Some("uses 'F', which meant week-of-month in 2.4 and means aligned-day-of-week-in-month from 3.0 -- same pattern, different result")
    else None
    ).orElse(if (parsing && letters('h') && !letters('a'))
      Some("parses the 12-hour clock 'h' with no 'a' (am/pm) marker. 2.4 parsed 00-23 leniently; Spark 3.x returns NULL for any hour outside 1-12")
    else None)
  }
}
