"""Family D: datetime patterns, add_months, time-zone ids.

Ports of the Scala rules of the same ids. Each judgment reproduces what Spark
3.5.5 itself does, and every claim was run against a real 3.5.5 session
(PYSPARK.md SS8.12): the pattern checks are `DateTimeFormatterHelper.
convertIncompatiblePattern` plus the letter-count limits of the JDK's
`DateTimeFormatter`, and the time-zone check is `ZoneId.of(id, SHORT_IDS)`
after Spark's own `getZoneId` normalisation.
"""
from __future__ import annotations

import re
import zoneinfo

import libcst as cst
from libcst.helpers import get_full_name_for_node

from spark_upgrade_rules import _cst
from spark_upgrade_rules.detector import Detector, Fixer, NameDiscarded

# ---- datetime patterns --------------------------------------------------------

# Functions whose pattern parses input, and those whose pattern formats output.
# `unix_timestamp`'s and `from_unixtime`'s PySpark defaults are fine in 3.x.
PARSING = {"to_date": 1, "to_timestamp": 1, "unix_timestamp": 1, "to_unix_timestamp": 1}
FORMATTING = {"date_format": 1, "from_unixtime": 1}
PATTERN_OPTIONS = ("dateformat", "timestampformat")

# Letters SimpleDateFormat (2.4) accepted that Spark 3.x refuses. `e`, `c`, `A`,
# `B`, `n`, `N`, `p`, `q`, `Q` are refused too, but SimpleDateFormat never
# accepted them either, so a 2.4 job cannot have relied on them.
WEEK_BASED = {
    "Y": "the week-based year -- almost always meant as `y`, the calendar year",
    "w": "week of the week-based year -- use weekofyear() instead",
    "W": "week of month -- no 3.x pattern letter means this",
    "u": "day number of the week in 2.4 (1 = Monday) -- use dayofweek() or `E`",
}
PARSING_ONLY = {
    "E": "the day-of-week name",
    "F": "the week of month (2.4) / aligned day-of-week-in-month (3.x)",
}
# Five-letter text forms are "narrow" in DateTimeFormatter, full in 2.4: Spark
# refuses them rather than change the output silently. Seven `y`s overflow.
TOO_LONG = ("GGGGG", "MMMMM", "LLLLL", "EEEEE", "yyyyyyy")
# DateTimeFormatter's own letter-count limits, for letters SimpleDateFormat
# padded to any width. Over the limit, the JDK throws and Spark reports it as
# an upgrade failure.
MAX_RUN = {"a": 1, "h": 2, "H": 2, "k": 2, "K": 2, "m": 2, "s": 2, "d": 2, "D": 3, "F": 1, "z": 4, "Z": 5}


def _unquoted_runs(pattern: str) -> list[str]:
    """Runs of one repeated letter outside quoted text, the way both formatters
    tokenise: `yyyy-MM-dd'T'HH` -> ["yyyy", "MM", "dd", "HH"]. Non-letters are
    returned as one-character runs so `#`, `[` and `{` stay visible."""
    runs = []
    for index, part in enumerate(pattern.split("'")):
        if index % 2:
            continue
        for m in re.finditer(r"([A-Za-z])\1*|[^A-Za-z]", part):
            runs.append(m.group(0))
    return runs


def check_pattern(pattern: str, parsing: bool) -> tuple[str, str] | None:
    """(confidence, reason) for a pattern Spark 3.5.5 treats differently from
    2.4, or None. "high" means Spark 3.x throws on the pattern itself, whatever
    the data; "medium" means the same pattern gives a different result."""
    runs = _unquoted_runs(pattern)
    letters = {r[0] for r in runs if r[0].isalpha()}

    for c, what in WEEK_BASED.items():
        if c in letters:
            return ("high", f"uses '{c}', {what}. Spark 3.0+ rejects week-based pattern letters outright, so the call fails")
    if parsing:
        for c, what in PARSING_ONLY.items():
            if c in letters:
                return ("high", f"uses '{c}' ({what}) to parse. Spark 3.0+ can format with it but refuses to parse with it")
    for style in TOO_LONG:
        if any(style in r for r in runs):
            return ("high", f"uses '{style}'. Spark 3.0+ refuses this length (its meaning changed in DateTimeFormatter)")
    for r in runs:
        limit = MAX_RUN.get(r[0])
        if limit is not None and len(r) > limit:
            return ("high", f"repeats '{r[0]}' {len(r)} times. 2.4's SimpleDateFormat padded to any width; Spark 3.x's "
                    f"DateTimeFormatter allows at most {limit}, so the pattern is rejected")
    for c in ("#", "{", "}"):
        if c in runs:
            return ("high", f"contains '{c}', reserved by Spark 3.x's DateTimeFormatter, so the pattern is rejected")
    if "[" in runs or "]" in runs:
        return ("medium", "contains '[' or ']', literal text in 2.4 but an optional section in Spark 3.x")
    if not parsing and "F" in letters:
        return ("medium", "uses 'F', which meant week-of-month in 2.4 and means aligned-day-of-week-in-month from 3.0 -- "
                "same pattern, different result")
    if parsing and "h" in letters and "a" not in letters:
        return ("medium", "parses the 12-hour clock 'h' with no 'a' (am/pm) marker. 2.4 parsed 00-23 leniently; Spark 3.x "
                "returns NULL for any hour outside 1-12")
    return None


# A datetime function call inside SQL text, with its argument list.
_SQL_FUNCTION = re.compile(r"(?i)\b(" + "|".join([*PARSING, *FORMATTING]) + r")\s*\(")


def _sql_calls(sql: str) -> list[tuple[str, list[str]]]:
    """(function, top-level arguments) for each datetime function call in `sql`,
    found by matching parentheses outside quotes."""
    out = []
    for m in _SQL_FUNCTION.finditer(sql):
        depth, quote, start, args, i = 1, None, m.end(), [], m.end()
        while i < len(sql) and depth:
            ch = sql[i]
            if quote:
                if ch == quote:
                    quote = None
            elif ch in "'\"":
                quote = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 1:
                args.append(sql[start:i].strip())
                start = i + 1
            i += 1
        if depth == 0:
            args.append(sql[start:i - 1].strip())
            out.append((m.group(1).lower(), args))
    return out


def _sql_string(arg: str) -> str | None:
    m = re.fullmatch(r"'((?:[^']|'')*)'", arg)
    return m.group(1).replace("''", "'") if m else None


class DateTimeFormatPatternValidator(Detector):
    rule_id = "DateTimeFormatPatternValidator"

    def visit_Call(self, node: cst.Call) -> None:
        fn = self.function(node, *PARSING, *FORMATTING)
        if fn is not None:
            parsing = fn in PARSING
            pattern = self.string(_cst.argument(node, 1, "format"))
            if pattern is not None:
                self._check(node, pattern, parsing, f"{fn}(...)")
        self._check_options(node)
        super().visit_Call(node)

    def check_sql(self, node: cst.CSTNode, sql: str) -> None:
        for name, args in _sql_calls(sql):
            pattern = _sql_string(args[1]) if len(args) > 1 else None
            if pattern is not None:
                self._check(node, pattern, name in PARSING, f"{name}(...) in SQL text")

    def _check_options(self, node: cst.Call) -> None:
        name = _cst.method_name(node)
        if name not in ("csv", "json", "load", "save", "start", "table", "saveAsTable", "insertInto",
                        "from_json", "from_csv") and self.function(node, "from_json", "from_csv", "to_json", "to_csv") is None:
            return
        fn = self.function(node, "from_json", "from_csv", "to_json", "to_csv")
        if fn is not None:
            items = dict((k.lower(), v) for k, v in _cst.dict_items(_cst.argument(node, 2 if fn.startswith("from") else 1, "options")))
            parsing = fn.startswith("from")
        else:
            attrs = _cst.spine_attrs(node)
            if not attrs & {"read", "readStream", "write", "writeStream"}:
                return
            items = _cst.options_set(node, self.ctx.bindings)
            parsing = bool(attrs & {"read", "readStream"})
        for key in PATTERN_OPTIONS:
            pattern = self.string(items.get(key))
            if pattern is not None:
                self._check(node, pattern, parsing, f"the {key.replace('format', 'Format')} option")

    def _check(self, node: cst.Call, pattern: str, parsing: bool, where: str) -> None:
        verdict = check_pattern(pattern, parsing)
        if verdict is None:
            return
        confidence, reason = verdict
        self.report(node, f'Pattern "{pattern}" ({where}) {reason}. 2.4 used SimpleDateFormat; Spark 3.x uses '
                          "DateTimeFormatter. Correct the pattern (preferred), or set spark.sql.legacy.timeParserPolicy=LEGACY.",
                    confidence)


_ADD_MONTHS_SQL = re.compile(r"(?i)\badd_months\s*\(")
_SNAP = ("Spark 3.0 no longer snaps the result to the last day of the month when the input is a month-end date "
         "(2019-02-28 + 1 month is 2019-03-28, not 2019-03-31). Silent, data-dependent; no config restores it.")
# The functions the rewrite calls beside add_months itself.
_SNAP_HELPERS = ("last_day", "to_date", "when")


class AddMonthsSnapDetect(NameDiscarded, Fixer):
    """`add_months(d, n)`: 2.4 returned the last day of the target month when
    `d` was the last day of its month; 3.0 does not. Rewritten to
    `when(to_date(d) == last_day(d), last_day(add_months(d, n))).otherwise(add_months(d, n))`
    -- probed on 3.5.5 against 2.4.8 for dates, timestamps, strings, Feb 29,
    ordinary days and null, n = 1 and -1: the same values. The rewrite renames
    an unaliased result column, so it is made only where the generated name
    is discarded: under .alias/.name, as withColumn's value, or in a
    filter/where, orderBy/sort or join condition. The rewrite repeats
    `d` and `n`, so only where repeating them cannot change what they are: `d`
    a column name, `col("x")`, `df.x`, `df["x"]`, a plain name, or
    `to_date`/`to_timestamp` of one; `n` an int literal or a plain name. Other
    calls, named outputs, and add_months in SQL text, are reported as
    `AddMonthsSnapDetect.Manual`."""

    rule_id = "AddMonthsSnapDetect"
    MANUAL = "AddMonthsSnapDetect.Manual"

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        # A bare `add_months(...)` needs the helpers as bare names too: already
        # imported from pyspark.sql.functions (or by its `*`), or added to the
        # first module-level import from it -- unless the file binds the name
        # to something else.
        self.import_index: int | None = None
        imported: set[str] = set()
        for i, stmt in enumerate(ctx.module.body):
            for small in stmt.body if isinstance(stmt, cst.SimpleStatementLine) else ():
                if isinstance(small, cst.ImportFrom) and small.module is not None and \
                        get_full_name_for_node(small.module) == _cst.FUNCTIONS and not isinstance(small.names, cst.ImportStar):
                    if self.import_index is None:
                        self.import_index = i
                    imported.update(a.name.value for a in small.names if a.asname is None and isinstance(a.name, cst.Name))
        bound = ctx.bindings.count
        self.bare_ok = all((h in imported and bound.get(h) == 1) or
                           (h not in bound and (ctx.star_functions or self.import_index is not None)) for h in _SNAP_HELPERS)
        self.to_import = [] if ctx.star_functions else [h for h in _SNAP_HELPERS if h not in bound]
        self.bare_rewritten = False

    def visit_Call(self, node: cst.Call) -> None:
        if any(_ADD_MONTHS_SQL.search(sql) for sql in self.sql_text_args(node)):
            self._manual(node, "add_months(...) in SQL text")
        # The rewrite changes the generated name (add_months(d, 1) becomes
        # CASE WHEN ...), so it is made only where that name is discarded.
        self.mark_unnamed(node)

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.BaseExpression:
        if not self.function(original_node, "add_months"):
            return updated_node
        start, months = _cst.argument(original_node, 0, "start"), _cst.argument(original_node, 1, "months")
        func = original_node.func
        prefix = (self.ctx.module.code_for_node(func.value) + ".") if isinstance(func, cst.Attribute) else \
            ("" if self.bare_ok else None)
        if prefix is None or len(original_node.args) != 2 or _cst.has_star_args(original_node) or \
                not self._safe_date(start) or not self._safe_months(months):
            self._manual(original_node, "add_months(...) with arguments the rewrite cannot repeat safely (an expression "
                                        "other than a column, a name or to_date/to_timestamp of one)")
            return updated_node
        d, n = self.ctx.module.code_for_node(start), self.ctx.module.code_for_node(months)
        shifted = f"{self.ctx.module.code_for_node(func)}({d}, {n})"
        snap = f"{prefix}when({prefix}to_date({d}) == {prefix}last_day({d}), {prefix}last_day({shifted})).otherwise({shifted})"
        if not self.unnamed_depth:
            self.report(original_node, f"add_months(...) whose result keeps its generated column name: {_SNAP} The rewrite "
                                       "that restores 2.4's values would rename the column (add_months(...) becomes CASE "
                                       f"WHEN ...), so it is not made here. Use {snap}, with .alias(...) of the current "
                                       "name if anything reads the column by name.", tier=3, rule_id=self.MANUAL)
            return updated_node
        new = cst.parse_expression(snap)
        if not prefix:
            self.bare_rewritten = True
        return self.rewrite(original_node, new.with_changes(lpar=updated_node.lpar, rpar=updated_node.rpar),
                            f"add_months(...): {_SNAP} Rewritten to keep 2.4's result: where the input is the last day of "
                            "its month, the result is the last day of the target month. If the job never wanted the snap, "
                            "revert this change.")

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        for node, sql in self.ctx.sql_definitions:
            if _ADD_MONTHS_SQL.search(sql):
                self._manual(node, "add_months(...) in SQL text")
        if not (self.bare_rewritten and self.to_import):
            return updated_node
        stmt = updated_node.body[self.import_index]
        imp = next(s for s in stmt.body if isinstance(s, cst.ImportFrom) and not isinstance(s.names, cst.ImportStar)
                   and get_full_name_for_node(s.module) == _cst.FUNCTIONS)
        names = list(imp.names)
        trailing = names[-1].comma  # keep a trailing comma's layout for the added names
        added = [cst.ImportAlias(name=cst.Name(h), comma=trailing) for h in self.to_import]
        if not isinstance(trailing, cst.Comma):
            names[-1] = names[-1].with_changes(comma=cst.MaybeSentinel.DEFAULT)
        new_imp = imp.with_changes(names=names + added)
        body = list(updated_node.body)
        body[self.import_index] = stmt.with_changes(body=[new_imp if s is imp else s for s in stmt.body])
        return updated_node.with_changes(body=body)

    def _manual(self, node: cst.CSTNode, what: str) -> None:
        self.report(node, f"{what}: {_SNAP} Where the input can be a month-end date and 2.4's result is wanted, write "
                          "when(to_date(d) == last_day(d), last_day(add_months(d, n))).otherwise(add_months(d, n)).",
                    tier=3, rule_id=self.MANUAL)

    def _column(self, node: cst.BaseExpression | None) -> bool:
        """A column named without side effects: "x", col("x"), df.x, df["x"], or a plain name."""
        if _cst.string_value(node) is not None:
            return True
        if isinstance(node, cst.Name):
            return node.value not in ("True", "False", "None")
        if isinstance(node, cst.Attribute):
            return isinstance(node.value, cst.Name)
        if isinstance(node, cst.Subscript):
            return isinstance(node.value, cst.Name) and len(node.slice) == 1 and \
                isinstance(node.slice[0].slice, cst.Index) and _cst.string_value(node.slice[0].slice.value) is not None
        if isinstance(node, cst.Call) and self.function(node, "col", "column"):
            return len(node.args) == 1 and node.args[0].keyword is None and not node.args[0].star and \
                _cst.string_value(node.args[0].value) is not None
        return False

    def _safe_date(self, node: cst.BaseExpression | None) -> bool:
        if isinstance(node, cst.Call) and self.function(node, "to_date", "to_timestamp"):
            args = node.args
            return 1 <= len(args) <= 2 and all(a.keyword is None and not a.star for a in args) and \
                self._column(args[0].value) and (len(args) == 1 or _cst.string_value(args[1].value) is not None)
        return self._column(node)

    @staticmethod
    def _safe_months(node: cst.BaseExpression | None) -> bool:
        return _cst.int_value(node) is not None or (isinstance(node, cst.Name) and node.value not in ("True", "False", "None"))


# ---- time zones --------------------------------------------------------------

# java.time.ZoneId.SHORT_IDS, which Spark passes to ZoneId.of: these resolve.
SHORT_IDS = {
    "ACT", "AET", "AGT", "ART", "AST", "BET", "BST", "CAT", "CNT", "CST", "CTT", "EAT", "ECT", "EST", "HST", "IET",
    "IST", "JST", "MIT", "MST", "NET", "NST", "PLT", "PNT", "PRT", "PST", "SST", "VST",
}
_OFFSET = re.compile(r"[+-](\d{1,2})(?::?(\d{2})(?::?(\d{2}))?)?")
_REGION = re.compile(r"[A-Za-z][A-Za-z0-9~/._+-]+")


def _region_ids() -> frozenset[str]:
    try:
        return frozenset(zoneinfo.available_timezones())
    except Exception:  # pylint: disable=broad-except
        return frozenset()


REGIONS = _region_ids()


def _offset_ok(text: str) -> bool:
    m = _OFFSET.fullmatch(text)
    if not m:
        return False
    hours, minutes, seconds = int(m.group(1)), int(m.group(2) or 0), int(m.group(3) or 0)
    if len(m.group(1)) == 1 and m.group(2) and ":" not in text:
        return False  # "+530" is not a ZoneOffset form
    return hours <= 18 and minutes < 60 and seconds < 60 and (hours < 18 or minutes == seconds == 0)


def zone_resolves(tz: str) -> bool | None:
    """Whether Spark 3.5.5 resolves `tz`: True, False, or None when it is a
    region-shaped id and this interpreter has no time-zone database to check it
    against (so the rule stays silent rather than guess)."""
    tz = re.sub(r"([+-])(\d):", r"\g<1>0\2:", tz, count=1)
    tz = re.sub(r"([+-])(\d\d):(\d)$", r"\1\2:0\3", tz, count=1)
    if tz == "Z" or tz in SHORT_IDS:
        return True
    if tz[:1] in "+-":
        return _offset_ok(tz)
    for prefix in ("UTC", "GMT", "UT"):
        if tz == prefix:
            return True
        if tz.startswith(prefix) and tz[len(prefix):len(prefix) + 1] in ("+", "-"):
            return _offset_ok(tz[len(prefix):])
    if not _REGION.fullmatch(tz):
        return False
    if not REGIONS:
        return None
    return tz in REGIONS


SESSION_TZ = "spark.sql.session.timeZone"


class InvalidTimeZoneIdDetect(Detector):
    rule_id = "InvalidTimeZoneIdDetect"

    def visit_Call(self, node: cst.Call) -> None:
        fn = self.function(node, "from_utc_timestamp", "to_utc_timestamp")
        if fn is not None:
            self._check(node, self.string(_cst.argument(node, 1, "tz")), f"passed to {fn}")
        elif _cst.method_name(node) in ("config", "set", "setConf"):
            if self.string(_cst.argument(node, 0, "key")) == SESSION_TZ:
                self._check(node, self.string(_cst.argument(node, 1, "value")), f"set as {SESSION_TZ}")
        super().visit_Call(node)

    def check_sql(self, node: cst.CSTNode, sql: str) -> None:
        for m in re.finditer(r"(?i)\b(from_utc_timestamp|to_utc_timestamp)\s*\([^,]*,\s*'([^']*)'\s*\)", sql):
            self._check(node, m.group(2), f"passed to {m.group(1).lower()} in SQL text")
        for m in re.finditer(r"(?im)^\s*SET\s+(?:TIME\s+ZONE\s+'([^']*)'|spark\.sql\.session\.timeZone\s*=\s*(\S+))", sql):
            self._check(node, m.group(1) or m.group(2), "set in SQL text")

    def _check(self, node: cst.Call, tz: str | None, where: str) -> None:
        if tz is None or zone_resolves(tz) is not False:
            return
        self.report(node, f'Time-zone id "{tz}" ({where}) does not resolve with ZoneId.of(id, ZoneId.SHORT_IDS), which is how '
                          "Spark 3.x reads it. 2.4 silently fell back to GMT; 3.0+ throws -- and any data 2.4 produced here was "
                          "computed in GMT, not the intended zone. Use a region id (America/Los_Angeles) or an offset (+05:30).")


class DateTimeStrictParsingDetect(DateTimeFormatPatternValidator):
    """Parsing with a pattern 3.x reads the same way -- the case
    `DateTimeFormatPatternValidator` leaves alone -- is still exposed. 2.4's
    SimpleDateFormat parsed leniently; 3.x's parser is strict, and under the
    default `spark.sql.legacy.timeParserPolicy=EXCEPTION` a value the old
    parser would have read throws SparkUpgradeException. Probed on 3.5.5 with
    `yyyy-MM-dd`: '2020-1-5' (no zero padding), '2020-01-05 10:00:00'
    (trailing text) and '  2020-01-05' (leading space) all fail the job; under
    LEGACY each parses as in 2.4, under CORRECTED each is NULL. Whether a job
    is hit depends on its data, so every parsing call site is one repo-wide
    decision, at low confidence. Parsing without a pattern (`to_date(c)`, a
    CSV schema with no dateFormat) is lenient in 3.x too and is not flagged;
    `unix_timestamp(c)` parses with its default pattern and is."""

    rule_id = "DateTimeStrictParsingDetect"

    def visit_Call(self, node: cst.Call) -> None:
        fn = self.function(node, *PARSING)
        if fn is not None:
            arg = _cst.argument(node, 1, "format")
            if arg is not None:
                pattern = self.string(arg)
                if pattern is None:
                    self._strict(node, f"{fn}(...) with a pattern that is not a literal")
                else:
                    self._check(node, pattern, True, f"{fn}(...)")
            elif fn == "unix_timestamp" and _cst.argument(node, 0, "timestamp") is not None:
                self._strict(node, f'{fn}(...), which parses a string with its default "yyyy-MM-dd HH:mm:ss"')
        self._check_options(node)
        Detector.visit_Call(self, node)

    def _check(self, node: cst.Call, pattern: str, parsing: bool, where: str) -> None:
        if parsing and check_pattern(pattern, True) is None:
            self._strict(node, f'"{pattern}" ({where})')

    def _strict(self, node: cst.CSTNode, what: str) -> None:
        self.report(node, f"Parses with {what}. Spark 3.x's parser is strict where 2.4's was lenient: a value without zero "
                          "padding ('2020-1-5' for yyyy-MM-dd), with trailing text ('2020-01-05 10:00') or a leading space "
                          "throws SparkUpgradeException under the default spark.sql.legacy.timeParserPolicy=EXCEPTION. "
                          "spark.sql.legacy.timeParserPolicy=LEGACY parses exactly as 2.4 did.", "low")


# ---- a date or timestamp compared with a string ---------------------------------

_DATE_STRING = re.compile(r"\d{4}-\d{1,2}-\d{1,2}(?:[ T]\d{1,2}:\d{1,2}(?::\d{1,2}(?:\.\d{1,9})?)?)?")
_CANONICAL_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_OPERATORS = {cst.Equal: "==", cst.NotEqual: "!=", cst.LessThan: "<", cst.LessThanEqual: "<=",
              cst.GreaterThan: ">", cst.GreaterThanEqual: ">="}
_MIRRORED = {"<": ">", ">": "<", "<=": ">=", ">=": "<="}
# Calls whose arguments are column conditions: a comparison inside one is a Column's.
_CONDITIONS = ("filter", "where", "when")


def date_string_comparison_changes(literal: str, operator: str) -> bool:
    """The sqlfluff plugin's DATESTRCOMPARE_L013 test, for a column on the left
    of `operator` ("between_lo"/"between_hi" for a bound). Probed on 2.4.8 and
    3.5.5, SQL and the Column API alike: 2.4 compared a date with a string as
    strings, and a timestamp too except under ==/eqNullSafe; 3.x casts the
    string. A zero-padded 'yyyy-MM-dd' changes only a timestamp under > or <=
    (or a between upper bound); any other date-shaped string can change every
    operator on a date column. isin is unchanged."""
    if not _DATE_STRING.fullmatch(literal):
        return False
    return not _CANONICAL_DATE.fullmatch(literal) or operator in (">", "<=", "between_hi")


class DateTimeStringComparisonDetect(Detector):
    """Port of the Scala rule of the same id: a Column compared with a
    date-shaped string, directly, through `lit(...)` or a name bound once:
    `F.col("ts") > "2020-01-01"`, `.between(...)`, `.eqNullSafe(...)`.
    Spark 3.0 casts the string to the column's date or timestamp type; 2.4
    cast the column to a string. Only the literal/operator pairs that can
    change the answer are flagged (`date_string_comparison_changes`). The
    other side must be a Column: a `pyspark.sql.functions` call (and methods
    on one), or -- inside filter/where/when -- any expression, since a
    comparison there builds a Column. SQL text is the sqlfluff plugin's."""

    rule_id = "DateTimeStringComparisonDetect"

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        self._conditions = 0
        self._lambdas = 0

    def _is_condition(self, node: cst.Call) -> bool:
        return _cst.method_name(node) in _CONDITIONS or self.function(node, "when") is not None

    def _date_string(self, node: cst.BaseExpression) -> str | None:
        """A date-shaped string literal, bare or in lit(...), or a name bound once to one."""
        if isinstance(node, cst.Call) and self.function(node, "lit"):
            node = _cst.argument(node, 0, "col")
        text = self.string(node)
        return text if text is not None and _DATE_STRING.fullmatch(text) else None

    def _is_column(self, node: cst.BaseExpression) -> bool:
        # An RDD's filter(lambda r: r.d > "...") compares Python strings.
        if self._conditions and not self._lambdas:
            return True
        # A pyspark.sql.functions call, or a method chained on one.
        return any(isinstance(n, cst.Call) and (self.function(n, "col", "column") is not None or
                                                any(q.startswith(_cst.FUNCTIONS + ".") for q in self.qualified(n.func)))
                   for n in _cst.spine(node))

    def _check(self, node: cst.CSTNode, column: cst.BaseExpression, value: cst.BaseExpression | None, operator: str,
               column_known: bool = False) -> None:
        literal = self._date_string(value)
        if literal is None or not date_string_comparison_changes(literal, operator):
            return
        # lit(...) makes the comparison a Column's whatever the other side is.
        if not (column_known or isinstance(value, cst.Call) or self._is_column(column)):
            return
        self.report(node, f"A column compared with the string '{literal}'. If the column is a date or timestamp, Spark 3.x "
                          "casts the string to its type, where 2.4 cast the column to a string (except a timestamp under == "
                          "and eqNullSafe), so the rows selected can change silently: col('ts') > '2020-01-01' kept midnight "
                          "rows on 2.4 and drops them on 3.x; col('d') == '2020-1-1' matched nothing on 2.4. "
                          "spark.sql.legacy.typeCoercion.datetimeToString.enabled=true restores 2.4 and is injected (a date "
                          "partition column compared with a string is then no longer pruned). To drop it, compare with "
                          "to_date(lit(...))/to_timestamp(lit(...)), stating the comparison you mean.", "medium")

    def visit_Comparison(self, node: cst.Comparison) -> None:
        left = node.left
        for target in node.comparisons:
            operator = _OPERATORS.get(type(target.operator))
            if operator is not None:
                self._check(node, left, target.comparator, operator)
                self._check(node, target.comparator, left, _MIRRORED.get(operator, operator))
            left = target.comparator

    def visit_Call(self, node: cst.Call) -> None:
        name = _cst.method_name(node)
        receiver = _cst.receiver(node)
        if name == "between" and receiver is not None:
            self._check(node, receiver, _cst.argument(node, 0, "lowerBound"), "between_lo")
            self._check(node, receiver, _cst.argument(node, 1, "upperBound"), "between_hi")
        elif name == "eqNullSafe" and receiver is not None:
            self._check(node, receiver, _cst.argument(node, 0, "other"), "==", column_known=True)
        if self._is_condition(node):
            self._conditions += 1

    def leave_Call(self, original_node: cst.Call) -> None:
        if self._is_condition(original_node):
            self._conditions -= 1

    def visit_Lambda(self, node: cst.Lambda) -> None:
        self._lambdas += 1

    def leave_Lambda(self, original_node: cst.Lambda) -> None:
        self._lambdas -= 1
