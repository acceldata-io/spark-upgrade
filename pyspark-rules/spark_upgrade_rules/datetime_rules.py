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

from spark_upgrade_rules import _cst
from spark_upgrade_rules.detector import Detector

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


class AddMonthsSnapDetect(Detector):
    rule_id = "AddMonthsSnapDetect"

    def visit_Call(self, node: cst.Call) -> None:
        in_sql = any(re.search(r"(?i)\badd_months\s*\(", sql) for sql in self.sql_text_args(node))
        if self.function(node, "add_months") or in_sql:
            self._report(node, in_sql)

    def check_sql(self, node: cst.CSTNode, sql: str) -> None:
        if re.search(r"(?i)\badd_months\s*\(", sql):
            self._report(node, True)

    def _report(self, node: cst.CSTNode, in_sql: bool) -> None:
        self.report(node, "add_months(...)" + (" in SQL text" if in_sql else "") + ": Spark 3.0 no longer snaps the result "
                    "to the last day of the month when the input is a month-end date (2019-02-28 + 1 month is 2019-03-28, "
                    "not 2019-03-31). Silent, data-dependent; no config restores it.")


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
