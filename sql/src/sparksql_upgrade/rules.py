"""Custom Spark SQL upgrade rules."""

import os.path
import re
from typing import Optional


from sqlfluff.core.config import ConfigLoader
from sqlfluff.core.parser import (
    KeywordSegment,
    SymbolSegment,
    WhitespaceSegment,
)
from sqlfluff.core.parser.segments.raw import CodeSegment
from sqlfluff.core.plugin import hookimpl
from sqlfluff.core.rules import (
    BaseRule,
    LintFix,
    LintResult,
    RuleContext,
)
from sqlfluff.core.rules.crawlers import SegmentSeekerCrawler
from sqlfluff.utils.functional import FunctionalContext, sp


@hookimpl
def load_default_config() -> dict:
    """Loads the default configuration for the plugin."""
    return ConfigLoader.get_global().load_config_file(
        file_dir=os.path.dirname(__file__),
        file_name="plugin_default_config.cfg",
    )


@hookimpl
def get_configs_info() -> dict:
    """Get rule config validations and descriptions."""
    return {
        "forbidden_columns": {"definition": "A list of column to forbid"},
    }


class Rule_SPARKSQLCAST_L001(BaseRule):
    """Spark 3.0 cast as int on strings will fail.

    Instead use the int() function.

    **Spark 2.4**

    Cast a string to an int

    .. code-block:: sql

        SELECT cast(foocount as int)
        FROM foo

    **Best practice**

    Use the int() function.

    .. code-block:: sql

        SELECT int(foocount)
        FROM foo
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"function"})
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Check integer casts."""
        functional_context = FunctionalContext(context)
        children = functional_context.segment.children()
        function_name_id_seg = (
            children.first(sp.is_type("function_name"))
            .children()
            .first(sp.is_type("function_name_identifier"))[0]
        )
        raw_function_name = function_name_id_seg.raw.upper().strip()
        function_name = raw_function_name.upper().strip()
        bracketed_segments = children.first(sp.is_type("bracketed"))
        if not bracketed_segments:
            return None
        bracketed = bracketed_segments[0]

        # Is this a cast function call
        if function_name == "CAST":
            print("Found cast function!")
            data_type_info = bracketed.get_child("data_type").raw.upper().strip()
            if data_type_info == "INT":
                # Here we know we have a possible one
                expr = bracketed.get_child("expression")
                print(f"Found expr {expr} - {expr.raw}")
                # Replace cast(X as int) with int(X) TODO
                return LintResult(
                    anchor=context.segment,
                    fixes=[
                        LintFix.replace(
                            function_name_id_seg,
                            [function_name_id_seg.edit(f"int({expr.raw})")],
                        ),
                        LintFix.delete(
                            bracketed,
                        ),
                    ],
                )

        return None


class Rule_FORMATSTRONEINDEX_L004(BaseRule):
    """Spark 3.3 Format strings are one indexed.


    Previously on JDK8 format strings were still one indexed, but zero was treated as one.
    On JDK17 an exception was thrown.

    The fix (%0$ -> %1$) is JDK 8's own reading, so values are exact. It also
    changes the format string inside the expression, and with it Spark's
    auto-generated column name (`format_string(%1$s, s)` against 2.4's
    `format_string(%0$s, s)`). So the fix is only offered where that name is
    discarded -- an aliased SELECT item, or outside the SELECT list (WHERE,
    ORDER BY, JOIN ON, ...). An unaliased SELECT item is flagged without a fix.
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"function"})
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Check for invalid format strs"""
        if _function_name(context) not in ("FORMAT_STRING", "PRINTF"):
            return None
        bracketed = context.segment.get_child("bracketed")
        first_arg = bracketed.get_child("expression") if bracketed else None
        format_str_seg = first_arg.get_child("quoted_literal") if first_arg else None
        # Only a literal format string can be read; `%0$` must be in it.
        if format_str_seg is None or "%0$" not in format_str_seg.raw:
            return None
        if _select_name_kept(context):
            return LintResult(anchor=context.segment)
        return LintResult(
            anchor=context.segment,
            fixes=[
                LintFix.replace(
                    format_str_seg,
                    [format_str_seg.edit(format_str_seg.raw.replace("%0$", "%1$"))],
                ),
            ],
        )


class Rule_NOCHARS_L003(BaseRule):
    """Spark 3.0 No longer supports CHAR type in non-Hive tables.

    In Spark 2.4 the CHAR type was treated as a String type anyways so we'll
    just rewrite all CHAR types to String types.
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"primitive_type"})
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Check for char types."""
        type_name = context.segment.raw.lower()
        if type_name.startswith("char"):
            return LintResult(
                anchor=context.segment,
                description=f"char type found ({type_name}) in {context} see "
                "https://archive.apache.org/dist/spark/docs/3.0.0/sql-migration-guide.html for migration"
                "advice. In Spark 2.4 on non-Hive tables these were treated as strings so "
                "rewritting to string.",
                fixes=[LintFix.replace(context.segment, [CodeSegment(raw="string")])],
            )
        else:
            return None


class Rule_RESERVEDROPERTIES_L002(BaseRule):
    """Spark 3.0 Reserves some table properties

    You can no longer set the provider, location, or owner property.
    For provider this is replaced with USING and location with LOCATION in the create.
    Sets after creation are not supported.
    Owner property is infered from running user.
    """

    groups = ("all",)
    # TODO -- Also look at SET calls once we fix SET DBPROPS in SQLFLUFF grammar.
    crawl_behaviour = SegmentSeekerCrawler({"property_name_identifier"})
    reserved = {"provider", "location", "owner"}
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Check for reserved properties being configured."""
        functional_context = FunctionalContext(context)
        property_name_segment = context.segment
        property_name = (
            property_name_segment.raw.lower()
            .strip()
            .lstrip('"')
            .rstrip('"')
            .lstrip("'")
            .rstrip("'")
        )
        print(f'Called with context {context} with "{property_name}"')
        if property_name not in self.reserved:
            print(f"Property: {property_name} is *ok*")
            return None
        else:
            # Reserved property found, lets check and see if we are in a "CREATE" which we can fix
            # or if we are in an "ALTER" which we can not automatically fix.
            create_or_alter_segment = context.parent_stack[-2]
            print(f"{dir(create_or_alter_segment)}")
            if create_or_alter_segment.is_type(
                "alter_database_statement"
            ) or create_or_alter_segment.is_type("alter_table_statement"):
                return LintResult(
                    anchor=context.segment,
                    description=f"Reserved table/db property {property_name} found in alter "
                    " statement see "
                    "https://archive.apache.org/dist/spark/docs/3.0.0/sql-migration-guide.html for migration "
                    " advice."
                    "In Spark 2.4 these alter statements were (effectively) ignored so you can "
                    " likely delete it, automatically "
                    f'rewritten to "legacy_{property_name}".',
                    fixes=[
                        LintFix.replace(
                            property_name_segment,
                            [
                                property_name_segment.get_child(
                                    "quoted_identifier"
                                ).edit(f'"legacy_{property_name}"')
                            ],
                        )
                    ],
                )
            # Ok we know it's a create statement since it is not an alter :)
            parent_segment = context.parent_stack[-1]
            # Now we want to get the segments that are "bad" (e.g. we want to delete) and that is
            # everything from this segment up until either a comma segment or a endbracket segment.
            segments_to_remove = []
            edits = []
            property_value = None
            siblings_post = functional_context.siblings_post
            segments_to_remove = [property_name_segment]
            for segment in siblings_post:
                # We want to keep the end bracket so check for it before adding to the list
                if segment.is_type("end_bracket"):
                    break
                segments_to_remove.append(segment)
                if segment.is_type("quoted_literal"):
                    property_value = segment.raw.strip().lstrip('"').rstrip('"')
                print(f"{segment} - {segment.get_type()}")
                # We want to drop the comma so we do the check _after_ the ops
                if segment.is_type("comma"):
                    break
            if len(parent_segment.get_children("property_name_identifier")) == 1:
                # If there are no other properties besides the property we are going to delete then
                # we need to drop the entire properties part to do this correctly, but "for now"
                # as a hack we will just edit the property name to prepend "legacy_"
                segments_to_remove = []
                edits = [
                    LintFix.replace(
                        property_name_segment,
                        [
                            property_name_segment.get_child("quoted_identifier").edit(
                                f'"legacy_{property_name}"'
                            )
                        ],
                    )
                ]
            deletes = map(lambda t: LintFix.delete(t), segments_to_remove)
            new_statement = None
            if property_name == "provider":
                new_segment = CodeSegment(raw=f" USING {property_value}")
                print(functional_context.parent_stack)
                create_table_segment = functional_context.parent_stack[-2]
                # We want to insert after the first bracketed segment containing column_definition
                # but if there are no column definitions we instead insert after the table
                # identifier.
                first_bracketed_segment = create_table_segment.get_child("bracketed")
                print(dir(first_bracketed_segment))
                if (
                    "column_definition"
                    in first_bracketed_segment.direct_descendant_type_set
                ):
                    new_statement = LintFix.create_after(
                        first_bracketed_segment,
                        [new_segment],
                    )
                else:
                    new_statement = LintFix.create_after(
                        create_table_segment.get_child("table_reference"),
                        [new_segment],
                    )
            elif property_name == "location":
                create_segment = functional_context.parent_stack[-2]
                # We want to insert after the database reference (and comment if present) or before
                # "TBLPROPERTIES" depending.
                if "database_reference" in create_segment.direct_descendant_type_set:
                    new_statement = LintFix.create_after(
                        create_segment.get_child("database_reference"),
                        [CodeSegment(raw=f' LOCATION "{property_value}"')],
                    )
                else:
                    keywords = create_segment.get_children("keyword")
                    tbl_properties_ref = next(
                        iter(filter(lambda s: s.raw_upper == "TBLPROPERTIES", keywords))
                    )
                    new_statement = LintFix.create_before(
                        tbl_properties_ref,
                        [CodeSegment(raw=f'LOCATION "{property_value}" ')],
                    )
            else:
                # For "owner" property we don't have an easy work around so instead just raise
                # a lint error.
                return LintResult(
                    anchor=context.segment,
                    description=f"Reserved table/db property {property_name} found see "
                    "https://archive.apache.org/dist/spark/docs/3.0.0/sql-migration-guide.html for "
                    "migration advice.",
                    fixes=None,
                )
            fixes = list(edits) + list(deletes) + [new_statement]
            return LintResult(
                anchor=context.segment,
                description="Reserved table property {property_name} found.",
                fixes=fixes,
            )
            # TODO - Make a rewrite rule.


class Rule_SPARKSQL_L004(BaseRule):
    """Spark 3.0 extract second field returns DecimalType

    Since Spark 3.0, when using EXTRACT expression to extract the
    second field from date/timestamp values, the result will be a
    DecimalType(8, 6) value with 2 digits for second part, and 6 digits
    for the fractional part with microsecond precision. e.g.
    extract(second from to_timestamp('2019-09-20 10:10:10.1')) results
    10.100000. In Spark version 2.4 and earlier, it returns an IntegerType
    value and the result for the former example is 10.

    The fix is `second(x)`: Spark 2.4's parser turned `extract(second FROM x)`
    into exactly that expression, so the value, the type (int) and the
    auto-generated column name (`second(x)`) are all 2.4's -- probed on 2.4.8
    and 3.5.5. The earlier `int(extract(...))` fix got the value right but
    named the column `extract(second FROM x)`; code already wrapped that way
    is left alone.
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"function"})
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        if _function_name(context) != "EXTRACT":
            return None
        bracketed = context.segment.get_child("bracketed")
        if not bracketed:
            return None
        date_part = bracketed.get_child("date_part")
        source = bracketed.get_child("expression")
        if date_part is None or source is None or date_part.raw.upper().strip() != "SECOND":
            return None

        functional_context = FunctionalContext(context)
        parent_stack_reversed = functional_context.parent_stack.reversed()
        if (
            len(parent_stack_reversed) >= 3 and
            parent_stack_reversed[0].get_type() == "expression" and
            parent_stack_reversed[1].get_type() == "bracketed" and
            parent_stack_reversed[2].get_type() == "function" and
            parent_stack_reversed[2].segments[0].raw.upper().strip() == "INT"
        ):
            return None

        edits = [
            KeywordSegment("second"),
            SymbolSegment("(", type="start_bracket"),
            source,
            SymbolSegment(")", type="end_bracket"),
        ]
        return LintResult(
            anchor=context.segment,
            fixes=[LintFix.replace(context.segment, edits)],
        )


class Rule_GLOBALTEMPVIEW_L006(BaseRule):
    """Global temporary views are not portable across all Spark runtimes.

    ``CREATE GLOBAL TEMPORARY VIEW`` ties the view to the cross-session ``global_temp``
    database and the lifetime of the Spark application. Some runtimes (for example Spark
    Connect / Databricks serverless) do not support global temporary views. There is no
    safe automatic rewrite, so this rule only warns: depending on intent, switch to a
    regular session-scoped view (``CREATE TEMPORARY VIEW``) or a persistent table/view.

    **Flagged**

    .. code-block:: sql

        CREATE GLOBAL TEMPORARY VIEW v AS SELECT 1
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"create_view_statement"})
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Warn when a view is created as GLOBAL TEMPORARY."""
        keywords = [
            segment.raw_upper
            for segment in context.segment.segments
            if segment.is_type("keyword")
        ]
        if "GLOBAL" in keywords:
            return LintResult(
                anchor=context.segment,
                description="GLOBAL TEMPORARY VIEW is not supported on all Spark runtimes "
                "(e.g. Spark Connect / serverless). Use a regular TEMPORARY VIEW or a "
                "persistent table/view instead. See "
                "https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-view.html",
            )
        return None


class Rule_SPARKSQL_L005(BaseRule):
    """Spark 3.0 approx_percentile only accepts int type

    In Spark 3.0, the function percentile_approx and its
    alias approx_percentile only accept integral value with
    range in [1, 2147483647] as its 3rd argument accuracy,
    fractional and string types are disallowed, for example,
    percentile_approx(10.0, 0.2, 1.8D) causes AnalysisException.
    In Spark version 2.4 and below, if accuracy is fractional or
    string value, it is coerced to an int value, percentile_approx(10.0, 0.2, 1.8D)
    is operated as percentile_approx(10.0, 0.2, 1) which results in 10.0.

    Flags a fractional or string literal accuracy only: 2.4 already required
    the accuracy to be a constant, and an integer literal is unchanged. The
    fix `int(acc)` -- the same Cast as `cast(acc AS int)`, which
    SPARKSQLCAST_L001 would flag -- is the coercion 2.4 applied (probed: same
    values and type on 2.4.8 and 3.5.5). An unaliased SELECT item is flagged without
    a fix: its auto-generated column name differs from 2.4's either way
    (2.4.8 `percentile_approx(v, CAST(0.5 AS DOUBLE), CAST(1.8 AS INT))`,
    3.5.5 `percentile_approx(v, 0.5, CAST(1.8 AS INT))`).
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"function"})
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        if _function_name(context) not in ("APPROX_PERCENTILE", "PERCENTILE_APPROX"):
            return None
        bracketed = context.segment.get_child("bracketed")
        if not bracketed:
            return None
        args = bracketed.get_children("expression")
        if len(args) != 3:
            return None
        accuracy = args[2].segments
        code = [seg for seg in accuracy if not seg.is_whitespace and not seg.is_meta]
        if len(code) != 1:
            return None
        literal = code[0]
        fractional = literal.is_type("numeric_literal") and not re.fullmatch(
            r"[0-9]+[LSY]?", literal.raw, re.IGNORECASE
        )
        if not (fractional or literal.is_type("quoted_literal")):
            return None
        if _select_name_kept(context):
            return LintResult(anchor=context.segment)
        edits = [
            KeywordSegment("int"),
            SymbolSegment("(", type="start_bracket"),
            literal,
            SymbolSegment(")", type="end_bracket"),
        ]
        return LintResult(
            anchor=context.segment,
            fixes=[LintFix.replace(literal, edits)],
        )


def _select_name_kept(context: RuleContext) -> bool:
    """Whether the function sits in an unaliased SELECT item, at any depth.

    Spark names such a column after the whole expression's text, so a fix
    that changes the expression changes the column name -- what a write, a
    by-name reference or `df.columns` sees. An alias, or any place outside
    the SELECT list (WHERE, ORDER BY, JOIN ON, GROUP BY), discards the name.
    """
    return any(
        seg.is_type("select_clause_element") and seg.get_child("alias_expression") is None
        for seg in context.parent_stack
    )


def _function_name(context: RuleContext) -> Optional[str]:
    """Shared helper: the upper-cased function name of a `function` segment.

    Every rule above that matches on function name (SPARKSQLCAST_L001,
    FORMATSTRONEINDEX_L004, SPARKSQL_L004/L005) duplicated this same
    children-walk; factored out here for the "just flag any usage of this
    function" rules below, which have no fix to offer and so don't need the
    rest of that duplicated bracketed/argument-walking logic.
    """
    functional_context = FunctionalContext(context)
    children = functional_context.segment.children()
    function_name_segs = children.first(sp.is_type("function_name")).children().first(
        sp.is_type("function_name_identifier")
    )
    if not function_name_segs:
        return None
    return function_name_segs[0].raw.upper().strip()


class Rule_HISTOGRAMNUMERIC_L007(BaseRule):
    """Spark 3.3 histogram_numeric's output element type changed.

    ``histogram_numeric(expr, nb)`` returns an array of (x, y) structs. Before
    Spark 3.3 the ``x`` field was always ``DoubleType``; from 3.3 onward it
    matches the input expression's own numeric type. Downstream code that
    assumes ``DoubleType`` (a cast, a UDF signature, a schema check) can break
    silently rather than throw, so this is flag-only -- there's no legacy
    config to restore the old output type, and there's nothing here safe to
    rewrite automatically.

    **Flagged**

    .. code-block:: sql

        SELECT histogram_numeric(amount, 5) FROM t
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"function"})
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        if _function_name(context) == "HISTOGRAM_NUMERIC":
            return LintResult(
                anchor=context.segment,
                description="histogram_numeric(...)'s output element type matches the input "
                "expression's numeric type from Spark 3.3 onward (was always DoubleType before). "
                "Verify any downstream code that assumes DoubleType. See "
                "https://spark.apache.org/docs/latest/sql-migration-guide.html",
            )
        return None


class Rule_BYTEPADDING_L008(BaseRule):
    """Spark 3.3 added a BINARY-argument overload to lpad/rpad.

    ``lpad``/``rpad`` gained an overload for ``BINARY`` input in Spark 3.3,
    which pads with a byte sequence rather than characters -- if the first
    argument's type is ambiguous or was relying on an implicit cast to
    STRING, the result can differ. Flag-only: telling STRING from BINARY
    apart needs the argument's resolved type, which isn't available from the
    SQL text alone.

    **Flagged**

    .. code-block:: sql

        SELECT lpad(col, 10, '0') FROM t
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"function"})
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        function_name = _function_name(context)
        if function_name in ("LPAD", "RPAD"):
            return LintResult(
                anchor=context.segment,
                description=f"{function_name.lower()}(...) gained a BINARY-argument overload in "
                "Spark 3.3 with byte-sequence (not character) padding semantics. Verify the first "
                "argument's type if it might be BINARY. See "
                "https://spark.apache.org/docs/latest/sql-migration-guide.html",
            )
        return None


class Rule_BINARYCONV_L009(BaseRule):
    """Spark 3.3/3.4 to_binary/unbase64 now throw on malformed input.

    ``to_binary(str, fmt)`` (3.3) and ``unbase64`` (3.4, for a `fmt` other
    than the default) now raise on malformed input instead of returning
    NULL. Data-dependent (Class B-shaped: exposure is certain, whether it
    actually throws depends on the data), so flag-only rather than a
    rewrite or a config injection -- there's no legacy config that restores
    the old null-on-error behavior for these two functions specifically.

    **Flagged**

    .. code-block:: sql

        SELECT to_binary(col, 'base64') FROM t
        SELECT unbase64(col) FROM t
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"function"})
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        function_name = _function_name(context)
        if function_name in ("TO_BINARY", "UNBASE64"):
            return LintResult(
                anchor=context.segment,
                description=f"{function_name.lower()}(...) now raises on malformed input instead "
                "of returning NULL (Spark 3.3/3.4). Whether this actually fires depends on the "
                "data -- review error handling around this call. See "
                "https://spark.apache.org/docs/latest/sql-migration-guide.html",
            )
        return None


class Rule_CTEPRECEDENCE_L010(BaseRule):
    """Spark 3.0 rejects an inner WITH that shadows an outer CTE of the same name.

    A nested ``WITH`` clause defining a CTE with the same name as an
    enclosing one resolved to the OUTER definition in 2.4; Spark 3.x fails
    the query by default (``spark.sql.legacy.ctePrecedencePolicy=EXCEPTION``).
    ``LEGACY`` restores 2.4's outer-wins resolution; ``CORRECTED`` makes the
    INNER one win, which changes the result. Detected structurally: any
    ``common_table_expression`` whose name also names an ANCESTOR
    ``common_table_expression`` in the same query is exactly the shape that
    changed meaning.

    **Flagged**

    .. code-block:: sql

        WITH t AS (SELECT 1),
             q AS (WITH t AS (SELECT 2) SELECT * FROM t)
        SELECT * FROM q
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"common_table_expression"})
    is_fix_compatible = False

    @staticmethod
    def _cte_name(segment) -> Optional[str]:
        name_seg = segment.get_child("naked_identifier") or segment.get_child("quoted_identifier")
        return name_seg.raw.lower().strip('"').strip("'") if name_seg is not None else None

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        # The OUTER definition with the same name is a SIBLING of an
        # ancestor `with_compound_statement`, not an ancestor itself (the
        # parse tree nests the inner WITH inside the outer CTE's own body,
        # it doesn't nest CTEs inside each other directly) -- so this walks
        # every enclosing WITH clause and checks its direct CTE children,
        # not just the literal ancestor chain.
        name = self._cte_name(context.segment)
        if name is None:
            return None
        for ancestor in context.parent_stack:
            if not ancestor.is_type("with_compound_statement"):
                continue
            for cte in ancestor.get_children("common_table_expression"):
                if cte is context.segment:
                    continue
                if self._cte_name(cte) == name:
                    return LintResult(
                        anchor=context.segment,
                        description=f'CTE "{name}" shadows an enclosing CTE of the same name. 2.4 resolved '
                        "references to the OUTER definition; Spark 3.x fails the query by default. "
                        "spark.sql.legacy.ctePrecedencePolicy=LEGACY restores 2.4's resolution (CORRECTED "
                        "would use the INNER one and change the result). See "
                        "https://spark.apache.org/docs/latest/sql-migration-guide.html",
                    )
        return None


class Rule_TYPEDPARTITION_L011(BaseRule):
    """Spark 3.0 requires typed literals for some partition spec values.

    Untyped literal partition values that don't cleanly coerce to the
    partition column's type are handled more strictly from Spark 3.0 onward.
    Detected coarsely -- any ``PARTITION (...)`` clause -- since telling
    which specific literals are affected needs the target table's partition
    column types, not just the SQL text.

    **Flagged**

    .. code-block:: sql

        INSERT INTO t PARTITION (dt='2020-01-01') SELECT * FROM s
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"keyword"})
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        if context.segment.raw_upper == "PARTITION":
            return LintResult(
                anchor=context.segment,
                description="PARTITION (...) clause found -- Spark 3.0 handles partition literal "
                "values that don't cleanly coerce to the column's type more strictly. Verify the "
                "literal types against the target table's partition column types. See "
                "https://spark.apache.org/docs/latest/sql-migration-guide.html",
            )
        return None


class Rule_TRANSFORMDELIM_L012(BaseRule):
    """Spark 3.0 changed TRANSFORM's default row/field delimiters and schema inference.

    ``SELECT TRANSFORM(...) USING 'script'`` (Hive-style scripting transform)
    changed its default delimiters and no-``AS``-clause output schema
    inference in Spark 3.0. Detected coarsely -- any ``TRANSFORM`` clause --
    since verifying whether the specific delimiter/schema defaults in use
    actually changed behavior needs the script's own I/O format, not just
    the SQL text.

    **Flagged**

    .. code-block:: sql

        SELECT TRANSFORM (a, b) USING 'script.py' FROM t
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"transform_clause"})
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        return LintResult(
            anchor=context.segment,
            description="TRANSFORM (...) USING '...' found -- Spark 3.0 changed its default "
            "row/field delimiters and no-AS-clause output schema inference. Verify against the "
            "script's actual I/O format. See "
            "https://spark.apache.org/docs/latest/sql-migration-guide.html",
        )


# A date, optionally with a time: what Spark 3.x casts a compared string to.
_DATE_STRING = re.compile(r"\d{4}-\d{1,2}-\d{1,2}(?:[ T]\d{1,2}:\d{1,2}(?::\d{1,2}(?:\.\d{1,9})?)?)?")
_CANONICAL_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
# The operator seen from the column's side when the literal is on the left.
_MIRRORED = {"<": ">", ">": "<", "<=": ">=", ">=": "<="}


def date_string_comparison_changes(literal: str, operator: str) -> bool:
    """Whether comparing a date or timestamp column to the string `literal`
    with `operator` (the column on the left; "BETWEEN_LO"/"BETWEEN_HI" for a
    bound) can give a different answer on Spark 3.x than on 2.4.

    Probed on 2.4.8 and 3.5.5 over every operator: 2.4 compared a date to the
    string as strings, and a timestamp too except under = and <=>, which cast
    the string to a timestamp. 3.x casts the string to the column's type. So
    a zero-padded 'yyyy-MM-dd' literal changes only a timestamp compared with
    > or <= (or a BETWEEN upper bound): 2.4 ordered '2020-01-01 00:00:00'
    after '2020-01-01'. Any other date-shaped literal ('2020-1-1', one with a
    time) can change every operator on a date column. IN is unchanged.
    """
    if not _DATE_STRING.fullmatch(literal):
        return False
    if not _CANONICAL_DATE.fullmatch(literal):
        return True
    return operator in (">", "<=", "BETWEEN_HI")


class Rule_DATESTRCOMPARE_L013(BaseRule):
    """Spark 3.0 compares a date or timestamp with a string as a date or timestamp.

    2.4 cast the date or timestamp to a string and compared strings (except
    a timestamp under = and <=>); 3.x casts the string to the column's type.
    The answer changes silently: ``ts > '2020-01-01'`` keeps midnight rows on
    2.4 and drops them on 3.x, and ``d = '2020-1-1'`` matched nothing on 2.4
    and matches 2020-01-01 on 3.x. Without types the literal is the evidence:
    only the literal/operator pairs a probe showed can change are flagged
    (``date_string_comparison_changes``), so the common
    ``dt = '2020-01-01'`` is not. Feeds
    ``spark.sql.legacy.typeCoercion.datetimeToString.enabled``, which gave
    2.4.8's rows for every pair probed on 3.5.5.

    **Flagged**

    .. code-block:: sql

        SELECT * FROM t WHERE ts > '2020-01-01' OR d = '2020-1-1'
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"quoted_literal"})
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        parent = context.parent_stack[-1] if context.parent_stack else None
        if parent is None or not parent.is_type("expression"):
            return None
        code = [s for s in parent.segments if s.is_code]
        i = next(k for k, s in enumerate(code) if s is context.segment)
        before = code[i - 1] if i > 0 else None
        after = code[i + 1] if i + 1 < len(code) else None
        if before is not None and before.is_type("comparison_operator"):
            operator = "".join(before.raw.split())
        elif after is not None and after.is_type("comparison_operator"):
            operator = "".join(after.raw.split())
            operator = _MIRRORED.get(operator, operator)
        elif before is not None and before.is_type("keyword") and before.raw_upper == "BETWEEN":
            operator = "BETWEEN_LO"
        elif before is not None and before.is_type("keyword") and before.raw_upper == "AND":
            # A keyword AND is BETWEEN's; the logical one is a binary_operator.
            operator = "BETWEEN_HI"
        else:
            return None
        literal = context.segment.raw[1:-1]
        if not date_string_comparison_changes(literal, operator):
            return None
        return LintResult(
            anchor=context.segment,
            description=f"'{literal}' is compared as a string. If the other side is a date or timestamp, Spark 3.x "
            "casts the string to it, where 2.4 cast the date or timestamp to a string (except a timestamp under = and "
            "<=>), so the result can change silently: ts > '2020-01-01' kept midnight rows on 2.4 and drops them on "
            "3.x; d = '2020-1-1' matched nothing on 2.4 and matches on 3.x. "
            "spark.sql.legacy.typeCoercion.datetimeToString.enabled=true restores 2.4 and is injected; its cost is that "
            "a date partition column compared with a string is no longer pruned. To drop it, compare with a typed "
            "literal (DATE '...', TIMESTAMP '...') that states the comparison you mean.",
        )


_NONEMPTY_CTAS_PROBED = (
    "Probed on 2.4.8 and 3.5.5, re-running the CTAS after DROP TABLE left the files in place."
)


def _ctas_location(segment) -> bool:
    """Whether a ``create_table_statement`` is a CTAS with a location: a
    LOCATION clause, or a 'path' in OPTIONS, and an ``AS <query>``."""
    keywords = [s.raw_upper for s in segment.segments if s.is_type("keyword")]
    if "AS" not in keywords:
        return False
    if "LOCATION" in keywords:
        return True
    options = segment.get_child("bracketed")
    return options is not None and "OPTIONS" in keywords and any(
        p.raw.strip("`'\"").lower() == "path" for p in options.recursive_crawl("property_name_identifier")
    )


class Rule_CTASLOCATION_L014(BaseRule):
    """Spark 3.2 rejects a data source CTAS into a non-empty location.

    ``CREATE TABLE t USING <format> LOCATION '<dir>' AS SELECT ...`` fails on
    3.x when the directory already holds files ("CREATE-TABLE-AS-SELECT cannot
    create table with location to a non-empty directory"); 2.4 replaced
    them. The common way to hit it is a re-run: the table was dropped (or the
    job died after writing) and the files stayed. Probed: with
    ``spark.sql.legacy.allowNonEmptyLocationInCTAS=true`` 3.5.5 replaces the
    old files and the table holds what 2.4.8's does. A Hive-format CTAS is
    HIVECTASLOCATION_L015.

    **Flagged**

    .. code-block:: sql

        CREATE TABLE t USING parquet LOCATION '/data/t' AS SELECT * FROM s
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"create_table_statement"})
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        if context.segment.get_child("using_clause") is None or not _ctas_location(context.segment):
            return None
        return LintResult(
            anchor=context.segment,
            description="CREATE TABLE ... USING ... LOCATION ... AS SELECT: Spark 3.2 fails when the location already "
            "holds files (a re-run after DROP TABLE, or after a failed run); 2.4 replaced them. "
            "spark.sql.legacy.allowNonEmptyLocationInCTAS=true restores 2.4's result and is injected. "
            + _NONEMPTY_CTAS_PROBED,
        )


class Rule_HIVECTASLOCATION_L015(BaseRule):
    """Spark 3.2 rejects a Hive-format CTAS into a non-empty location, and its
    legacy config does not restore 2.4.

    ``CREATE TABLE t [STORED AS <format>] LOCATION '<dir>' AS SELECT ...``
    with no ``USING`` is a Hive table. 3.x fails when the directory already
    holds files, as for L014; 2.4 replaced them. Probed on 3.5.5 with
    ``spark.sql.legacy.allowNonEmptyLocationInCTAS=true``, with
    ``spark.sql.hive.convertMetastoreCtas`` on and off: the CTAS succeeds but
    KEEPS the old files, so the table holds the old rows and the new ones --
    2.4.8 held only the new. So no config is injected for this one: empty the
    location before the CTAS.

    **Flagged**

    .. code-block:: sql

        CREATE TABLE t STORED AS PARQUET LOCATION '/data/t' AS SELECT * FROM s
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"create_table_statement"})
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        if context.segment.get_child("using_clause") is not None or not _ctas_location(context.segment):
            return None
        return LintResult(
            anchor=context.segment,
            description="Hive-format CREATE TABLE ... LOCATION ... AS SELECT: Spark 3.2 fails when the location "
            "already holds files (a re-run after DROP TABLE, or after a failed run); 2.4 replaced them. "
            "spark.sql.legacy.allowNonEmptyLocationInCTAS=true does NOT restore 2.4 here: the old files are kept "
            "and the table returns the old rows plus the new. Delete the location's files before the CTAS. "
            + _NONEMPTY_CTAS_PROBED,
        )


class Rule_RECURSIVEVIEW_L016(BaseRule):
    """Spark 3.1 rejects a temporary view defined in terms of itself.

    ``CREATE OR REPLACE TEMP VIEW v AS SELECT ... FROM v`` -- redefining a
    temp view on top of its previous definition -- worked in 2.4, which
    stored the view's analyzed plan, so ``v`` in the body meant the old view.
    3.1+ stores the view's text and fails with ``[RECURSIVE_VIEW] Recursive
    view `v` detected``. Probed on 2.4.8 and 3.5.5, TEMP, TEMPORARY and
    GLOBAL TEMPORARY alike: ``spark.sql.legacy.storeAnalyzedPlanForView=true``
    gives 2.4.8's rows. ``df.createOrReplaceTempView`` is not affected.

    **Flagged**

    .. code-block:: sql

        CREATE OR REPLACE TEMP VIEW v AS SELECT a + 1 AS a FROM v
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"create_view_statement"})
    is_fix_compatible = False

    @staticmethod
    def _name(reference) -> str:
        return ".".join(p.raw.strip("`").lower() for p in reference.segments if p.is_type("naked_identifier", "quoted_identifier"))

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        keywords = [s.raw_upper for s in context.segment.segments if s.is_type("keyword")]
        view = context.segment.get_child("table_reference")
        if view is None or not ({"TEMP", "TEMPORARY"} & set(keywords)):
            return None
        # A temp view is referred to by its bare name; a global one through global_temp.
        name = ("global_temp." if "GLOBAL" in keywords else "") + self._name(view)
        for ref in context.segment.recursive_crawl("table_reference"):
            if ref is not view and self._name(ref) == name:
                return LintResult(
                    anchor=context.segment,
                    description=f"Temporary view {name} is defined in terms of itself. 2.4 read the name in the body as "
                    "the view's previous definition; Spark 3.1+ fails with RECURSIVE_VIEW. "
                    "spark.sql.legacy.storeAnalyzedPlanForView=true restores 2.4's behaviour and is injected; "
                    "giving the new view its own name avoids it in code.",
                )
        return None
