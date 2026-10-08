"""The PySpark guide's 3.3 -> 3.4 pandas-on-Spark changes (spark-migrate-cli
PYSPARK.md G14). spark-migrate-cli runs these only when the repo uses pandas API
on Spark -- `pyspark.pandas`, or Koalas, which a 2.4 repo uses and which has to
be ported to it (PandasOnSparkGate, `requiresPandasOnSpark`).

Inside such a repo plain pandas is usually in use too, and shares every method
name here, so each rule also asks where the frame came from: a `ps.`/`ks.`
call or `.pandas_api()`/`.to_koalas()` is pandas-on-Spark (medium); `pd.` or
`.toPandas()` is plain pandas, and is skipped; anything else is unknown (low).
One hop through a name assigned in the same function or module, as everywhere.

Probed on PySpark 3.5.5 with pandas 2.0 (PYSPARK.md SS8.13): each "now"
behaviour below is what 3.5.5 does.
"""
from __future__ import annotations

import libcst as cst

from spark_upgrade_rules import _cst
from spark_upgrade_rules.detector import Detector

PS_MODULES = ("pyspark.pandas", "databricks.koalas")
PS_CONVERTERS = {"pandas_api", "to_pandas_on_spark", "to_koalas"}
PD_CONVERTERS = {"toPandas", "to_pandas"}


class _Assignments(cst.CSTVisitor):
    """Every value assigned to a name, per function scope (module at index 0)."""

    def __init__(self) -> None:
        self.stack: list[dict[str, list]] = [{}]
        self.by_scope: dict[cst.CSTNode, dict[str, list]] = {}

    def visit_FunctionDef(self, node: cst.FunctionDef) -> None:
        self.stack.append({})
        self.by_scope[node] = self.stack[-1]

    def leave_FunctionDef(self, original_node: cst.FunctionDef) -> None:
        self.stack.pop()

    def visit_Assign(self, node: cst.Assign) -> None:
        for t in node.targets:
            if isinstance(t.target, cst.Name):
                self.stack[-1].setdefault(t.target.value, []).append(node.value)


class _PandasOnSparkDetector(Detector):
    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        found = _Assignments()
        ctx.module.visit(found)
        self.module_scope = found.stack[0]
        self.by_scope = found.by_scope
        self.scope = [self.module_scope]

    def visit_FunctionDef(self, node: cst.FunctionDef) -> None:
        self.scope.append(self.by_scope.get(node, {}))

    def leave_FunctionDef(self, original_node: cst.FunctionDef) -> None:
        self.scope.pop()

    def _from_module(self, node: cst.BaseExpression) -> str | None:
        for q in self.qualified(node):
            if q.startswith(PS_MODULES):
                return "ps"
            if q == "pandas" or q.startswith("pandas."):
                return "pd"
        return None

    def provenance(self, node: cst.BaseExpression | None, depth: int = 0) -> str | None:
        """"ps", "pd", or None when the frame's origin is not visible."""
        for n in _cst.spine(node):
            if isinstance(n, cst.Call) and _cst.method_name(n) in PS_CONVERTERS:
                return "ps"
            if isinstance(n, cst.Call) and _cst.method_name(n) in PD_CONVERTERS:
                return "pd"
        root = _cst.spine_root(node)
        if root is None:
            return None
        kind = self._from_module(root)
        if kind or not isinstance(root, cst.Name) or depth > 2:
            return kind
        for scope in reversed(self.scope):
            if root.value in scope:
                # The first assignment that is not the name rebinding itself
                # (`kdf = kdf.drop(...)`) says what it is.
                for value in scope[root.value]:
                    if _cst.spine_root(value) is not None and getattr(_cst.spine_root(value), "value", None) == root.value:
                        continue
                    return self.provenance(value, depth + 1)
                return None
        return None

    def flag(self, node: cst.CSTNode, frame: cst.BaseExpression | None, message: str) -> None:
        kind = self.provenance(frame)
        if kind != "pd":
            self.report(node, message, "medium" if kind == "ps" else "low")

    @staticmethod
    def groupby_on_spine(node: cst.BaseExpression | None) -> cst.Call | None:
        return next(iter(_cst.spine_calls(node, "groupby")), None)


class PandasOnSparkGroupByHeadTailNegative(_PandasOnSparkDetector):
    rule_id = "PandasOnSparkGroupByHeadTailNegative"

    def visit_Call(self, node: cst.Call) -> None:
        name = _cst.method_name(node)
        if name not in ("head", "tail"):
            return
        n = _cst.int_value(_cst.argument(node, 0, "n"))
        groupby = self.groupby_on_spine(_cst.receiver(node))
        if n is not None and n < 0 and groupby is not None:
            self.flag(node, _cst.receiver(groupby), f"groupby(...).{name}({n}) on pandas API on Spark: from Spark 3.4 a "
                      "negative n selects positionally, as in pandas 1.4 -- all but the last/first rows of each group "
                      "-- where 3.3 returned an empty frame. Check what the caller expects.")


class PandasOnSparkGroupByApplyInference(_PandasOnSparkDetector):
    """Two bullets, one call site: `groupby(...).apply(func)` with no return-type
    hint. 3.4 infers the result schema from the pandas dtypes first, and with
    `compute.shortcut_limit` at 0 samples 2 rows instead of 0."""

    rule_id = "PandasOnSparkGroupByApplyInference"

    def _hinted(self, func: cst.BaseExpression | None) -> bool:
        if isinstance(func, cst.Name):
            for node in self.ctx.module.body:
                if isinstance(node, cst.FunctionDef) and node.name.value == func.value:
                    return node.returns is not None
        return False

    def visit_Call(self, node: cst.Call) -> None:
        if _cst.method_name(node) != "apply":
            return
        groupby = self.groupby_on_spine(_cst.receiver(node))
        func = _cst.argument(node, 0, "func")
        if groupby is None or func is None or self._hinted(func):
            return
        self.flag(node, _cst.receiver(groupby), "groupby(...).apply(func) with no return-type hint on pandas API on Spark: "
                  "Spark 3.4 infers the result schema from the pandas dtypes first, and samples at least 2 rows when "
                  "compute.shortcut_limit is 0, so the inferred column types can differ from 3.3. Annotate func's return "
                  "type (-> ps.DataFrame[...]) to fix the schema.")


class PandasOnSparkIndexInsertBounds(_PandasOnSparkDetector):
    rule_id = "PandasOnSparkIndexInsertBounds"

    def visit_Call(self, node: cst.Call) -> None:
        recv = _cst.receiver(node)
        if _cst.method_name(node) != "insert" or not (isinstance(recv, cst.Attribute) and recv.attr.value == "index"):
            return
        self.flag(node, recv.value, "Index.insert(loc, ...) on pandas API on Spark: from Spark 3.4 a loc outside the index "
                  "raises IndexError (\"index N is out of bounds for axis 0\"), as pandas 1.4 does. Check loc is bounded.")


class PandasOnSparkSeriesModeName(_PandasOnSparkDetector):
    rule_id = "PandasOnSparkSeriesModeName"

    def visit_Call(self, node: cst.Call) -> None:
        if _cst.method_name(node) != "mode" or node.args or _cst.spine_attrs(node) & {"write", "writeStream"}:
            return
        self.flag(node, _cst.receiver(node), "Series.mode() on pandas API on Spark: from Spark 3.4 the result keeps the "
                  "series' name, as pandas 1.4 does, where 3.3 dropped it -- a to_frame() or join on the result sees a "
                  "different column name.")


class PandasOnSparkAstypeCategory(_PandasOnSparkDetector):
    rule_id = "PandasOnSparkAstypeCategory"

    def visit_Call(self, node: cst.Call) -> None:
        if _cst.method_name(node) != "astype":
            return
        arg = _cst.argument(node, 0, "dtype")
        if self.string(arg) == "category" or any(self.string(v) == "category" for _, v in _cst.dict_items(arg)):
            self.flag(node, _cst.receiver(node), "astype('category') on pandas API on Spark: from Spark 3.4 the categories "
                      "keep the original data's dtype (int64 for integers), as pandas 1.4 does. Code that compares or "
                      "joins on categories.dtype sees the change.")


class PandasOnSparkConcatSort(_PandasOnSparkDetector):
    rule_id = "PandasOnSparkConcatSort"

    def visit_Call(self, node: cst.Call) -> None:
        if _cst.keyword(node, "sort") is None:
            return
        if any(q in ("pyspark.pandas.concat", "databricks.koalas.concat") for q in self.qualified(node.func)):
            self.report(node, "concat(..., sort=...) on pandas API on Spark: Spark 3.4 respects sort, as pandas 1.4 does "
                              "-- sort=True orders the non-concatenation axis, sort=False keeps it as given. 3.3 ignored "
                              "it. Check the column order downstream code expects.", "medium")
