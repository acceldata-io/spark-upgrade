"""PySpark API changes between 2.4.8 and 3.5.5 that break a call outright, and
the three with an exact rewrite.

The list of removals is not transcribed from a guide: it is every public name
2.4.8's `python/pyspark` defines that a live 3.5.5 import no longer has,
diffed module by module and member by member (spark-migrate-cli PYSPARK.md
SS2.7). Three are covered elsewhere: `pyspark.streaming.kafka` and
`pyspark.streaming.flume` are hard blockers on the JVM side, and the `Has*`
mixins' setters are `MlSharedParamSetters` below.

The rewrites:

  RowKwargsFieldOrder    2.4's Row(**kwargs) sorted the fields by name; 3.x
                         keeps them as written. Sorting the keyword arguments
                         in the source restores 2.4's field order exactly.
  MlSharedParamSetters   3.0 dropped `set<Param>()` from every
                         `pyspark.ml.param.shared.Has<Param>` mixin. The
                         method 2.4 inherited is added back to the class.
  RemovedPySparkApi      `OneHotEncoderEstimator` is `OneHotEncoder` since 3.0,
                         same parameters; the pandas/pyarrow version checks
                         moved to `pyspark.sql.pandas.utils`.
"""
from __future__ import annotations

import libcst as cst
import libcst.helpers  # noqa: F401 -- cst.helpers

from spark_upgrade_rules import _cst
from spark_upgrade_rules.detector import Fixer

ROW = ("pyspark.sql.Row", "pyspark.sql.types.Row")

# Builtins and str/dict/date methods with no side effects: reordering keyword
# arguments built from these cannot change what they evaluate to.
_PURE_FUNCTIONS = {"int", "float", "str", "bool", "len", "abs", "round", "min", "max", "sum", "tuple", "list", "dict",
                   "set", "frozenset", "sorted", "repr", "Decimal", "date", "datetime"}
_PURE_METHODS = {"strip", "lstrip", "rstrip", "lower", "upper", "title", "capitalize", "split", "rsplit", "replace",
                 "get", "isoformat", "date", "time", "format", "join", "startswith", "endswith", "encode", "decode",
                 "keys", "values", "items", "asDict", "zfill", "rjust", "ljust", "count", "index", "find"}


def _pure(node: cst.CSTNode | None) -> bool:
    """An expression whose evaluation has no side effect a reorder could expose."""
    if node is None:
        return True
    if isinstance(node, (cst.Name, cst.SimpleString, cst.Integer, cst.Float, cst.Imaginary, cst.Ellipsis)):
        return True
    if isinstance(node, cst.ConcatenatedString):
        return _pure(node.left) and _pure(node.right)
    if isinstance(node, cst.FormattedString):
        return all(_pure(p.expression) for p in node.parts if isinstance(p, cst.FormattedStringExpression))
    if isinstance(node, cst.Attribute):
        return _pure(node.value)
    if isinstance(node, cst.Subscript):
        return _pure(node.value) and all(
            isinstance(e.slice, cst.Index) and _pure(e.slice.value) or isinstance(e.slice, cst.Slice) and
            _pure(e.slice.lower) and _pure(e.slice.upper) and _pure(e.slice.step) for e in node.slice)
    if isinstance(node, cst.UnaryOperation):
        return _pure(node.expression)
    if isinstance(node, (cst.BinaryOperation, cst.BooleanOperation)):
        return _pure(node.left) and _pure(node.right)
    if isinstance(node, cst.Comparison):
        return _pure(node.left) and all(_pure(c.comparator) for c in node.comparisons)
    if isinstance(node, cst.IfExp):
        return _pure(node.test) and _pure(node.body) and _pure(node.orelse)
    if isinstance(node, (cst.Tuple, cst.List, cst.Set)):
        return all(_pure(e.value) for e in node.elements)
    if isinstance(node, cst.Dict):
        return all(isinstance(e, cst.DictElement) and _pure(e.key) and _pure(e.value) for e in node.elements)
    if isinstance(node, cst.Call):
        f = node.func
        ok = (isinstance(f, cst.Name) and f.value in _PURE_FUNCTIONS) or \
             (isinstance(f, cst.Attribute) and f.attr.value in _PURE_METHODS and _pure(f.value))
        return ok and all(_pure(a.value) for a in node.args)
    return False


class RowKwargsFieldOrder(Fixer):
    """`Row(name=..., age=...)`: 2.4 sorted the fields by name, so the row was
    (age, name) -- an inferred schema's column order, `row[0]`, tuple
    unpacking and positional writes all saw that order. 3.0 keeps them as
    written (SPARK-29748), and no setting restores the sort. Sorting the
    arguments in the source gives 2.4's order on 3.x. Rewritten when every
    value is free of side effects (the reorder changes evaluation order);
    `Row(**d)` becomes `Row(**dict(sorted(d.items())))`. Already-sorted
    arguments behave the same on both and are not flagged. Supersedes
    PySparkler's PY24-30-007, which matched any callable named `Row`, flagged
    sorted rows too, and crashed on `**`."""

    rule_id = "RowKwargsFieldOrder"

    def _is_row(self, call: cst.Call) -> bool:
        if any(q in ROW for q in self.qualified(call.func)):
            return True
        return isinstance(call.func, cst.Name) and call.func.value == "Row" and \
            (self.ctx.star_sql or self.ctx.star_types) and "Row" not in self.ctx.bindings.count

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.BaseExpression:
        args = original_node.args
        if not args or not self._is_row(original_node) or any(a.keyword is None and a.star != "**" for a in args):
            return updated_node
        stars = [a for a in args if a.star == "**"]
        if stars:
            if len(args) == 1:
                d = updated_node.args[0].value
                sorted_items = cst.Call(func=cst.Name("dict"), args=[cst.Arg(cst.Call(func=cst.Name("sorted"), args=[
                    cst.Arg(cst.Call(func=cst.Attribute(value=d, attr=cst.Name("items"))))]))])
                return self.rewrite(original_node, updated_node.with_changes(
                    args=[updated_node.args[0].with_changes(value=sorted_items)]),
                    "Row(**mapping): 2.4 sorted the fields by name; Spark 3.0 keeps the mapping's order, so an inferred "
                    "schema's column order and positional access change. Rewritten to sort the mapping, which is 2.4's order.")
            self.report(original_node, "Row(...) mixes keyword arguments and **: 2.4 sorted all the fields by name, 3.0 keeps "
                                       "them as written. Merge them into one sorted mapping by hand.", "high", tier=3)
            return updated_node
        names = [a.keyword.value for a in args]
        if names == sorted(names):
            return updated_node
        message = (f"Row({', '.join(names)}): 2.4 sorted keyword fields by name ({', '.join(sorted(names))}); Spark 3.0 "
                   "keeps them as written, so an inferred schema's column order, row[0], tuple unpacking and positional "
                   "writes (union, insertInto) change.")
        if not all(_pure(a.value) for a in args):
            self.report(original_node, message + " A value here may have side effects, so the arguments are not reordered "
                                                 "automatically: put them in sorted order by hand.", "high", tier=3)
            return updated_node
        ordered = sorted(updated_node.args, key=lambda a: a.keyword.value)
        new_args = [arg.with_changes(comma=slot.comma) for arg, slot in zip(ordered, updated_node.args)]
        return self.rewrite(original_node, updated_node.with_changes(args=new_args),
                            message + " Rewritten with the arguments in sorted order, 2.4's field order.")


# The pyspark.ml.param.shared mixins whose `set<Param>(value)` 2.4 defined as
# `return self._set(<param>=value)` and 3.0 removed (SPARK-29093) -- read from
# 2.4.8's shared.py, and each confirmed absent on 3.5.5 by import.
SHARED_PARAMS = ("MaxIter", "RegParam", "FeaturesCol", "LabelCol", "PredictionCol", "ProbabilityCol", "RawPredictionCol",
                 "InputCol", "InputCols", "OutputCol", "OutputCols", "NumFeatures", "CheckpointInterval", "Seed", "Tol",
                 "StepSize", "HandleInvalid", "ElasticNetParam", "FitIntercept", "Standardization", "Thresholds",
                 "Threshold", "WeightCol", "Solver", "VarianceCol", "AggregationDepth", "Parallelism",
                 "CollectSubModels", "Loss", "DistanceMeasure")
SHARED = "pyspark.ml.param.shared"
# Bases that define none of those setters, in 2.4 or 3.5: a class built from
# these and the mixins has exactly the setters the mixins gave it.
_SETTERLESS = {"pyspark.ml.Transformer", "pyspark.ml.base.Transformer", "pyspark.ml.Estimator", "pyspark.ml.base.Estimator",
               "pyspark.ml.Model", "pyspark.ml.base.Model", "pyspark.ml.param.Params", "pyspark.ml.param.shared.Params",
               "pyspark.ml.util.DefaultParamsReadable", "pyspark.ml.util.DefaultParamsWritable",
               "pyspark.ml.util.MLReadable", "pyspark.ml.util.MLWritable", "pyspark.ml.util.JavaMLReadable",
               "pyspark.ml.util.JavaMLWritable", "pyspark.ml.wrapper.JavaTransformer", "pyspark.ml.wrapper.JavaEstimator",
               "pyspark.ml.wrapper.JavaModel", "builtins.object"}


class MlSharedParamSetters(Fixer):
    """A class of the repo's own built on `pyspark.ml.param.shared.Has*`
    mixins. Spark 3.0 removed the mixins' setters, so `t.setInputCol("x")` on
    it is an AttributeError. The setters it inherited in 2.4 are added back
    to the class, with 2.4's body. Only where the class's other bases are
    known to define no setters of their own; otherwise reported. Supersedes
    PySparkler's PY24-30-008, which flagged the import only."""

    rule_id = "MlSharedParamSetters"

    def leave_ClassDef(self, original_node: cst.ClassDef, updated_node: cst.ClassDef) -> cst.ClassDef:
        mixins, other = [], []
        for base in original_node.bases:
            names = self.qualified(base.value)
            hit = next((q[len(SHARED) + 4:] for q in names if q.startswith(SHARED + ".Has") and q[len(SHARED) + 4:] in SHARED_PARAMS), None)
            if hit:
                mixins.append(hit)
            elif not names & _SETTERLESS:
                other.append(self.code(base.value))
        defined = {s.name.value for s in original_node.body.body if isinstance(s, cst.FunctionDef)}
        missing = [p for p in mixins if f"set{p}" not in defined]
        if not missing:
            return updated_node
        setters = ", ".join(f"set{p}" for p in missing)
        message = (f"{original_node.name.value} gets {setters} from pyspark.ml.param.shared's Has* mixins in 2.4. Spark 3.0 "
                   "removed the mixins' setters (SPARK-29093), so calling them is an AttributeError.")
        if other or (original_node.keywords and any(k.keyword.value == "metaclass" for k in original_node.keywords)):
            self.report(original_node.name, message + f" Its other base{'s' if len(other) > 1 else ''} ({', '.join(other)}) may "
                                            "define them; if not, add them as self._set(<param>=value).", "medium", tier=3)
            return updated_node
        methods = []
        for i, p in enumerate(missing):
            param = p[0].lower() + p[1:]
            fn = cst.parse_statement(f"def set{p}(self, value):\n    return self._set({param}=value)\n")
            lines = [cst.EmptyLine(indent=False)]
            if i == 0:
                lines.append(cst.EmptyLine(comment=cst.Comment(
                    "# Inherited from the Has* mixins in Spark 2.4; Spark 3.0 removed them (SPARK-29093).")))
            methods.append(fn.with_changes(leading_lines=lines))
        new = updated_node.with_changes(body=updated_node.body.with_changes(body=[*updated_node.body.body, *methods]))
        self.report(original_node.name, message + f" Rewritten: {setters} added to the class with 2.4's body.", "high",
                    tier=1, fix=f"class {original_node.name.value}: + " + "; ".join(
                        f"def set{p}(self, value): return self._set({p[0].lower() + p[1:]}=value)" for p in missing))
        return new


# ---- removed APIs ----------------------------------------------------------------

# Imported names that no longer exist, by module: (replacement module or None, what to do).
_MOVED = {
    ("pyspark.sql.utils", "require_minimum_pandas_version"): "pyspark.sql.pandas.utils",
    ("pyspark.sql.utils", "require_minimum_pyarrow_version"): "pyspark.sql.pandas.utils",
}
_GONE = {
    ("pyspark.ml.param.shared", "DecisionTreeParams"):
        "removed in 3.0 -- the tree params moved to private classes; set maxDepth & co. through the estimator itself",
    ("pyspark.ml.util", "JavaPredictionModel"): "removed in 3.0 -- extend pyspark.ml.wrapper.JavaPredictionModel's successor, "
                                                "pyspark.ml.base.PredictionModel",
    ("pyspark.sql.utils", "capture_sql_exception"): "removed -- PySpark's exception conversion is internal (pyspark.errors) since 3.4",
    ("pyspark.sql.utils", "install_exception_handler"): "removed -- PySpark's exception conversion is internal (pyspark.errors) since 3.4",
}
ENCODER_ESTIMATOR = "pyspark.ml.feature.OneHotEncoderEstimator"
ENCODER = "pyspark.ml.feature.OneHotEncoder"


class RemovedPySparkApi(Fixer):
    """A 2.4 PySpark API that 3.5.5 does not have -- an ImportError or
    AttributeError the moment it runs. Rewritten where the replacement is the
    same thing under a new name: `OneHotEncoderEstimator` (3.0 renamed it
    `OneHotEncoder`, same parameters and model), and the pandas/pyarrow
    version checks, which moved to `pyspark.sql.pandas.utils`. Reported:
    `KMeansModel.computeCost`, 2.4's transformer `OneHotEncoder` used
    without `fit`, ML readers/writers' `.context(...)`, and imports of names
    with no successor."""

    rule_id = "RemovedPySparkApi"

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        imported = {n for n in _cst.names_imported(ctx.module, "pyspark.ml.feature")}
        self.has_encoder = "OneHotEncoder" in imported
        # Names bound once to `OneHotEncoder(...)` -- 2.4's transformer, if the
        # file does not import the estimator too.
        self.encoders = {name for name, value in ctx.bindings.value.items()
                         if ctx.bindings.count.get(name) == 1 and isinstance(value, cst.Call)
                         and isinstance(value.func, cst.Name) and value.func.value == "OneHotEncoder"} \
            if self.has_encoder and "OneHotEncoderEstimator" not in imported else set()

    # -- imports
    def leave_ImportFrom(self, original_node: cst.ImportFrom, updated_node: cst.ImportFrom) -> cst.BaseSmallStatement:
        if original_node.module is None or isinstance(original_node.names, cst.ImportStar):
            module = cst.helpers.get_full_name_for_node(original_node.module) if original_node.module else None
            if module and (module, "*") in _GONE:
                self.report(original_node, f"from {module} import *: {_GONE[(module, '*')]}.", "high", tier=3)
            return updated_node
        module = cst.helpers.get_full_name_for_node(original_node.module)
        names = list(updated_node.names)
        for alias in original_node.names:
            name = cst.helpers.get_full_name_for_node(alias.name)
            if (module, name) in _GONE:
                self.report(original_node, f"from {module} import {name}: {_GONE[(module, name)]}.", "high", tier=3)
            if (module, name) in _MOVED:
                target = _MOVED[(module, name)]
                if len(original_node.names) == 1:
                    return self.rewrite(original_node, updated_node.with_changes(module=cst.parse_expression(target)),
                                        f"{module}.{name} moved to {target} in Spark 3.0; importing it from {module} is an "
                                        "ImportError on 3.5.5. Rewritten to import it from its new module.")
                self.report(original_node, f"{module}.{name} moved to {target} in Spark 3.0 -- import it from there.",
                            "high", tier=3)
        if module == "pyspark.ml.feature" and any(cst.helpers.get_full_name_for_node(a.name) == "OneHotEncoderEstimator"
                                                  for a in original_node.names):
            renamed = []
            for before, alias in zip(original_node.names, names):
                if cst.helpers.get_full_name_for_node(before.name) != "OneHotEncoderEstimator":
                    renamed.append(alias)
                elif before.asname is not None or not self.has_encoder:
                    renamed.append(alias.with_changes(name=cst.Name("OneHotEncoder")))
            renamed[-1] = renamed[-1].with_changes(comma=cst.MaybeSentinel.DEFAULT)
            return self.rewrite(original_node, updated_node.with_changes(names=renamed), self._encoder_message())
        return updated_node

    def _encoder_message(self) -> str:
        return ("OneHotEncoderEstimator was renamed OneHotEncoder in Spark 3.0 (the 2.4 transformer of that name was "
                "removed); same parameters, same OneHotEncoderModel. Importing it is an ImportError on 3.5.5. Rewritten "
                "to OneHotEncoder.")

    def leave_Name(self, original_node: cst.Name, updated_node: cst.Name) -> cst.Name:
        if original_node.value == "OneHotEncoderEstimator" and ENCODER_ESTIMATOR in self.qualified(original_node):
            return updated_node.with_changes(value="OneHotEncoder")
        return updated_node

    def leave_Attribute(self, original_node: cst.Attribute, updated_node: cst.Attribute) -> cst.BaseExpression:
        if original_node.attr.value == "OneHotEncoderEstimator" and ENCODER_ESTIMATOR in self.qualified(original_node):
            return self.rewrite(original_node, updated_node.with_changes(attr=cst.Name("OneHotEncoder")), self._encoder_message())
        return updated_node

    # -- calls
    def visit_Call(self, node: cst.Call) -> None:
        name = _cst.method_name(node)
        recv = _cst.receiver(node)
        if name == "computeCost":
            self.report(node, "computeCost(...) was removed from KMeansModel in Spark 3.0 (AttributeError; "
                              "BisectingKMeansModel keeps a deprecated one). For the training data, model.summary.trainingCost "
                              "is the same number; for other data, compute the squared distances to "
                              "model.clusterCenters(), or use pyspark.ml.evaluation.ClusteringEvaluator (a different metric, "
                              "silhouette).", "medium", tier=3)
        elif name == "transform" and self._is_old_encoder(recv):
            self.report(node, "OneHotEncoder(...).transform(...): in 2.4 OneHotEncoder was a Transformer; since Spark 3.0 "
                              "it is an Estimator (2.4's OneHotEncoderEstimator) and has no transform -- AttributeError. "
                              "Fit it first: encoder.fit(df).transform(df). In a Pipeline it needs no change.", "high", tier=3)
        elif name == "context" and isinstance(recv, cst.Call) and _cst.method_name(recv) in ("write", "read"):
            self.report(node, f".{_cst.method_name(recv)}().context(sqlContext) was removed from ML readers and writers "
                              "in Spark 3.0 -- use .session(spark).", "high", tier=3)

    def _is_old_encoder(self, node: cst.BaseExpression | None) -> bool:
        if isinstance(node, cst.Name):
            return node.value in self.encoders
        return self.has_encoder and isinstance(node, cst.Call) and isinstance(node.func, cst.Name) and \
            node.func.value == "OneHotEncoder" and ENCODER in self.qualified(node.func)
