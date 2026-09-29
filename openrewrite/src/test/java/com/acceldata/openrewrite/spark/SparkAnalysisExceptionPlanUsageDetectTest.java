package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkAnalysisExceptionPlanUsageDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkAnalysisExceptionPlanUsageDetect());
    }

    @Test
    void flagsPlanAccessor() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.AnalysisException;

                        class QueryRunner {
                            void logFailure(AnalysisException e) {
                                System.out.println(e.plan());
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.AnalysisException;

                        class QueryRunner {
                            void logFailure(AnalysisException e) {
                                System.out.println(/*~~(AnalysisException.plan moves to EnhancedAnalysisException in Spark 3.5; this call stops compiling unless the exception is narrowed to that type first.)~~>*/e.plan());
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesGetMessageUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.AnalysisException;

                        class QueryRunner {
                            void logFailure(AnalysisException e) {
                                System.out.println(e.getMessage());
                            }
                        }
                        """
                )
        );
    }
}
