# Correlate migration to master

- Base: local `master` at `af3b972` (Flink 1.16.3).
- Source: local `Flink-1.17_ctrip` at `87fbace56c5d08cd68bd760568c32750548f2654`.
- Branch: `migrate-correlate-to-master` (paired with the same branch in OmniStream).

Migrates CommonExecCorrelate metadata generation, nested argument serialization,
native validation and registration, performance-mode and SQL graph support, and
Correlate native input metrics. Existing master Source metrics are preserved.
The native function whitelist remains `jsontest` and `nrejsontest`.

The code-generation call uses Flink 1.16.3's 12-argument signature instead of
Flink 1.17's 13-argument signature. Master's existing RexNodeUtil and type
validation are retained rather than replaced with their older branch versions.

Validation (2026-10-14):

- Java extension: all 209 production sources compiled successfully with Java 8.
- Table planner: all 25 production sources compiled against Flink 1.16.3.
- All 14 Java test sources compiled.
- Three new Correlate validation tests and two existing NOT NULL type tests
  passed by direct invocation with the real JUnit assertions. Maven Surefire
  could not run offline because its transitive plugin dependencies were missing.
- `git diff --check` passed. Native and full Flink integration tests were not run.

Standard test command from the repository root when Maven dependencies are available:

```sh
mvn -f omnistream/omniop-flink-extension/java/pom.xml \
  -Dtest=ValidateCorrelateOPStrategyTest,AbstractValidateOperatorStrategyNotNullTest test
```
