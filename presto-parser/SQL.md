= Deviations from ANSI SQL

* CREATE TABLE AS SELECT

    <table definition>
      : CREATE <table scope>? TABLE <table name> <table contents source>
          (WITH <system versioning clause>)?
          (ON COMMIT <table commit action> ROWS)?
      ;

    <table contents source>
      : <table element list>
      | <typed table clause>
      | <as subquery clause>
      ;

    <as subquery clause>
      : (<left paren> <column name list> <right paren>)? AS <table subquery> <with or without data>
      ;

    <with or without data>
      : WITH NO DATA
      | WITH DATA
      ;

    <column name list>
      : <column name> ( <comma> <column name> )*
      ;

    <table subquery>
      : <subquery>
      ;

    <subquery>
      : <left paren> <query expression> <right paren>
      ;

- The "WITH DATA" / "WITH NO DATA" clause is mandatory. This option is not supported or expected in Presto
- The <subquery> in <table subquery> must have parentheses around it. Presto allows a non-parenthesized subquery.

= LIMIT

ANSI SQL does not actually support the LIMIT clause. The corresponding syntax is:

    <fetch first clause>
      : FETCH ( FIRST | NEXT ) <fetch first row count>? ( ROW | ROWS ) ONLY
      ;

    <fetch first row count>
      : <simple value specification>
      ;

- <fetch first row count> must be a number >= 1. Presto allows the limit to be 0

Example 1:

    SELECT * FROM t LIMIT 10

is equivalent to

    SELECT * FROM t FETCH FIRST 10 ROWS ONLY

Example 2:

    SELECT * FROM t LIMIT 1

is equivalent to

    SELECT * FROM t FETCH FIRST ROW ONLY
    SELECT * FROM t FETCH FIRST 1 ROW ONLY

