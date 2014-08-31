/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

grammar Statement;

// TODO: error handling
//  - detect invalid chars in idents (@, :, etc)
//  - replace non-reserved tokens with ident with their text

singleStatement
    : statement EOF
    ;

singleExpression
    : expression EOF
    ;

statement
    : query
    | explainStmt
    | showTablesStmt
    | showSchemasStmt
    | showCatalogsStmt
    | showColumnsStmt
    | showPartitionsStmt
    | showFunctionsStmt
    | useCollectionStmt
    | createTableStmt
    | insertStmt
    | dropTableStmt
    | alterTableStmt
    | createViewStmt
    | dropViewStmt
    ;

query
    : withClause?
      ( orderOrLimitQuerySpec  // TODO: need syntactic predicate to resolve ambiguity: (orderOrLimitQuerySpec) =>
      //
//      | queryExprBody orderClause? limitClause?
      )
      approximateClause?
    ;

orderOrLimitQuerySpec
    : simpleQuery (orderClause limitClause? | limitClause)
    ;

// left-associative
queryExprBody
    : queryExprBody (UNION | EXCEPT) setQuant? queryExprBody
    | queryTerm
    ;

// left-associative
queryTerm
    : queryTerm INTERSECT setQuant? queryTerm
    | queryPrimary
    ;

queryPrimary
    : simpleQuery
    | subquery
    | tableExpression
    | inlineTableExpression
    ;

tableExpression
    : TABLE table
    ;

inlineTableExpression
    : VALUES rowValue (',' rowValue)*
    ;

rowValue
    : '(' expression (',' expression)* ')'
    ;

simpleQuery
    : selectClause
      fromClause?
      whereClause?
      groupClause?
      havingClause?
    ;

approximateClause
    : APPROXIMATE AT number CONFIDENCE
    ;

withClause
    : WITH RECURSIVE? withList
    ;

selectClause
    : SELECT selectExpr
    ;

fromClause
    : FROM tableRef (',' tableRef)*
    ;

whereClause
    : WHERE expression
    ;

groupClause
    : GROUP BY expression (',' expression)*
    ;

havingClause
    : HAVING expression
    ;

orderClause
    : ORDER BY sortItem (',' sortItem)*
    ;

limitClause
    : LIMIT integer
    ;

withList
    : withQuery (',' withQuery)*
    ;

withQuery
    : ident aliasedColumns? AS subquery
    ;

selectExpr
    : setQuant? selectItem (',' selectItem)*
    ;

setQuant
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? ident)?
    | qualifiedName '.' '*'
    | '*'
    ;

tableRef
    : tableRef
      ( CROSS JOIN tableFactor
      | joinType JOIN tableFactor joinCriteria
      | NATURAL joinType JOIN tableFactor
      )
    | tableFactor
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    | POISSONIZED
    ;

stratifyOn
    : STRATIFY ON '(' expression (',' expression)* ')'
    ;

tableFactor
    : tablePrimary ( TABLESAMPLE sampleType '(' expression ')' RESCALED? stratifyOn? )?
    ;

tablePrimary
    : relation ( AS? ident aliasedColumns? )?
    ;

relation
    : table
    | joinedTable
    | subquery
    ;

table
    : qualifiedName
    ;

joinedTable
    : '(' tableRef ')'
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON expression
    | USING '(' ident (',' ident)* ')'
    ;

aliasedColumns
    : '(' ident (',' ident)* ')'
    ;

// 1 = 2 is null            => 1 = (2 is null)
// false = null is null     => false = (null is null)
// 1 BETWEEN 2 AND 3 BETWEEN 4 AND 5 => (1 BETWEEN 2 AND 3) BETWEEN 4 AND 5
// 'a' || 'b' IS NULL => 'a' || ('b' IS NULL)


expression
    : booleanExpression;

booleanExpression
    : NOT booleanExpression
    | booleanExpression AND booleanExpression
    | booleanExpression OR booleanExpression
    | EXISTS subquery
    | comparisonExpression
    ;

comparisonExpression
    : comparisonExpression BETWEEN comparisonExpression AND comparisonExpression
    | comparisonExpression NOT BETWEEN comparisonExpression AND comparisonExpression
    | comparisonExpression LIKE comparisonExpression (ESCAPE comparisonExpression)?
    | comparisonExpression NOT LIKE comparisonExpression (ESCAPE comparisonExpression)?
    | comparisonExpression IN inList
    | comparisonExpression NOT IN inList
    | expressionTerm
    ;

expressionTerm
    : literal
    | qualifiedName
    | functionCall
    | caseExpression
    | '(' expression ')'
    | subquery
    | expressionTerm ( AT TIME ZONE STRING | AT TIME ZONE intervalLiteral)
    | ('+' | '-') expressionTerm
    | expressionTerm ('*' | '/' | '%') expressionTerm
    | expressionTerm ('+' | '-') expressionTerm
    | expressionTerm IS NOT? NULL
    | expressionTerm '||' expressionTerm
    | expressionTerm cmpOp expressionTerm
    ;

functionCall
    : qualifiedName '(' '*' ')' over?
    | qualifiedName '(' (setQuant? expression (',' expression)*)? ')' over?
    | specialFunction
    ;

specialFunction
    : CURRENT_DATE
    | CURRENT_TIME ('(' integer ')')?
    | CURRENT_TIMESTAMP ('(' integer ')')?
    | LOCALTIME ('(' integer ')')?
    | LOCALTIMESTAMP ('(' integer ')')?
    | SUBSTRING '(' expression FROM expression (FOR expression)? ')'
    | EXTRACT '(' ident FROM expression ')'
    | CAST '(' expression AS type ')'
    | TRY_CAST '(' expression AS type ')'
    ;

inList
    : '(' expression (',' expression)* ')'
    | subquery
    ;

sortItem
    : expression ordering nullOrdering?
    ;

ordering
    :
    | ASC
    | DESC
    ;

nullOrdering
    : NULLS FIRST
    | NULLS LAST
    ;

cmpOp
    : EQ | NEQ | LT | LTE | GT | GTE | IS DISTINCT FROM | IS NOT DISTINCT FROM
    ;

subquery
    : '(' query ')'
    ;

literal
    : NULL
    | VARCHAR STRING
    | BIGINT STRING
    | DOUBLE STRING
    | BOOLEAN STRING
    | DATE STRING
    | TIME STRING
    | TIMESTAMP STRING
    | intervalLiteral
    | ident STRING
    | number
    | bool
    | STRING
    ;

intervalLiteral
    : INTERVAL intervalSign? STRING intervalField ( TO intervalField )?
    ;

intervalSign
    : '+'
    | '-'
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

// TODO: this should be 'dataType', which supports arbitrary type specifications. For now we constrain to simple types
type
    : VARCHAR
    | BIGINT
    | DOUBLE
    | BOOLEAN
    | TIME WITH TIME ZONE
    | TIMESTAMP WITH TIME ZONE
    | ident
    ;

caseExpression
    : CASE expression whenClause+ elseClause? END
    | CASE whenClause+ elseClause? END
    ;

whenClause
    : WHEN expression THEN expression
    ;

elseClause
    : ELSE expression
    ;

over
    : OVER '(' window ')'
    ;

window
    : windowPartition? orderClause? windowFrame?
    ;

windowPartition
    : PARTITION BY expression (',' expression)*
    ;

windowFrame
    : RANGE frameBound
    | ROWS frameBound
    | RANGE BETWEEN frameBound AND frameBound
    | ROWS BETWEEN frameBound AND frameBound
    ;

frameBound
    : UNBOUNDED PRECEDING
    | UNBOUNDED FOLLOWING
    | CURRENT ROW
    | expression ( PRECEDING | FOLLOWING )
    ;

useCollectionStmt
    : USE CATALOG ident
    | USE SCHEMA ident
    ;

explainStmt
    : EXPLAIN explainOptions? statement
    ;

explainOptions
    : '(' explainOption (',' explainOption)* ')'
    ;

explainOption
    : FORMAT TEXT
    | FORMAT GRAPHVIZ
    | FORMAT JSON
    | TYPE LOGICAL
    | TYPE DISTRIBUTED
    ;

showTablesStmt
    : SHOW TABLES showTablesFrom? showTablesLike?
    ;

showTablesFrom
    : (FROM | IN) qualifiedName
    ;

showTablesLike
    : LIKE STRING
    ;

showSchemasStmt
    : SHOW SCHEMAS showSchemasFrom?
    ;

showSchemasFrom
    : (FROM | IN) ident
    ;

showCatalogsStmt
    : SHOW CATALOGS
    ;

showColumnsStmt
    : SHOW COLUMNS (FROM | IN) qualifiedName
    | DESCRIBE qualifiedName
    | DESC qualifiedName
    ;

showPartitionsStmt
    : SHOW PARTITIONS (FROM | IN) qualifiedName whereClause? orderClause? limitClause?
    ;

showFunctionsStmt
    : SHOW FUNCTIONS
    ;

dropTableStmt
    : DROP TABLE qualifiedName
    ;

insertStmt
    : INSERT INTO qualifiedName query
    ;

createTableStmt
    : CREATE TABLE qualifiedName tableContentsSource
    ;

alterTableStmt
    : ALTER TABLE qualifiedName RENAME TO qualifiedName
    ;

createViewStmt
    : CREATE orReplace? VIEW qualifiedName tableContentsSource
    ;

dropViewStmt
    : DROP VIEW qualifiedName
    ;

orReplace
    : OR REPLACE
    ;

tableContentsSource
    : AS query
    ;

tableElementList
    : '(' tableElement (',' tableElement)* ')'
    ;

tableElement
    : ident dataType columnConstDef*
    ;

dataType
    : charType
    | exactNumType
    | dateType
    ;

charType
    : CHAR charlen?
    | CHARACTER charlen?
    | VARCHAR charlen?
    | CHAR VARYING charlen?
    | CHARACTER VARYING charlen?
    ;

charlen
    : '(' integer ')'
    ;

exactNumType
    : NUMERIC numlen?
    | DECIMAL numlen?
    | DEC numlen?
    | INTEGER
    | INT
    ;

numlen
    : '(' integer (',' integer)? ')'
    ;

dateType
    : DATE
    ;

columnConstDef
    : columnConst
    ;

columnConst
    : NOT NULL
    ;

qualifiedName
    : ident ('.' ident)*
    ;

ident
    : IDENT
    | QUOTED_IDENT
    | nonReserved
    ;

number
    : DECIMAL_VALUE
    | INTEGER_VALUE
    ;

bool
    : TRUE
    | FALSE
    ;

integer
    : INTEGER_VALUE
    ;

nonReserved
    : SHOW | TABLES | COLUMNS | PARTITIONS | FUNCTIONS | SCHEMAS | CATALOGS
    | OVER | PARTITION | RANGE | ROWS | PRECEDING | FOLLOWING | CURRENT | ROW
    | DATE | TIME | TIMESTAMP | INTERVAL
    | YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    | EXPLAIN | FORMAT | TYPE | TEXT | GRAPHVIZ | LOGICAL | DISTRIBUTED
    | TABLESAMPLE | SYSTEM | BERNOULLI | POISSONIZED | USE | SCHEMA | CATALOG | JSON | TO
    | RESCALED | APPROXIMATE | AT | CONFIDENCE
    | VIEW | REPLACE
    ;

SELECT: 'SELECT';
FROM: 'FROM';
AS: 'AS';
ALL: 'ALL';
DISTINCT: 'DISTINCT';
WHERE: 'WHERE';
GROUP: 'GROUP';
BY: 'BY';
ORDER: 'ORDER';
HAVING: 'HAVING';
LIMIT: 'LIMIT';
APPROXIMATE: 'APPROXIMATE';
AT: 'AT';
CONFIDENCE: 'CONFIDENCE';
OR: 'OR';
AND: 'AND';
IN: 'IN';
NOT: 'NOT';
EXISTS: 'EXISTS';
BETWEEN: 'BETWEEN';
LIKE: 'LIKE';
IS: 'IS';
NULL: 'NULL';
TRUE: 'TRUE';
FALSE: 'FALSE';
NULLS: 'NULLS';
FIRST: 'FIRST';
LAST: 'LAST';
ESCAPE: 'ESCAPE';
ASC: 'ASC';
DESC: 'DESC';
SUBSTRING: 'SUBSTRING';
FOR: 'FOR';
DATE: 'DATE';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
INTERVAL: 'INTERVAL';
YEAR: 'YEAR';
MONTH: 'MONTH';
DAY: 'DAY';
HOUR: 'HOUR';
MINUTE: 'MINUTE';
SECOND: 'SECOND';
ZONE: 'ZONE';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
LOCALTIME: 'LOCALTIME';
LOCALTIMESTAMP: 'LOCALTIMESTAMP';
EXTRACT: 'EXTRACT';
COALESCE: 'COALESCE';
NULLIF: 'NULLIF';
CASE: 'CASE';
WHEN: 'WHEN';
THEN: 'THEN';
ELSE: 'ELSE';
END: 'END';
IF: 'IF';
JOIN: 'JOIN';
CROSS: 'CROSS';
OUTER: 'OUTER';
INNER: 'INNER';
LEFT: 'LEFT';
RIGHT: 'RIGHT';
FULL: 'FULL';
NATURAL: 'NATURAL';
USING: 'USING';
ON: 'ON';
OVER: 'OVER';
PARTITION: 'PARTITION';
RANGE: 'RANGE';
ROWS: 'ROWS';
UNBOUNDED: 'UNBOUNDED';
PRECEDING: 'PRECEDING';
FOLLOWING: 'FOLLOWING';
CURRENT: 'CURRENT';
ROW: 'ROW';
WITH: 'WITH';
RECURSIVE: 'RECURSIVE';
VALUES: 'VALUES';
CREATE: 'CREATE';
TABLE: 'TABLE';
VIEW: 'VIEW';
REPLACE: 'REPLACE';
INSERT: 'INSERT';
INTO: 'INTO';
CHAR: 'CHAR';
CHARACTER: 'CHARACTER';
VARYING: 'VARYING';
VARCHAR: 'VARCHAR';
NUMERIC: 'NUMERIC';
NUMBER: 'NUMBER';
DECIMAL: 'DECIMAL';
DEC: 'DEC';
INTEGER: 'INTEGER';
INT: 'INT';
DOUBLE: 'DOUBLE';
BIGINT: 'BIGINT';
BOOLEAN: 'BOOLEAN';
CONSTRAINT: 'CONSTRAINT';
DESCRIBE: 'DESCRIBE';
EXPLAIN: 'EXPLAIN';
FORMAT: 'FORMAT';
TYPE: 'TYPE';
TEXT: 'TEXT';
GRAPHVIZ: 'GRAPHVIZ';
JSON: 'JSON';
LOGICAL: 'LOGICAL';
DISTRIBUTED: 'DISTRIBUTED';
CAST: 'CAST';
TRY_CAST: 'TRY_CAST';
SHOW: 'SHOW';
TABLES: 'TABLES';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
CATALOG: 'CATALOG';
CATALOGS: 'CATALOGS';
COLUMNS: 'COLUMNS';
USE: 'USE';
PARTITIONS: 'PARTITIONS';
FUNCTIONS: 'FUNCTIONS';
DROP: 'DROP';
UNION: 'UNION';
EXCEPT: 'EXCEPT';
INTERSECT: 'INTERSECT';
TO: 'TO';
SYSTEM: 'SYSTEM';
BERNOULLI: 'BERNOULLI';
POISSONIZED: 'POISSONIZED';
TABLESAMPLE: 'TABLESAMPLE';
RESCALED: 'RESCALED';
STRATIFY: 'STRATIFY';
ALTER: 'ALTER';
RENAME: 'RENAME';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    | DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENT
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@' | ':')*
    ;

DIGIT_IDENT
    : DIGIT (LETTER | DIGIT | '_' | '@' | ':')+
    ;

QUOTED_IDENT
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENT
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

COMMENT
    : (
        '--' ~[\r\n]* '\r'? '\n'?
        | '/*' .*? '*/'
      ) -> skip
    ;

WS
    : [ \r\n\t]+ -> skip
    ;
