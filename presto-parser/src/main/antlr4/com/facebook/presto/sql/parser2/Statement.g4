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
    : expr EOF
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
    : queryExpr
    ;

queryExpr
    : withClause?
      ( orderOrLimitQuerySpec
      | queryExprBody orderClause? limitClause?
      )
      approximateClause?
    ;

orderOrLimitQuerySpec
    : simpleQuery (orderClause limitClause? | limitClause)
    ;

// left-associative
queryExprBody
    : queryExprBody UNION setQuant? queryExprBody
    | queryExprBody EXCEPT setQuant? queryExprBody
    | queryTerm
    ;

// left-associative
queryTerm
    : queryTerm INTERSECT setQuant? queryTerm
    | queryPrimary
    ;

queryPrimary
    : simpleQuery
    | tableSubquery
    | explicitTable
    | tableValue
    ;

explicitTable
    : TABLE table
    ;

tableValue
    : VALUES rowValue (',' rowValue)*
    ;

rowValue
    : '(' expr (',' expr)* ')'
    ;

simpleQuery
    : selectClause
      fromClause?
      whereClause?
      groupClause?
      havingClause?
    ;

restrictedSelectStmt
    : selectClause
      fromClause
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
    : WHERE expr
    ;

groupClause
    : GROUP BY expr (',' expr)*
    ;

havingClause
    : HAVING expr
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
    : setQuant? selectList
    ;

setQuant
    : DISTINCT
    | ALL
    ;

selectList
    : selectSublist (',' selectSublist)*
    ;

selectSublist
    : expr (AS? ident)?
    | qname '.' '*'
    | '*'
    ;

tableRef
    : tableFactor
      ( CROSS JOIN tableFactor
      | joinType JOIN tableFactor joinCriteria
      | NATURAL joinType JOIN tableFactor
      )*
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    | POISSONIZED
    ;

stratifyOn
    : STRATIFY ON '(' expr (',' expr)* ')'
    ;

tableFactor
    : tablePrimary ( TABLESAMPLE sampleType '(' expr ')' RESCALED? stratifyOn? )?
    ;

tablePrimary
    : relation ( AS? ident aliasedColumns? )?
    ;

relation
    : table
    | joinedTable
    | tableSubquery
    ;

table
    : qname
    ;

tableSubquery
    : '(' query ')'
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
    : ON expr
    | USING '(' ident (',' ident)* ')'
    ;

aliasedColumns
    : '(' ident (',' ident)* ')'
    ;

expr
    : orExpression
    ;

orExpression
    : andExpression (OR andExpression)*
    ;

andExpression
    : notExpression (AND notExpression)*
    ;

notExpression
    : (NOT)* booleanTest
    ;

booleanTest
    : booleanPrimary
    ;

booleanPrimary
    : predicate
    | EXISTS subquery
    ;

predicate
    : predicatePrimary
      ( cmpOp predicatePrimary
      | IS DISTINCT FROM predicatePrimary
      | IS NOT DISTINCT FROM predicatePrimary
      | BETWEEN predicatePrimary AND predicatePrimary
      | NOT BETWEEN predicatePrimary AND predicatePrimary
      | LIKE predicatePrimary (ESCAPE predicatePrimary)?
      | NOT LIKE predicatePrimary (ESCAPE predicatePrimary)?
      | IS NULL
      | IS NOT NULL
      | IN inList
      | NOT IN inList
      )*
    ;

predicatePrimary
    : numericExpr ( '||' e=numericExpr )*
    ;

numericExpr
    : numericTerm (('+' | '-') numericTerm)*
    ;

numericTerm
    : numericFactor (('*' | '/' | '%') numericFactor)*
    ;

numericFactor
    : exprWithTimeZone
    | '+' numericFactor
    | '-' numericFactor
    ;

exprWithTimeZone
    : exprPrimary ( AT TIME ZONE STRING | AT TIME ZONE intervalLiteral )?
    ;

exprPrimary
    : NULL
    | literal
    | qnameOrFunction
    | specialFunction
    | number
    | bool
    | STRING
    | caseExpression
    | '(' expr ')'
    | subquery
    ;

qnameOrFunction
    : qname
      ( '(' '*' ')' over?
      | '(' (setQuant? expr (',' expr)*)? ')'
      )?
    ;

inList
    : '(' expr (',' expr)* ')'
    | subquery
    ;

sortItem
    : expr ordering nullOrdering?
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
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

subquery
    : '(' query ')'
    ;

literal
    : VARCHAR STRING
    | BIGINT STRING
    | DOUBLE STRING
    | BOOLEAN STRING
    | DATE STRING
    | TIME STRING
    | TIMESTAMP STRING
    | intervalLiteral
    | ident STRING
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

specialFunction
    : CURRENT_DATE
    | CURRENT_TIME ('(' integer ')')?
    | CURRENT_TIMESTAMP ('(' integer ')')?
    | LOCALTIME ('(' integer ')')?
    | LOCALTIMESTAMP ('(' integer ')')?
    | SUBSTRING '(' expr FROM expr (FOR expr)? ')'
    | EXTRACT '(' ident FROM expr ')'
    | CAST '(' expr AS type ')'
    | TRY_CAST '(' expr AS type ')'
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
    : CASE expr whenClause+ elseClause? END
    | CASE whenClause+ elseClause? END
    ;

whenClause
    : WHEN expr THEN expr
    ;

elseClause
    : ELSE expr
    ;

over
    : OVER '(' window ')'
    ;

window
    : p=windowPartition? o=orderClause? f=windowFrame?
    ;

windowPartition
    : PARTITION BY expr (',' expr)*
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
    | expr ( PRECEDING | FOLLOWING )
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
    : (FROM | IN) qname
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
    : SHOW COLUMNS (FROM | IN) qname
    | DESCRIBE qname
    | DESC qname
    ;

showPartitionsStmt
    : SHOW PARTITIONS (FROM | IN) qname whereClause? orderClause? limitClause?
    ;

showFunctionsStmt
    : SHOW FUNCTIONS
    ;

dropTableStmt
    : DROP TABLE qname
    ;

insertStmt
    : INSERT INTO qname query
    ;

createTableStmt
    : CREATE TABLE qname tableContentsSource
    ;

alterTableStmt
    : ALTER TABLE qname RENAME TO qname
    ;

createViewStmt
    : CREATE orReplace? VIEW qname tableContentsSource
    ;

dropViewStmt
    : DROP VIEW qname
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

qname
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
    : 'E' ('+' | '-')? DIGIT+
    ;

fragment DIGIT
    : '0'..'9'
    ;

fragment LETTER
    : 'A'..'Z'
    ;

COMMENT
    : (
        '--' (~('\r' | '\n'))* ('\r'? '\n')?
        | '/*' .*? '*/'
      ) -> skip
    ;

WS
    : (' ' | '\t' | '\n' | '\r')+ -> skip
    ;
