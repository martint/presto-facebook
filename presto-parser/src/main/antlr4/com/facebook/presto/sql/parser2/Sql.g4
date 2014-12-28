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

grammar Sql;

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
    | EXPLAIN explainOptions? statement
    | SHOW TABLES ((FROM | IN) qualifiedName)? (LIKE STRING)?
    | SHOW SCHEMAS ((FROM | IN) ident )?
    | SHOW CATALOGS
    | SHOW COLUMNS (FROM | IN) qualifiedName
    | DESCRIBE qualifiedName
    | DESC qualifiedName
    | SHOW PARTITIONS (FROM | IN) qualifiedName whereClause? orderClause? limitClause?
    | SHOW FUNCTIONS
    | USE ident ('.' ident)?
    | CREATE TABLE qualifiedName AS query
    | DROP TABLE qualifiedName
    | INSERT INTO qualifiedName query
    | ALTER TABLE qualifiedName RENAME TO qualifiedName
    | CREATE (OR REPLACE)? VIEW qualifiedName AS query
    | DROP VIEW qualifiedName
    ;

query
    : (WITH RECURSIVE? namedQuery (',' namedQuery)*)?
      queryBody
      orderClause?
      limitClause?
      (APPROXIMATE AT number CONFIDENCE)?
    ;

queryBody
    : simpleQuery
    | '(' query ')'
    | TABLE qualifiedName
    | VALUES primary (',' primary)*
    | queryBody INTERSECT setQuantifier? queryBody
    | queryBody (UNION | EXCEPT) setQuantifier? queryBody
    ;

orderClause
    : ORDER BY sortItem (',' sortItem)*
    ;

sortItem
    : expression ordering? nullOrdering?
    ;

limitClause
    : LIMIT integer
    ;

simpleQuery
    : SELECT setQuantifier? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      whereClause?
      (GROUP BY expression (',' expression)*)?
      (HAVING booleanExpression)?
    ;

namedQuery
    : ident columnAliases? AS '(' query ')'
    ;

whereClause
    : WHERE booleanExpression
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? ident)?
    | qualifiedName '.' '*'
    | '*'
    ;

relation
    : relation
      ( CROSS JOIN relation
      | joinType JOIN relation joinCriteria
      | NATURAL joinType JOIN relation
      )
    | sampledRelation
    ;

sampledRelation
    : aliasedRelation (TABLESAMPLE sampleType '(' expression ')' RESCALED? stratifyOn?)?
    ;

aliasedRelation
    : (
        qualifiedName
      | '(' query ')'
      | UNNEST '(' expression (',' expression)* ')')
      (AS? ident columnAliases?)?
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    | POISSONIZED
    ;

stratifyOn
    : STRATIFY ON '(' expression (',' expression)* ')'
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | USING '(' ident (',' ident)* ')'
    ;

columnAliases
    : '(' ident (',' ident)* ')'
    ;

// 1 = 2 is null            => 1 = (2 is null)
// false = null is null     => false = (null is null)
// 1 BETWEEN 2 AND 3 BETWEEN 4 AND 5 => (1 BETWEEN 2 AND 3) BETWEEN 4 AND 5
// 'a' || 'b' IS NULL => 'a' || ('b' IS NULL)

expression
    : valueExpression
    | booleanExpression
    ;

// TODO: precedence
booleanExpression
    : booleanExpression IS NOT? (TRUE | FALSE | UNKNOWN)
    | NOT booleanExpression
    | booleanExpression AND booleanExpression
    | booleanExpression OR booleanExpression
    | valueExpression comparisonOperator valueExpression
    | valueExpression NOT? BETWEEN valueExpression AND valueExpression // TODO: SYMMETRIC/ASYMETRIC
    | valueExpression NOT? IN '(' primary (',' primary)* ')'
    | valueExpression NOT? IN '(' query ')'
    | valueExpression NOT? LIKE valueExpression (ESCAPE valueExpression)? // TODO
    | valueExpression IS NOT? NULL
    | valueExpression comparisonOperator (ALL | SOME | ANY) '(' query ')' // TODO: add later
    | EXISTS '(' query ')'
    | UNIQUE '(' query ')' // TODO: add later
    | valueExpression MATCH UNIQUE? (SIMPLE | PARTIAL | FULL) '(' query ')' // TODO: add later
    | valueExpression OVERLAPS valueExpression // TODO: add later
    | valueExpression IS NOT? DISTINCT FROM valueExpression
    | valueExpression NOT? MEMBER OF? valueExpression // TODO add later
    | valueExpression NOT? SUBMULTISET OF? valueExpression // TODO add later
    | valueExpression IS NOT? A SET // TODO add later
    | primary
    ;

valueExpression
    : primary
    | valueExpression intervalQualifier
    | '-' valueExpression
    | valueExpression AT timeZoneSpecifier
    | valueExpression COLLATE (ident '.')? ident
    | valueExpression ('*' | '/') valueExpression
    | valueExpression ('+' | '-') valueExpression
    | valueExpression '||' valueExpression
    ;

primary
    : literal
    | qualifiedName
    | functionCall
    | '(' expression ',' expression (',' expression)* ')' // row expression
    | ROW '(' expression (',' expression)* ')'
    | '(' query ')'
    | CASE valueExpression whenClause+ elseClause? END
    | CASE whenClause+ elseClause? END
    | '(' expression ')' ('.' ident)?
    | primary '.' ident
    | CAST '(' expression AS type ')'
    | TRY_CAST '(' expression AS type ')'
    | ARRAY '[' (expression (',' expression)*)? ']'
    | primary '[' valueExpression ']' // TODO: valueExpression '[' ... ']' ?
    | ELEMENT '(' valueExpression ')'
    | CURRENT_DATE
    | CURRENT_TIME ('(' integer ')')?
    | CURRENT_TIMESTAMP ('(' integer ')')?
    | LOCALTIME ('(' integer ')')?
    | LOCALTIMESTAMP ('(' integer ')')?
    | SUBSTRING '(' valueExpression FROM valueExpression (FOR valueExpression)? ')'
    | EXTRACT '(' ident FROM valueExpression ')'
    ;

timeZoneSpecifier
    : LOCAL
    | TIME ZONE (intervalLiteral | STRING)
    ;

intervalQualifier
    : intervalField TO intervalField
    | intervalField
    ;

functionCall
    : qualifiedName '(' '*' ')' over?
    | qualifiedName '(' (setQuantifier? expression (',' expression)*)? ')' over?
    ;

ordering
    : ASC
    | DESC
    ;

nullOrdering
    : NULLS FIRST
    | NULLS LAST
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
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
    : INTERVAL ('+' | '-')? STRING intervalField ( TO intervalField )?
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

type
    : VARCHAR
    | BIGINT
    | DOUBLE
    | BOOLEAN
    | TIME WITH TIME ZONE
    | TIMESTAMP WITH TIME ZONE
    | ident
    ;


// TODO: special form of simple CASE:
// CASE x
//   WHEN > 3 THEN ...
//   WHEN DISTINCT FROM 4 THEN ...

whenClause
    : WHEN booleanExpression THEN expression
    ;

elseClause
    : ELSE expression
    ;

over
    : OVER '(' windowPartition? orderClause? windowFrame? ')'
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
    | expression ( PRECEDING | FOLLOWING ) // valueExpression should be unsignedLiteral
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
    | TABLESAMPLE | SYSTEM | BERNOULLI | POISSONIZED | USE | JSON | TO
    | RESCALED | APPROXIMATE | AT | CONFIDENCE
    | VIEW | REPLACE
    | A
    ;

SELECT: 'SELECT';
FROM: 'FROM';
AS: 'AS';
ALL: 'ALL';
SOME: 'SOME';
ANY: 'ANY';
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
CASE: 'CASE';
WHEN: 'WHEN';
THEN: 'THEN';
ELSE: 'ELSE';
END: 'END';
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
SCHEMAS: 'SCHEMAS';
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
UNNEST: 'UNNEST';
ARRAY: 'ARRAY';
UNIQUE: 'UNIQUE';
MATCH: 'MATCH';
SIMPLE: 'SIMPLE';
PARTIAL: 'PARTIAL';
OVERLAPS: 'OVERLAPS';
MEMBER: 'MEMBER';
OF: 'OF';
SUBMULTISET: 'SUBMULTISET';
A: 'A';
SET: 'SET';
UNKNOWN: 'UNKNOWN';
COLLATE: 'COLLATE';
LOCAL: 'LOCAL';
ELEMENT: 'ELEMENT';

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
