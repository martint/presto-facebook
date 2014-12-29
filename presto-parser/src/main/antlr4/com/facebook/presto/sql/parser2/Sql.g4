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

singleStatement
    : statement EOF
    ;

singleExpression
    : expression EOF
    ;

statement
    : query
    | USE identifier ('.' identifier)?
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
    : queryPrimary
    | queryBody INTERSECT setQuantifier? queryBody
    | queryBody (UNION | EXCEPT) setQuantifier? queryBody
    ;

queryPrimary
    : simpleQuery #queryBlock
    | '(' query ')' #subquery
    | TABLE qualifiedName #table
    | VALUES primaryExpression (',' primaryExpression)* #inlineTable
    | EXPLAIN explainOptions? statement #explain
    | SHOW TABLES ((FROM | IN) qualifiedName)? (LIKE STRING)? #showTables
    | SHOW SCHEMAS ((FROM | IN) identifier )? #showSchemas
    | SHOW CATALOGS #showCatalogs
    | SHOW COLUMNS (FROM | IN) qualifiedName #showColumns
    | DESCRIBE qualifiedName #describe
    | DESC qualifiedName #describe
    | SHOW PARTITIONS (FROM | IN) qualifiedName (WHERE booleanExpression)? orderClause? limitClause? #showPartitions
    | SHOW FUNCTIONS #showFunctions
    ;

orderClause
    : ORDER BY sortItem (',' sortItem)*
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

limitClause
    : LIMIT INTEGER_VALUE
    ;

simpleQuery
    : SELECT setQuantifier? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      (WHERE booleanExpression)?
      (GROUP BY expression (',' expression)*)?
      (HAVING booleanExpression)?
    ;

namedQuery
    : identifier columnAliases? AS '(' query ')'
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?
    | qualifiedName '.' ASTERISK
    | ASTERISK
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
        qualifiedName // table name reference
        | '(' query ')' // subquery expression
        | UNNEST '(' expression (',' expression)* ')' // table function
      )
      (AS? identifier columnAliases?)?
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
    | USING '(' identifier (',' identifier)* ')'
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

expression
    : valueExpression
    | booleanExpression
    ;

// TODO: precedence
booleanExpression
    // booleanExpression IS NOT? (TRUE | FALSE | UNKNOWN) // TODO: add later
    //    #truth
    : NOT booleanExpression #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression #logicalBinary
    | left=valueExpression comparisonOperator right=valueExpression #comparison
    | value=valueExpression NOT? BETWEEN lower=valueExpression AND upper=valueExpression #between // TODO: SYMMETRIC/ASYMETRIC
        // TODO: valueExpression NOT? IN valueExpression ?
    | valueExpression NOT? IN '(' primaryExpression (',' primaryExpression)* ')' #inList
    | valueExpression NOT? IN '(' query ')' #inSubquery
    | value=valueExpression NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)? #like
    | valueExpression IS NOT? NULL #nullPredicate
//    | valueExpression comparisonOperator (ALL | SOME | ANY) '(' query ')' // TODO: add later
//        #quantifiedComparison
    | EXISTS '(' query ')' #exists
//    | UNIQUE '(' query ')' // TODO: add later
//        #unique
//    | valueExpression MATCH UNIQUE? (SIMPLE | PARTIAL | FULL) '(' query ')' // TODO: add later
//        #match
//    | valueExpression OVERLAPS valueExpression // TODO: add later
//        #overlaps
    | left=valueExpression IS NOT? DISTINCT FROM right=valueExpression #distinctFrom
//    | valueExpression NOT? MEMBER OF? valueExpression // TODO add later
//        #memberOf
//    | valueExpression NOT? SUBMULTISET OF? valueExpression // TODO add later
//        #submultiset
//    | valueExpression IS NOT? A SET // TODO add later
//        #setPredicate
    | primaryExpression #booleanPrimary
    ;

valueExpression
    : primaryExpression #valuePrimary
//    | valueExpression intervalField (TO intervalField)? #intervalConversion
    | MINUS valueExpression #arithmeticNegation
    | valueExpression AT timeZoneSpecifier #atTimeZone
//    | valueExpression COLLATE (identifier '.')? identifier #collated
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression #concatenation
    ;

primaryExpression
    : NULL #nullLiteral
    | interval #intervalLiteral
    | identifier STRING #typeConstructor
    | number #numericLiteral
    | booleanValue #booleanLiteral
    | STRING #stringLiteral
    | qualifiedName #columnReference
    | qualifiedName '(' ASTERISK ')' over? #functionCall
    | qualifiedName '(' (setQuantifier? expression (',' expression)*)? ')' over? #functionCall
//    | '(' expression ',' expression (',' expression)* ')'  #rowConstructor
//    | ROW '(' expression (',' expression)* ')' #rowConstuctor
    | '(' query ')' #subqueryExpression
    | CASE valueExpression whenClause+ (ELSE elseExpression=expression)? END #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END #searchedCase
//    | primaryExpression '.' identifier #fieldReference TODO: later
    | CAST '(' expression AS type ')' #cast
    | TRY_CAST '(' expression AS type ')' #cast
    | ARRAY '[' (expression (',' expression)*)? ']' #arrayConstructor
    | value=primaryExpression '[' index=valueExpression ']' #subscript // TODO: valueExpression '[' ... ']' ?
//    | ELEMENT '(' valueExpression ')'  #element // TODO: add later
    | name=CURRENT_DATE #specialDateTimeFunction
    | name=CURRENT_TIME ('(' precision=INTEGER_VALUE ')')? #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP ('(' precision=INTEGER_VALUE ')')? #specialDateTimeFunction
    | name=LOCALTIME ('(' precision=INTEGER_VALUE ')')? #specialDateTimeFunction
    | name=LOCALTIMESTAMP ('(' precision=INTEGER_VALUE ')')? #specialDateTimeFunction
    | SUBSTRING '(' valueExpression FROM valueExpression (FOR valueExpression)? ')' #substring
    | EXTRACT '(' identifier FROM valueExpression ')' #extract
    | '(' expression ')' #subExpression
    ;

timeZoneSpecifier
    : LOCAL
    | TIME ZONE (interval | STRING)
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE
    | FALSE
    ;

interval
    : INTERVAL (PLUS | MINUS)? STRING intervalField ( TO intervalField )?
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

type
    : TIME WITH TIME ZONE
    | TIMESTAMP WITH TIME ZONE
    | identifier
    ;


// TODO: special form of simple CASE:
// CASE x
//   WHEN > 3 THEN ...
//   WHEN DISTINCT FROM 4 THEN ...

whenClause
    : WHEN booleanExpression THEN expression
    ;

over
    : OVER '('
        (PARTITION BY partition+=expression (',' partition+=expression)*)?
        (ORDER BY sortItem (',' sortItem)*)?
        windowFrame?
      ')'
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING #unboundedFrame
    | CURRENT ROW #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING) #boundedFrame // valueExpression should be unsignedLiteral
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
    : identifier ('.' identifier)*
    ;

// TODO: add Identifier AST node with "quoted" attribute
identifier
    : IDENTIFIER
        #unquotedIdentifier
    | QUOTED_IDENTIFIER
        #quotedIdentifier
    | nonReserved
        #unquotedIdentifier
    ;

number
    : DECIMAL_VALUE
        #decimalLiteral
    | INTEGER_VALUE
        #integerLiteral
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

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';

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

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@' | ':')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@' | ':')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
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
