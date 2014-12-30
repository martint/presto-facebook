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
    : query #queryStatement
    | USE schema=identifier #use
    | USE catalog=identifier '.' schema=identifier #use
    | CREATE TABLE qualifiedName AS query #createTableAsSelect
    | DROP TABLE qualifiedName #dropTable
    | INSERT INTO qualifiedName query #insertInto
    | ALTER TABLE from=qualifiedName RENAME TO to=qualifiedName #renameTable
    | CREATE (OR REPLACE)? VIEW qualifiedName AS query #createView
    | DROP VIEW qualifiedName #dropView
    | EXPLAIN ('(' explainOption (',' explainOption)* ')')? statement #explain
    | SHOW TABLES ((FROM | IN) qualifiedName)? (LIKE pattern=STRING)? #showTables
    | SHOW SCHEMAS ((FROM | IN) identifier )? #showSchemas
    | SHOW CATALOGS #showCatalogs
    | SHOW COLUMNS (FROM | IN) qualifiedName #showColumns
    | DESCRIBE qualifiedName #showColumns
    | DESC qualifiedName #showColumns
    | SHOW PARTITIONS (FROM | IN) qualifiedName (WHERE booleanExpression)? (ORDER BY sortItem (',' sortItem)*)? (LIMIT limit=INTEGER_VALUE)? #showPartitions
    | SHOW FUNCTIONS #showFunctions
    | SHOW SESSION #showSession
    | SET SESSION qualifiedName EQ STRING #setSession
    | RESET SESSION qualifiedName #resetSession
    ;

query
    :  with? queryNoWith
    ;

with
    : WITH RECURSIVE? namedQuery (',' namedQuery)*
    ;

queryNoWith:
      queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=INTEGER_VALUE)?
      (APPROXIMATE AT confidence=number CONFIDENCE)?
    ;

queryTerm
    : queryPrimary #queryTermPrimary
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm #setOperation
    | left=queryTerm operator=(UNION | EXCEPT) setQuantifier? right=queryTerm #setOperation
    ;

queryPrimary
    : querySpecification #queryPrimarySpecification
    | TABLE qualifiedName #table
//    | VALUES primaryExpression (',' primaryExpression)* #inlineTable // TODO
    | VALUES rowValue (',' rowValue)* #inlineTable
    | '(' queryNoWith  ')' #subquery
    ;

rowValue
    : '(' expression (',' expression )* ')'
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy+=expression (',' groupBy+=expression)*)?
      (HAVING having=booleanExpression)?
    ;

namedQuery
    : name=identifier (columnAliases)? AS '(' query ')'
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)? #selectSingle
    | qualifiedName '.' ASTERISK #selectAll
    | ASTERISK #selectAll
    ;

relation
    : left=relation
      ( CROSS JOIN right=relation
      | joinType JOIN right=relation joinCriteria
      | NATURAL joinType JOIN right=relation
      ) #joinRelation
    | sampledRelation #sampledRelationPrimary
    ;

sampledRelation
    : aliasedRelation (
        TABLESAMPLE sampleType '(' percentage=expression ')'
        RESCALED?
        (STRATIFY ON '(' stratify+=expression (',' stratify+=expression)* ')')?
      )?
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

relationPrimary
    : qualifiedName #tableName
    | '(' query ')' #subqueryRelation
    | UNNEST '(' expression (',' expression)* ')' #unnest
    | '(' relation ')' #parenthesizedRelation
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    | POISSONIZED
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
    :
     //valueExpression
     booleanExpression
    ;

// TODO: precedence
booleanExpression
    : NOT booleanExpression #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression #logicalBinary
    | predicated #booleanPrimary
    | EXISTS '(' query ')' #exists
    ;

// workaround for https://github.com/antlr/antlr4/issues/780
predicated
    : valueExpression predicate[$valueExpression.ctx]?
    ;

predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression #comparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression #between // TODO: SYMMETRIC/ASYMETRIC
        // TODO: valueExpression NOT? IN valueExpression ?
    | NOT? IN '(' expression (',' expression)* ')' #inList
    | NOT? IN '(' query ')' #inSubquery
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)? #like
    | IS NOT? NULL #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression #distinctFrom
    ;

//    | booleanExpression IS NOT? (TRUE | FALSE | UNKNOWN) #truth // TODO: add later
//    | valueExpression comparisonOperator (ALL | SOME | ANY) '(' query ')' #quantifiedComparison // TODO: add later
//    | UNIQUE '(' query ')' #unique // TODO: add later
//    | valueExpression MATCH UNIQUE? (SIMPLE | PARTIAL | FULL) '(' query ')' #match// TODO: add later
//    | valueExpression OVERLAPS valueExpression #overlaps // TODO: add later
//    | valueExpression NOT? MEMBER OF? valueExpression #memberOf // TODO add later
//    | valueExpression NOT? SUBMULTISET OF? valueExpression #submultiset// TODO add later
//    | valueExpression IS NOT? A SET #setPredicate// TODO add later

valueExpression
    : primaryExpression #valuePrimary
    | operator=(MINUS | PLUS) valueExpression #arithmeticUnary
    | valueExpression AT timeZoneSpecifier #atTimeZone
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression #concatenation

//    | valueExpression intervalField (TO intervalField)? #intervalConversion
//    | valueExpression COLLATE (identifier '.')? identifier #collated
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
    | '(' query ')' #subqueryExpression
    | CASE valueExpression whenClause+ (ELSE elseExpression=expression)? END #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END #searchedCase
    | CAST '(' expression AS type ')' #cast
    | TRY_CAST '(' expression AS type ')' #cast
    | ARRAY '[' (expression (',' expression)*)? ']' #arrayConstructor
    | value=primaryExpression '[' index=valueExpression ']' #subscript // TODO: valueExpression '[' ... ']' ?
    | name=CURRENT_DATE #specialDateTimeFunction
    | name=CURRENT_TIME ('(' precision=INTEGER_VALUE ')')? #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP ('(' precision=INTEGER_VALUE ')')? #specialDateTimeFunction
    | name=LOCALTIME ('(' precision=INTEGER_VALUE ')')? #specialDateTimeFunction
    | name=LOCALTIMESTAMP ('(' precision=INTEGER_VALUE ')')? #specialDateTimeFunction
    | SUBSTRING '(' valueExpression FROM valueExpression (FOR valueExpression)? ')' #substring
    | EXTRACT '(' identifier FROM valueExpression ')' #extract
    | '(' expression ')' #subExpression
    //    | '(' expression ',' expression (',' expression)* ')'  #rowConstructor
    //    | ROW '(' expression (',' expression)* ')' #rowConstuctor
    //    | primaryExpression '.' identifier #fieldReference TODO: later
    //    | ELEMENT '(' valueExpression ')'  #element // TODO: add later
    ;

timeZoneSpecifier
    : TIME ZONE interval #timeZoneInterval
    | TIME ZONE STRING   #timeZoneString
//    | LOCAL
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE
    | FALSE
    ;

interval
    : INTERVAL sign=(PLUS | MINUS)? STRING from=intervalField ( TO to=intervalField )?
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

type
    : TIME_WITH_TIME_ZONE
    | TIMESTAMP_WITH_TIME_ZONE
    | identifier
    ;


// TODO: special form of simple CASE:
// CASE x
//   WHEN > 3 THEN ...
//   WHEN DISTINCT FROM 4 THEN ...
//
// also, "WHEN" condition for simple case has a different shape (e.g., it supports a list of "expression" vs a booleanExpression)

whenClause
    : WHEN condition=expression THEN result=expression
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


explainOption
    : FORMAT value=(TEXT | GRAPHVIZ | JSON) #explainFormat
    | TYPE value=(LOGICAL | DISTRIBUTED) #explainType
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

// TODO: add Identifier AST node with "quoted" attribute
identifier
    : IDENTIFIER #unquotedIdentifier
    | quotedIdentifier #quotedIdentifierAlternative
    | BACKQUOTED_IDENTIFIER #backQuotedIdentifier
    | DIGIT_IDENTIFIER #digitIdentifier
    | nonReserved #unquotedIdentifier
    ;

quotedIdentifier
    : QUOTED_IDENTIFIER
    ;

number
    : DECIMAL_VALUE
        #decimalLiteral
    | INTEGER_VALUE
        #integerLiteral
    ;

nonReserved
    : SHOW | TABLES | COLUMNS | PARTITIONS | FUNCTIONS | SCHEMAS | CATALOGS | SESSION
    | OVER | PARTITION | RANGE | ROWS | PRECEDING | FOLLOWING | CURRENT | ROW
    | DATE | TIME | TIMESTAMP | INTERVAL
    | YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    | EXPLAIN | FORMAT | TYPE | TEXT | GRAPHVIZ | LOGICAL | DISTRIBUTED
    | TABLESAMPLE | SYSTEM | BERNOULLI | POISSONIZED | USE | JSON | TO
    | RESCALED | APPROXIMATE | AT | CONFIDENCE
    | SET | RESET
    | VIEW | REPLACE
//    | A
    | IF | NULLIF | COALESCE
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
SET: 'SET';
RESET: 'RESET';
SESSION: 'SESSION';
// UNIQUE: 'UNIQUE';
// MATCH: 'MATCH';
// SIMPLE: 'SIMPLE';
// PARTIAL: 'PARTIAL';
// OVERLAPS: 'OVERLAPS';
// MEMBER: 'MEMBER';
// OF: 'OF';
// SUBMULTISET: 'SUBMULTISET';
// A: 'A';
// SET: 'SET';
// UNKNOWN: 'UNKNOWN';
// COLLATE: 'COLLATE';
// LOCAL: 'LOCAL';
// ELEMENT: 'ELEMENT';

IF: 'IF';
NULLIF: 'NULLIF';
COALESCE: 'COALESCE';

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

TIME_WITH_TIME_ZONE
    : 'TIME' WS 'WITH' WS 'TIME' WS 'ZONE'
    ;

TIMESTAMP_WITH_TIME_ZONE
    : 'TIMESTAMP' WS 'WITH' WS 'TIME' WS 'ZONE'
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
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;
