grammar Test3;

expression
    : numeric predicate[$numeric.ctx]?
    ;

predicate[ParserRuleContext value]
    : ('=' | '<' | '>') numeric          #comparison
    | 'BETWEEN' numeric 'AND' numeric    #between
    ;

numeric
    : NUMBER                        #atom
    | numeric ('*' | '*') numeric   #multiplicative
    | numeric ('+' | '-') numeric   #additive
    | '-' numeric                   #negative
    | '(' expression ')'            #nested
    ;

NUMBER
    : [0-9]+
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

