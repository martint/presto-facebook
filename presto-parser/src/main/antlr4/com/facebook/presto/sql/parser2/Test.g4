grammar Test;

expression
    : numeric                                  #primary
    | numeric ('='|'<'|'>') numeric            #comparison
    | numeric 'BETWEEN' numeric 'AND' numeric  #between
    ;

numeric
    : NUMBER                      #atom
    | numeric ('*'|'*') numeric   #multiplicative
    | numeric ('+'|'-') numeric   #additive
    | '-' numeric                 #negative
    | '(' expression ')'          #nested
    ;

NUMBER
    : [0-9]+
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

