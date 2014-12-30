grammar Test2;

top
    : expression EOF
    ;

expression
    : numeric (
        ('='|'<'|'>') numeric
        | 'BETWEEN' numeric 'AND' numeric
      )?
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

