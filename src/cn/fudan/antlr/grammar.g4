grammar SimpleCalc;  
  
options {  
    language=CSharp;  
    output=AST;  
    ASTLabelType=CommonTree;  
}  
  
tokens {  
    PLUS     = '+' ;  
    MINUS    = '-' ;  
    MULT     = '*' ;  
    DIV      = '/' ;  
}  
  
@members {  
}  
  
/*------------------------------------------------------------------ 
 * PARSER RULES 
 *------------------------------------------------------------------*/  
  
expr    : term ( ( PLUS^ | MINUS^ )  term )* ;  
term    : factor ( ( MULT^ | DIV^ ) factor )* ;  
factor  : NUMBER | '(' expr ')' -> expr ;  
  
/*------------------------------------------------------------------ 
 * LEXER RULES 
 *------------------------------------------------------------------*/  
  
NUMBER     : (DIGIT)+ ;  
WHITESPACE : ( '\t' | ' ' | '\r' | '\n'| '\u000C' )+     { $channel = HIDDEN; } ;  
fragment DIGIT    : '0'..'9' ; 