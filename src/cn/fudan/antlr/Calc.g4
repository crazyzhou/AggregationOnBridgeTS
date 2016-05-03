grammar Calc;

goal
	:	(expression ';')*;
	
expression
	:	windowExpression
	|	calcExpression;

windowExpression
	:	Identifier '=' function '("' Channel '"' ',' Integer ',' Integer ')';
	
function
	:	'max'
	|	'min'
	|	'avg'
	|	'sum';

calcExpression
	:	Identifier '=' additiveExpression;

additiveExpression
	:	multiplicativeExpression
	|	additiveExpression '+' multiplicativeExpression
	|	additiveExpression '-' multiplicativeExpression
	;

multiplicativeExpression
	:	parenthesisExpression
	|	multiplicativeExpression '*' multiplicativeExpression
	|	multiplicativeExpression '/' multiplicativeExpression
	|	multiplicativeExpression '%' multiplicativeExpression
	;
	
parenthesisExpression
	:	'(' additiveExpression ')'
	|	Identifier
	|	Integer;
	
Integer
	:	NonZeroDigit DIGIT*;

Identifier
	:	OutIdentifier
	|	InIdentifier;

OutIdentifier
	:	'out_' (LetterOrDigit)*;
	
InIdentifier
	:	LETTER LetterOrDigit*;
	
Channel
	:	LetterOrDigit+;

LetterOrDigit
	:	LETTER | DIGIT;

DIGIT
	:	'0'
	|	NonZeroDigit;

NonZeroDigit
	:	[1-9];

LETTER
	:	[a-zA-Z$_-];

//
// Whitespace and comments
//

WS  :  [ \t\r\n]+ -> skip
    ;

COMMENT
    :   '/*' .*? '*/' -> skip
    ;

LINE_COMMENT
    :   '//' ~[\r\n]* -> skip
    ;