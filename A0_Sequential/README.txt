************* Instructions to Compile and Run the Program *****************

1) Compilation

On Windows:
javac -classpath "lib/*" SequentialAnalysis.java

On Linux:
javac -classpath lib/* SequentialAnalysis.java

2) Run

On Windows:
java -classpath "lib/*;." SequentialAnalysis

On Linux:
java -classpath ".:lib/*" SequentialAnalysis


************* Output of the Program ****************************
Header consisting of fields was not considered as a valid record and hence not part of the result.

K, Number of corrupt lines of input  = 4082
(lines not in the same format as the rest and lines with flights that do not pass the sanity test))
F, Number of sane flights = 435940
Airline with the Least Expensive Fare = WN, Average Ticket = 59.501

Console Output:
------------------

4082
435940
WN 59.50146206771682
HP 67.05983501136733
AS 75.47083205574913
PI 190.54003840531232
CO 204.74606779090323
EA 273.32861983471076
PA 284.24774102523907
US 289.2110895697759
TW 298.9654932243493
AA 307.3344732377233
DL 348.1242176764765
UA 547.0840192682176
NW 54354.39127438232
