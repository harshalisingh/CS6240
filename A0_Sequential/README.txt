Question:
The Bureau of Transport Statistics' On-time Performance (OTP) dataset has information about flights in the USA. The full dataset covers 27 years of air travel and is over 60GB in plain text. For this assignment you should answer the question: Which airlines have the least expensive fares? 
Fine print: 
(0) Individual assignment. 
(1) Data for one  month is here. 
(2) Write a sequential Java program reading one gzipped file and writing results on the console. 
(3) The only output should be the K and F, one per line, where K is the number of corrupt lines of input (lines not in the same format as the rest and lines with flights that do not pass the sanity test),  F is  the number of sane flights. Next, output pairs "C p" where C is a carrier two letter code  and p is the mean price of tickets. Sort the list by increasing price.  
(4) The sanity test is:
CRSArrTime and CRSDepTime should not be zero
timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
timeZone % 60 should be 0
AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
Origin, Destination,  CityName, State, StateName should not be empty
For flights that not Cancelled:
ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
if ArrDelay < 0 then ArrDelayMinutes should be zero
if ArrDelayMinutes >= 15 then ArrDel15 should be true

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