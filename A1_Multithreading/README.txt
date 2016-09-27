************* Output of the Program ****************************
Header consisting of fields was not considered as a valid record and hence not part of the result.
The K and F values are calculated for all records however, whether the flight is Active in January 2015 is 
calculated only for sane flights(i.e. after incrementing F).

K, Number of corrupt lines of input  = 12598804
(lines not in the same format as the rest and lines with flights that do not pass the sanity test))
F, Number of sane flights = 60457
Airline with the Least Expensive Fare = F9, Median Ticket Price= 91.8

Console Output:
------------------

12598804
60457
F9 131.74750439624853 91.8
HA 302.60046161019585 129.43
WN 159.35513394875093 136.0
AS 230.1068712631499 191.73
MQ 288.26421258646866 265.5
OO 307.34642960387123 278.08
EV 305.68035471576354 282.44
NK 487.50814891179834 469.36
B6 523.6481987117105 483.48
AA 530.1804209592686 493.2
US 600.1634588918142 493.74
DL 598.8062004931181 505.115
VX 652.9210114261532 591.55
UA 969.0941454441081 865.23
Duration: 122971ms


The sequential program runs in 186sec on my i3 quad processor and after implementing multithreading the program runs in 122secs which is approx 64 secs faster.


