%let pgm=utl-create-a-special-synthetic-index-to-speed-up-sql-joins;

For complex schemas synthetic indices can creatly simpliy and speed up joins.

The  code below creates special synthetic index where we join two tables on x1, x2 and x3
where the order of x1, x2, x3 does not matter.

Without the states index/variable this would be a very complicated join?

github
https://tinyurl.com/yhe55ffk
https://github.com/rogerjdeangelis/utl-create-a-special-synthetic-index-to-speed-up-sql-joins

Synthetic indices are an important part of complex schemas, Teradata makes extensive use
of these indices. For long queries Teradata may sample the data optimize index usage.

Note R sqldf only provides temp tables and queries.
To create a index or permanent table you need to create a permanent sqllite data base first and
use packages DBI and Rsqllite.

Sqldf does not support an outer join, however you can achieve an outer join
using first, a left join and then a right join to get the missing keys in the left table.

    SOLUTIONS

         1  sas input
         2  sas sql
         3  r input  (same as sas but created in r)
         4  r sql

/*               _     _
 _ __  _ __ ___ | |__ | | ___ _ __ ___
| `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
| |_) | | | (_) | |_) | |  __/ | | | | |
| .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
|_|
*/

/**************************************************************************************************************************/
/*                                                                                                                        */
/* THE DATA BELOW IS NOT THE SAME AS THE DATA IN THE SOLUTIONS BELOW. THIS DATA IS FOR DOCUMENTATION ONLY                 */
/*                                                                                                                        */
/*------------------------------------------------------------------------------------------------------------------------*/
/*                                        |                                    |                                          */
/*                                        |                                    |                                          */
/*               INPUTS                   |         PROCESS                    |              OUTPUT                      */
/*                                        |                                    |                                          */
/*   SD1.HAVDAT1        SD1.HAVDAT2       |                                    |          HAVDAT1  HAVDAT2                */
/*  ==============    ================    | DO THIS PROCESS FOR EACH TABLE     |                                          */
/*  X1    X2    X3     X1    X2    X3     |                                    |  STATES    CNT1      CNT2                */
/*                                        | 1 SORT ACROSS ROWS AND ADD STATES  |                                          */
/*   2     1     2      2     1     2     |                                    |   000        .         2                 */
/*   1     0     1      1     1     1     |   data havGrp1;                    |   001        1         1                 */
/*   2     2     0      0     1     0     |     set sd1.have;                  |   002        2         2                 */
/*   1     0     2      2     1     2     |     call sortn(of x:);             |   011        5         .                 */
/*   1     0     2      0     0     0     |     states=cats(of x:);            |   012        4         7                 */
/*   2     0     0      0     0     0     |   run;quit;                        |   022        4         2                 */
/*   0     1     0      2     1     2     |                                    |   111        1         1                 */
/*   0     1     2      0     1     0     |   ROW VALUES ARE IN SORT ORDE      |   112        2         1                 */
/*   1     0     1      2     0     1     |                                    |   122        1         4                 */
/*   2     1     1      2     2     1     |   X1    X2    X3  STATES           |                                          */
/*   0     0     1      2     0     1     |                                    |                                          */
/*   0     1     0      0     0     1     |    1     2     2    122  1         |                                          */
/*   0     0     1      2     0     2     |    0     1     1    011            |                                          */
/*   0     2     1      2     0     1     |    0     2     2    022            |                                          */
/*   2     2     0      1     2     0     |    0     1     2    012            |                                          */
/*   2     1     2      1     2     1     |    0     1     2    012            |                                          */
/*   2     0     2      2     1     2     |    0     0     2    002            |                                          */
/*   2     2     1      0     0     0     |    0     0     1    001            |                                          */
/*   0     2     1      0     2     1     |    0     1     2    012            |                                          */
/*   1     2     2      2     2     0     |    0     1     1    011            |                                          */
/*                                        |    1     1     2    112            |                                          */
/*                                        |    0     0     1    001            |                                          */
/*                                        |    0     0     1    001            |                                          */
/*                                        |    0     0     1    001            |                                          */
/*                                        |    0     1     2    012            |                                          */
/*                                        |    0     2     2    022            |                                          */
/*                                        |    1     2     2    122  2         |                                          */
/*                                        |    0     2     2    022            |                                          */
/*                                        |    1     2     2    122  3         |                                          */
/*                                        |    0     1     2    012            |                                          */
/*                                        |                          STATE 122 |                                          */
/*                                        |    1     2     2    122  HAS CNT=4 |                                          */
/*                                        |                                    |                                          */
/*                                        | 2. COMPUTE FRQUENCIES              |                                          */
/*                                        |                                    |                                          */
/*                                        |     X1 X2 X3 STATES CNT  GRP       |                                          */
/*                                        |                                    |                                          */
/*                                        |      0  0  1  001    4    1        |                                          */
/*                                        |      0  0  2  002    1    2        |                                          */
/*                                        |      0  1  1  011    2    3        |                                          */
/*                                        |      0  1  2  012    5    4        |                                          */
/*                                        |      0  2  2  022    3    5        |                                          */
/*                                        |      1  1  2  112    1    6        |                                          */
/*                                        |      1  2  2  122    4    7        |                                          */
/*                                        |                                    |                                          */
/*                                        |     JOIN HAVGRP1 AND HAVGRP2       |                                          */
/*                                        |     ON STATES                      |                                          */
/*                                        |                                    |                                          */
/*                                        |  3  CREATE INDEIcES ON STATES      |                                          */
/*                                        |  4  DO THE JOIN                    |                                          */
/*                                        |                                    |                                          */
/**************************************************************************************************************************/
/*                   _                   _
/ |  ___  __ _ ___  (_)_ __  _ __  _   _| |_
| | / __|/ _` / __| | | `_ \| `_ \| | | | __|
| | \__ \ (_| \__ \ | | | | | |_) | |_| | |_
|_| |___/\__,_|___/ |_|_| |_| .__/ \__,_|\__|
                            |_|
*/

%macro utl_rep(rep);

data sd1.havdat&rep;

  array xs[3] x1-x3;
  do r=1 to 20;
    do c=1 to 3;
      xs[c] = int(3*(uniform(&rep)));
    end;
    output;
  end;
  drop r c;
run;quit;

data sd1.havGrp&rep;
  set sd1.havDat&rep;
  call sortn(of x:);
  states=cats(of x:);
  keep states;
run;quit;

%mend utl_rep;

%utl_rep(1);
%utl_rep(2);

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  CREATE THES FOUR INPUT TABLES                                                                                         */
/*                                                                                                                        */
/*  SD1.HAVDAT1       SD1.HAVDAT2         SD1.HAVGRP2  SD1.HAVGRP2                                                        */
/*  --------------    --------------      -----------  -----------                                                        */
/*                                                                                                                        */
/*  X1    X2    X3    X1    X2    X3        STATES        STATES                                                          */
/*                                                                                                                        */
/*   0     2     1     1     2     2         122           122                                                            */
/*   0     2     2     1     2     2         122           122                                                            */
/*   1     1     0     0     0     0         000           000                                                            */
/*   0     2     1     0     1     0         001           001                                                            */
/*   2     0     2     2     0     2         022           022                                                            */
/*   0     0     2     1     1     1         111           111                                                            */
/*   2     0     2     2     1     1         112           112                                                            */
/*   1     1     0     2     0     1         012           012                                                            */
/*   1     2     1     2     2     0         022           022                                                            */
/*   1     1     1     0     0     2         002           002                                                            */
/*   2     1     2     1     0     2         012           012                                                            */
/*   2     1     0     2     0     1         012           012                                                            */
/*   1     1     2     2     2     1         122           122                                                            */
/*   0     0     2     1     0     2         012           012                                                            */
/*   0     2     2     1     2     2         122           122                                                            */
/*   1     0     0     0     0     0         000           000                                                            */
/*   1     1     0     0     2     1         012           012                                                            */
/*   1     1     0     0     2     0         002           002                                                            */
/*   1     0     1     2     1     0         012           012                                                            */
/*   2     1     0     1     2     0         012           012                                                            */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                              _
|___ \   ___  __ _ ___   ___  __ _| |
  __) | / __|/ _` / __| / __|/ _` | |
 / __/  \__ \ (_| \__ \ \__ \ (_| | |
|_____| |___/\__,_|___/ |___/\__, |_|
                                |_|
*/

proc sql;
  create index states on havGrp1;
  create index states on havGrp2;
  create
     table want as
  select
     coalesce(l.states,r.states) as states
    ,l.cnt as cnt1
    ,r.cnt as cnt2
  from
    (
     select
        states
       ,count(*) as cnt
     from
        sd1.havGrp1
     group
        by states
     ) as l full outer join
    (
     select
        states
       ,count(*) as cnt
     from
        sd1.havGrp2
     group
        by states
     ) as r
  on
     l.states = r.states
;quit;


/**************************************************************************************************************************/
/*                                                                                                                        */
/*    STATES    CNT1    CNT2                                                                                              */
/*                                                                                                                        */
/*     000        .       2                                                                                               */
/*     001        1       1                                                                                               */
/*     002        2       2                                                                                               */
/*     011        5       .                                                                                               */
/*     012        4       7                                                                                               */
/*     022        4       2                                                                                               */
/*     111        1       1                                                                                               */
/*     112        2       1                                                                                               */
/*     122        1       4                                                                                               */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____          _                   _
|___ /   _ __  (_)_ __  _ __  _   _| |_
  |_ \  | `__| | | `_ \| `_ \| | | | __|
 ___) | | |    | | | | | |_) | |_| | |_
|____/  |_|    |_|_| |_| .__/ \__,_|\__|
                       |_|
*/

%utl_rbeginx;
parmcards4;
library(haven)
library(sqldf)
source("c:/oto/fn_tosas9x.R")
havdat1<-read_sas("d:/sd1/havdat1.sas7bdat")
havdat2<-read_sas("d:/sd1/havdat2.sas7bdat")
states1<-data.frame(states=" ")
for (r in seq(1,nrow(havdat1),1)) {
    rowsrt<-sort(as.numeric(havdat1[r,]))
    states1[r,]<-paste0(rowsrt[1],rowsrt[2], rowsrt[3])
    }
states1;
states2<-data.frame(states=" ")
for (r in seq(1,nrow(havdat2),1)) {
    rowsrt<-sort(as.numeric(havdat2[r,]))
    states2[r,]<-paste0(rowsrt[1],rowsrt[2], rowsrt[3])
    }
states2;
save(states1, file = "d:/rds/states1.rds");
save(states2, file = "d:/rds/states2.rds");
;;;;
%utl_rendx;


/**************************************************************************************************************************/
/*                                                                                                                        */
/*  CREATE THES FOUR INPUT TABLES                                                                                         */
/*                                                                                                                        */
/*  R DATAFRAMES FROM SAS DATASETS                                                                                        */
/*                                                                                                                        */
/*  HAVDAT1            HAVDAT2            d:/rds/states1.rds     d:/rds/states2.rds                                       */
/*  --------------    --------------      ------------------     ------------------                                       */
/*                                                                                                                        */
/*  X1    X2    X3    X1    X2    X3        STATES                  STATES                                                */
/*                                                                                                                        */
/*   0     2     1     1     2     2         122                     122                                                  */
/*   0     2     2     1     2     2         122                     122                                                  */
/*   1     1     0     0     0     0         000                     000                                                  */
/*   0     2     1     0     1     0         001                     001                                                  */
/*   2     0     2     2     0     2         022                     022                                                  */
/*   0     0     2     1     1     1         111                     111                                                  */
/*   2     0     2     2     1     1         112                     112                                                  */
/*   1     1     0     2     0     1         012                     012                                                  */
/*   1     2     1     2     2     0         022                     022                                                  */
/*   1     1     1     0     0     2         002                     002                                                  */
/*   2     1     2     1     0     2         012                     012                                                  */
/*   2     1     0     2     0     1         012                     012                                                  */
/*   1     1     2     2     2     1         122                     122                                                  */
/*   0     0     2     1     0     2         012                     012                                                  */
/*   0     2     2     1     2     2         122                     122                                                  */
/*   1     0     0     0     0     0         000                     000                                                  */
/*   1     1     0     0     2     1         012                     012                                                  */
/*   1     1     0     0     2     0         002                     002                                                  */
/*   1     0     1     2     1     0         012                     012                                                  */
/*   2     1     0     1     2     0         012                     012                                                  */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*  _                      _
| || |    _ __   ___  __ _| |
| || |_  | `__| / __|/ _` | |
|__   _| | |    \__ \ (_| | |
   |_|   |_|    |___/\__, |_|
                        |_|
*/

options ps=64;

%utl_rbeginx;
parmcards4;
library(sqldf)
source("c:/oto/fn_tosas9x.R")
load("d:/rds/states1.rds")
load("d:/rds/states2.rds")
want <- sqldf('
   select
      l.states
     ,l.cnt1
     ,r.cnt2
   from
      (
      select
           states
          ,count(states) as cnt1
      from
           states1
      group
           by states
      ) as l left join
      (
      select
           states
          ,count(states) as cnt2
      from
           states2
      group
           by states
      ) as r
   on
      l.states = r.states
   union all
   select
      r.states
     ,l.cnt1
     ,r.cnt2
   from
      (
      select
           states
          ,count(states) as cnt1
      from
           states1
      group
           by states
      ) as l right join
      (
      select
           states
          ,count(states) as cnt2
      from
           states2
      group
           by states
      ) as r
   on
      l.states = r.states
   where
      l.states is null
   order
      by l.states
   ')
want
fn_tosas9x(
      inp    = want
     ,outlib ="d:/sd1/"
     ,outdsn ="rwant"
     )
;;;;
%utl_rendx;

proc print data=sd1.rwant;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* R                                                                                                                      */
/*                                                                                                                        */
/*  > want                                                                                                                */
/*    states cnt1 cnt2                                                                                                    */
/*  1    000   NA    2                                                                                                    */
/*  2    001    1    1                                                                                                    */
/*  3    002    2    2                                                                                                    */
/*  4    011    5   NA                                                                                                    */
/*  5    012    4    7                                                                                                    */
/*  6    022    4    2                                                                                                    */
/*  7    111    1    1                                                                                                    */
/*  8    112    2    1                                                                                                    */
/*  9    122    1    4                                                                                                    */
/*                                                                                                                        */
/*                                                                                                                        */
/* SAS                                                                                                                    */
/*                                                                                                                        */
/* ROWNAMES    STATES    CNT1    CNT2                                                                                     */
/*                                                                                                                        */
/*     1        000        .       2                                                                                      */
/*     2        001        1       1                                                                                      */
/*     3        002        2       2                                                                                      */
/*     4        011        5       .                                                                                      */
/*     5        012        4       7                                                                                      */
/*     6        022        4       2                                                                                      */
/*     7        111        1       1                                                                                      */
/*     8        112        2       1                                                                                      */
/*     9        122        1       4                                                                                      */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
