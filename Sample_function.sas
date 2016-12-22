/*to create sum function*/

%macro sum(var=usubjid,ds=&syslast.,cond=1);
/*dropping the datasets that are going to be used*/
proc delete data= mwrk.temp_&rand.;
run;

/*getting the sum in an intermidiary dataset
making sure to apply condition*/

proc sql;
create table mwrk.sum_out&rand. as
select sum(&var) as sum_&var
from &ds.
where &cond.;
quit;

/*transposing datasets and renaming variables so that it is in the required format
ensuring the final output is in a dataset called mwrk.temp&rand.*/

proc transpose data=mwrk.sum_out&rand. out= mwrk.temp_&rand.(rename=(COL1=col)) name=measure;
run;

%mend sum;

