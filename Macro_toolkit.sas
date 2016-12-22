%let x = %sysfunc(ranuni(0));
%let rand = %sysfunc(round(&x*1000,1));
%let column_cnt = 1;
%let row_set_cnt=1;

/*creating a seperate library for all */
libname mwrk '/mwrk';

data mwrk.BlankDs;
input measure $;
datalines;
blank_row
;
run;

/*dataset definitions*/


proc delete data= mwrk.mergedDS_&rand.;
run;
proc delete data= mwrk.appendedDS_&rand.;
run;

%macro freq(var=,ds=&syslast.,cond=1,mstr=0);

proc delete data= mwrk.temp_&rand.;
run;

proc freq data= &ds. noprint;
tables &var./ list missing out=mwrk.temp_&rand.(rename=(count=col &var.= measure) drop= percent);
where &cond.;
run;

%if &mstr. ne 0 %then %do ;

proc sql undo_policy=none;
create table mwrk.temp_&rand. as
select a.*, b.col
from &mstr. as a left join mwrk.temp_&rand. as b
on a.measure = b.measure;
quit;

%end;

%mend freq;

/*======================================================================================================*/
%macro count(var=usubjid,ds=&syslast.,cond=1,mstr=0);

proc delete data= mwrk.temp_&rand.;
run;

proc sql undo_policy=none noprint;
select count(&var) into :cnt_macro_out
from &ds.
where &cond.;
quit;

data mwrk.temp_&rand.;
measure= "count of &var.";
col = &cnt_macro_out.;
output;
run;

%mend count;
/*=======================================================================================================*/
%macro means(var=usubjid,ds=&syslast.,cond=1,mstr=0);

proc delete data= mwrk.temp_&rand.;
run;
proc delete data= mwrk.means_temp_&rand.;
run;

proc means data= &ds. noprint;
var &var.;
output out= mwrk.means_temp_&rand. (drop=_type_ _freq_) n=n mean=mean std=std median=med q1=q1 q3=q3 min=min max=max;
where &cond.;
run;

proc transpose data=mwrk.means_temp_&rand. out= mwrk.temp_&rand.(rename=(COL1=col)) name=measure;
run;

data mwrk.temp_&rand.;
set mwrk.temp_&rand.;
col= coalesce(col,.);
run;

%if &mstr. ne 0 %then %do ;

proc sql undo_policy=none;
create table mwrk.temp_&rand. as
select a.*, b.col
from &mstr. as a left join mwrk.temp_&rand. as b
on a.measure = b.measure;
quit;

%end;

proc delete data= mwrk.means_temp_&rand.;
run;

%mend means;

%macro merge();

%if %sysfunc(exist(mwrk.mergedDS_&rand.)) %then %do;

data mwrk.mergedDS_&rand.;
set mwrk.mergedDS_&rand.;
row_num=_N_;    
run;

proc sql undo_policy=none;
create table mwrk.mergedDS_&rand. as
select a.*, b.col as col_&column_cnt.
from mwrk.mergedDS_&rand. as a left join mwrk.temp_&rand. as b
on a.measure = b.measure;
quit;

proc sort data=mwrk.mergedDS_&rand.;
by row_num;
run;

data mwrk.mergedDS_&rand.(drop=row_num);
set mwrk.mergedDS_&rand.;
run;

%end;

%else %do;
data mwrk.mergedDS_&rand.;
set mwrk.temp_&rand.;
run;
%end;

%let column_cnt = %eval(&column_cnt+1);

data mwrk.temp_&rand.;
set mwrk.mergedDS_&rand.;
run;

%mend merge;

%macro append();

%let dsid=%sysfunc(open(mwrk.temp_&rand.));
%let measure_type=%sysfunc(vartype(&dsid,1));
%let rc=%sysfunc(close(&dsid));

%if &measure_type=N %then %do;

data mwrk.temp_&rand.;
set mwrk.temp_&rand.;
mes = put(measure, 10.) ; 
drop measure ; 
rename mes=measure ;
run;

data mwrk.temp_&rand.;
retain measure;
set mwrk.temp_&rand.;
run;

%end;

%if %sysfunc(exist(mwrk.appendedDS_&rand.)) %then
%do;

proc append base=mwrk.appendedDS_&rand. data=mwrk.temp_&rand. force nowarn;
run;

%end;
%else
%do;

data mwrk.appendedDS_&rand.;
set mwrk.temp_&rand.;
run;

%end;

%let row_set_cnt = %eval(&row_set_cnt+1);

data mwrk.temp_&rand.;
set mwrk.appendedDS_&rand.;
run;

%let column_cnt = 1;

proc delete data= mwrk.mergedDS_&rand.;
run;

%mend append;

%macro blank(blnkcnt);

%do i=1 %to %eval(&blnkcnt.);
data mwrk.temp_&rand.;
set mwrk.BlankDs;
run;

%append()
%end;

%mend blank;


%macro print(ds=&syslast.);

proc print data=&ds.;
run;

proc delete data=mwrk.appendedDS_&rand.;
run;
proc delete data=mwrk.mergedDS_&rand.;
run;
proc delete data=mwrk.temp_&rand.;
run;

%mend print;

%macro format(var=all,type=char,format=&&form&i);

%let dsid=%sysfunc(open(mwrk.temp_&rand.));
%let cnt=%sysfunc(attrn(&dsid,nvars));
%do i = 1 %to &cnt;
%let x&i=%sysfunc(varname(&dsid,&i));
%let form&i=%sysfunc(varfmt(&dsid,&i));
%end;
%if &var. ne all %then %do;
%let single_form_num =%sysfunc(varnum(&dsid,&var.));
%end;
%let rc=%sysfunc(close(&dsid));

%if &var. = all %then %do ;

%do i = 2 %to &cnt;
data mwrk.temp_&rand.;
set mwrk.temp_&rand.;
temp_var = put(&&x&i., &format.) ; 
drop &&x&i. ; 
rename temp_var=&&x&i. ;
run;
%end;

%end;

%else %do;

%let i = &single_form_num.

data mwrk.temp_&rand.;
set mwrk.temp_&rand.;
temp_var = put(&var, &format) ; 
drop &var. ; 
rename temp_var=&var. ;
run;
%end;

data mwrk.temp_&rand.;
set mwrk.temp_&rand.;
retain
%do i = 1 %to &cnt;
&&x&i
%end;
;
run;

%mend format;

%macro dssave(outds=);
data &outds.;
set mwrk.temp_&rand.;
run;
%mend format;

%macro text_print(text);
data _null_;
    file print;
    put "&text.";
    file log;
run;
%mend text_print;

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

QC and SCHEMA

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

proc delete data= mwrk.qc_list;
run;

%macro qc(ds=&syslast., description = patient count , var=usubjid, cond= 1, type=count);

/*initializing all macro variables*/

%let cnt1=.;
%let cnt2=.;

	%if &type.=count %then
		%do;
			proc sql;
				select count(&var.)
					into: cnt1
				from &ds. 
					where &cond.;
				select count(distinct(&var.)) 
					into: cnt2
				from &ds. 
					where &cond.;
			quit;

			proc delete data=mwrk.to_merge;
			run;

			data mwrk.to_merge;
				length description $ 69;
				length dataset extra_condition $ 32;
				description = "&description.";
				dist_cnt = &cnt2.;
				rec_cnt = &cnt1.;
				dataset = "&ds.";
				extra_condition = "&cond.";
			run;

		%END;

	%if &type.=freq %then
		%do;

			proc delete data=mwrk.out_freq;
			run;

			proc freq data= &ds. noprint;
				tables &var./ list missing 
				out=mwrk.out_freq(rename=(count=rec_cnt &var.= description) drop= percent);
				where &cond.;
			run;

					data mwrk.to_merge;
						set mwrk.out_freq;
						length description $ 69;
						length dataset extra_condition $ 32;
						description = description;
						dist_cnt = .;
						rec_cnt = rec_cnt;
						dataset = "&ds.";
						extra_condition = "&cond.";
					run;

			proc delete data= mwrk.to_merge mwrk.out_freq;
			run;

		%END;

	%if %sysfunc(exist(qc_list)) %then
		%do;

			proc append base=mwrk.qc_list data=mwrk.to_merge force;
			run;

		%end;
	%else
		%do;

			data mwrk.qc_list;
				set mwrk.to_merge;
			run;

		%end;

	%put ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
	%put &description.;
	%put dataset = &ds.							var=&var.								&type.;
	%put record level count =&cnt1. 			patient level count =&cnt2. ;
	%put ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

%mend qc;

/*deprecated function*/
%macro credits();
%include '/Credit.sas';
%mend credits;

%macro schema(schema_ds=&syslast.,schema_type=list);

%let schema_last_ds = &syslast.;
%put &schema_last_ds. &syslast.;

/*scheme macro*/
%let dsid=%sysfunc(open(&schema_ds.));
%let schema_var_cnt= %sysfunc(attrn(&dsid.,nvars));

%if &schema_type.=comma %then %do;
%let schema_varlist=; 

%do i = 1 %to &schema_var_cnt;

%let x=%sysfunc(varname(&dsid,&i));
%let schema_varlist=&schema_varlist.,&x.;

%end;

data mwrk.schema_list;
length var_list $999.;
var_list = "&schema_varlist.";
run;

%end;

%if &schema_type.=list %then %do;

data mwrk.schema_list;
length var_name $10.;

%do i = 1 %to &schema_var_cnt;

var_name="%sysfunc(varname(&dsid,&i))";
var_type="%sysfunc(vartype(&dsid,&i))";
output;

%end;
run;

%end;

proc print data= mwrk.schema_list;
run;

%put &schema_last_ds. &syslast.;

%let syslast = &schema_last_ds.;
%put &schema_last_ds. &syslast.;

%mend schema;

