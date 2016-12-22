
libname mwrk '/mwrk';

/*To declare the formats as 'missing' for both ' ' and '.'*/
proc format;
     value $missfmt ' '='missing'
           other='non-missing';
     value missfmt .='missing'
           other='non-missing';
run;
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
/*Template creation:-  run once*/
proc template;
define style mystyle;
parent=styles.minimal;
replace header /
Backgroundcolor=maroon Color=white bordercolor=black textalign=center;
end;
run;
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

ods tagsets.ExcelXP style=mwrk.mystyle
     file='/out.xls' options (sheet_interval = 'proc' absolute_column_width = '20,7,10,10,30,10,10,10,10,20,20,80' zoom= '90');

%macro ds_description(ds=);
     ods tagsets.ExcelXP options(sheet_name = "&ds." embedded_titled = 'yes' print_header="Contents of table &ds." );

     /*To supress unnecessary output to the lst*/
     ods select none;

     proc delete data =  mwrk.temp2 mwrk.temp1 mwrk.output_ds;
     run;

     /*To get the variables part out of the proc contents output*/
     proc contents data=&ds.;
           ods output Variables=mwrk.TEMP1;
     run;

     /*To get the descriptive stats for all numeric variables*/
     proc means data=&ds.;
           var _numeric_;
           output out=mwrk.temp2(drop = _FREQ_ _TYPE_ _LABEL_);
     run;

     /*To check whether the dataset has any numeric columns -*/
     /*1. Yes - Then separate out those columns and proceed with the conversions*/
     /*2. No - Then don't do anything*/
     proc sql;
           select count(VARIABLE) 
                into :numcnt
                     from mwrk.temp1
                           where TYPE ="Num";
     quit;

     /*Put the count of numeric columns into a variable to be used as a macro*/
     %put &numcnt.;

     %if &numcnt. ne 0 %then
           %do;

                proc transpose data=mwrk.TEMP2 out=mwrk.trans name=VARIABLE;
                     ID _STAT_;
                run;

                data mwrk.temp1 (drop=member pos num  );
                     set mwrk.temp1;
                run;

                proc sql;
                     create table mwrk.output_ds as
                           select a.*, b.*
                                from mwrk.temp1 as a left join mwrk.trans as b
                                     on a.variable = b.variable;
                quit;

                data mwrk.output_ds(drop=_LABEL_);
                     set mwrk.output_ds;
                run;

           %end;
     %else
           %do;

                data mwrk.output_ds;
                     set mwrk.temp1 (drop=member pos num);
                     N=.;
                     MIN =.;
                     MAX =.;
                     MEAN=.;
                     STD=.;
                run;

           %end;

     ods select all;

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Missing and non missing counts~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

     /** open dataset **/
     %let dsid=%sysfunc(open(&ds.));

     /** cnt will contain the number of variables in the dataset passed in **/
     %let cnt=%sysfunc(attrn(&dsid,nvars));

     %do i = 1 %to &cnt;

           /** create a different macro variable for each variable in dataset **/
           %let x&i=%sysfunc(varname(&dsid,&i));

           /** list the type of the current variable **/
           %let typ&i=%sysfunc(vartype(&dsid,&i));
     %end;

     /** close dataset **/
     %let rc=%sysfunc(close(&dsid));

     %do i = 1 %to &cnt;

           /* loop through each variable in PROC FREQ and create */
           /* a separate output data set */
           proc freq data=&ds. noprint;
                tables &&x&i / missing out=out&i(drop=percent rename=(&&x&i=Value));
                format &&x&i

                     %if &&typ&i = C %then
                           %do;
                                $missfmt.
                           %end;
                     %else
                           %do;
                                missfmt.
                           %end;;
           run;

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~list of values~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
           %let cat_list = "-";
           %let cat_cnt = "99";
           ods select none;

           proc sql;
                select count (distinct(&&x&i)) 
                     into : cat_cnt 
                           from &ds.;
                %if %eval(&cat_cnt. <= 5) %then
                     %do;
                           select distinct(&&x&i) 
                                into : cat_list SEPARATED by ","  
                                from &ds.
                                where not missing(&&x&i);
                     %end;
           quit;

           ods select all;
 /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
           data out&i;
                length categories $1000;
                set out&i;
                varname="&&x&i";
                categories = "&cat_list.";

                /* create a new variable that is character so that */
                /* the data sets can be combined */
                %if &&typ&i=N %then
                     %do;
                           Value1=put(Value, missfmt.);
                     %end;
                %else %if &&typ&i=C %then
                     %do;
                           Value1=put(Value, $missfmt.);
                     %end;

                drop Value;
                rename Value1=Value;
           run;

     %end;

     data combine;
           length VARNAME $15;
           set

                %do i=1 %to &cnt;
                     out&i
                %end;;
     run;

     data comb_missing comb_nonmiss;
           set combine;

           if Value = 'non-missing' then
                output comb_nonmiss;
           else output comb_missing;
     run;

     proc sql;
           create table final as 
                select coalesce(a.varname,b.varname) as variable, coalesce(a.categories,b.categories) as Values,
                     a.count as Nonmissing,b.count as Missing
                from comb_nonmiss as a full join comb_missing as b 
                     on a.varname = b.varname;
     quit;

     data final;
           set final;

           if NONMISSING = . then
                NONMISSING = 0;

           if MISSING = . then
                MISSING = 0;
     run;

     proc sql;
           create table mwrk.final_ds as
                select a.*, b.*
                     from mwrk.output_ds as a left join final as b
                           on a.variable = b.variable
           ;
     quit;

     /*Part - Classify the tables according to min max formats and append again*/
     proc delete data= mwrk.final_char mwrk.formflag mwrk.dt mwrk.dttime mwrk.rest;
     run;

     data mwrk.formflag (drop= min max);
           set mwrk.final_ds (rename=(min=min_temp max=max_temp));

           if Format = 'DATE9.' then
                formflag = 1;
           else if (Format = 'DATETIME20.' or Format = 'DATETIME19.') then
                formflag = 2;
           else formflag = 3;
     run;

     data mwrk.dt (drop=N min_temp max_temp formflag);
           set mwrk.formflag;
           where formflag = 1;
           Min = put (min_temp,date9.);
           Max = put (max_temp,date9.);
           Mean = .;
           Std = .;
     run;

     data mwrk.dttime(drop=N min_temp max_temp formflag);
           set mwrk.formflag;
           where formflag = 2;
           Min = put (min_temp,datetime20.);
           Max = put (max_temp,datetime20.);
           Mean = .;
           Std = .;
     run;

     data mwrk.rest(drop=N min_temp max_temp formflag);
           set mwrk.formflag;
           where formflag = 3;
           Min = put(min_temp, 10.2);
           Max = put(max_temp,10.2);
     run;

     proc sql;
           create table mwrk.final_char as
                select Variable, Type, Format, Len, Label, Nonmissing, Missing, Mean, STD, Min, Max, Values from mwrk.dt
                     union
                select Variable, Type, Format, Len, Label, Nonmissing, Missing, Mean, STD, Min, Max, Values from mwrk.dttime
                     union
                select Variable, Type, Format, Len, Label, Nonmissing, Missing, Mean, STD, Min, Max, Values from mwrk.rest;
     quit;

     title "output for &ds.";

     proc print data=mwrk.final_char noobs;
     run;

%mend ds_description;

%ds_description(ds = random_test_table);

ods tagsets.ExcelXP close;
;
