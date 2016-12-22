/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
/*                                              Usual looping macro                                                    */
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

/*this code snippet would print out multiple tables for each cohort in question*/

%include '00_setup_file.sas';

%macro main(table1,tag,source);
proc sql;

	Title "Rolled up to index date";
	Select count(distinct person_id) as pat_roll,count(*) as row_cnt from rw.abai_rhe_&tag.1;

	/*To find the number of patients having claims for rheumatologists*/
	Title "To find rheumatologist claims - &tag";
	Select count(distinct person_id) as pat_&tag, count(*) as row_cnt from rw.abai_rhe_&tag.1
	where stdprov = 300;
quit;
%mend main;
%main(table1=abai_case,tag=ra_m,source='714%');
%main(table1=abai_unmatched_ra,tag=ra_um,source='714%');
%main(table1=abai_ra_matches,tag=oa_m,source='715%');
%main(table1=abai_unmatched_oa,tag=oa_um,source='715%');

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
/*                                                         NEW                                                         */
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
%include '00_setup_file.sas';
%include 'general_macro.sas';

%macro main(table1,tag,source);
%count(var=distinct person_id,ds=rw.abai_rhe_&tag.1);

%merge();

%mend main;
%main(table1=abai_case,tag=ra_m,source='714%');
%main(table1=abai_unmatched_ra,tag=ra_um,source='714%');
%main(table1=abai_ra_matches,tag=oa_m,source='715%');
%main(table1=abai_unmatched_oa,tag=oa_um,source='715%');

%append();

%macro main(table1,tag,source);
%count(var=distinct person_id,ds=rw.abai_rhe_&tag.1,cond=(stdprov = 300));
%merge();
%mend main;
%main(table1=abai_case,tag=ra_m,source='714%');
%main(table1=abai_unmatched_ra,tag=ra_um,source='714%');
%main(table1=abai_ra_matches,tag=oa_m,source='715%');
%main(table1=abai_unmatched_oa,tag=oa_um,source='715%');

%append();

%print();
