# nwcdsql
Record Mysql SQL

SQL – add profile ---

----
select 
*
from 
(select                                                                                     
temp.*,
date_format(CONVERT_TZ(temp.end_time,'+00:00','+08:00'),'%Y%m') working_month_1,
oc1.OWNER_AGENT_LOGIN owner,
case when SUBSTRING(oc1.comm_subject,22,4)='Call' then 'Call'
     when SUBSTRING(oc1.comm_subject,22,4)='Chat' then 'Chat'
else 'Email' end Incoming_type,
case when (temp.SEVERITY = 5 or temp.SEVERITY = 1)  then 1
else 0 end high_severity_flag,
case when temp.case_id  >= 1999999999 then 1
else 0 end cn_case_flag                                                                                    
from                                                                                       
(select                                                                                    
dc.CASE_ID,                                                                                
dc.SEVERITY,                                                                               
dc.SUPERVISOR, 
#dc.CASE_DESCRIPTION,
MERCHANT_CUSTOMER_ID,
dc.account_id,  
dc.REASON,
dc.CASE_TYPE_NAME,                                                                                                                                                                                                                                                                   
dc.OWNING_AGENT_LOGIN_ID,                                                                  
date_format(CONVERT_TZ(dc.CASE_RESOLVE_DATE_UTC,'+00:00','+08:00'),'%Y%m') working_month,
#CONVERT_TZ(oc.COMM_DATE_UTC,'+00:00','+08:00') as res_time,                               
max(CONVERT_TZ(oc.COMM_DATE_UTC,'+00:00','+08:00')) as end_time,                           
max(oc.comm_id) max_comm_id,                                                                           
count(oc.COMM_DATE_UTC) response_time,                                                                                                                                   
dc.CASE_START_DATE_UTC start_time,                                                         
dc.CREATION_DATE_UTC CREATION_DATE_UTC,                                                    
#oc.IS_AMAZON_SENDER,                                                                      
#oc.OWNER_AGENT_LOGIN,                                                                     
dc.case_status_name,                                                                       
dc.category_name,                                                                          
case when substring(dc.email_queue_name,1,17)='aws-support-tier1' then 'Dev'               
     when substring(dc.email_queue_name,1,17)='aws-support-tier2' then 'Bus'               
     when substring(dc.email_queue_name,1,17)='aws-support-tier3' then 'ES'                
     end as support_tier,
TIMESTAMPDIFF(minute,dc.CREATION_DATE_UTC,dc.CASE_START_DATE_UTC) FRSLA_min,
TIMESTAMPDIFF(day,dc.CREATION_DATE_UTC,max(oc.COMM_DATE_UTC)) TTR_day,                     
TIMESTAMPDIFF(hour,dc.CREATION_DATE_UTC,max(oc.COMM_DATE_UTC)) TTR_hour,
TIMESTAMPDIFF(minute,dc.CREATION_DATE_UTC,max(oc.COMM_DATE_UTC)) TTR_min,                   
date_format(dc.CREATION_DATE_UTC,'%Y%m') create_working_month,
case when ccw.weight is null then 1 
else ccw.weight end weight,
ccw.profile,
dc.RESPONSE_SLA_MINUTES,
case when dc.RESPONSE_SLA_MINUTES -TIMESTAMPDIFF(minute,dc.CREATION_DATE_UTC,dc.CASE_START_DATE_UTC) >= 0 then 1
     else 0 end if_meet_sla
from aws_support_cases_bjs.d_case_details_cn dc                                                                                                           
inner join aws_support_cases_bjs.o_case_communications oc                                    
on dc.case_id = oc.case_id                                                                
and oc.is_amazon_sender='Y'                                                                
left join aws_support_cases_bjs.d_employee_history_cn cl                                                               
on oc.OWNER_AGENT_LOGIN=cl.login
left join aws_support_cases_bjs.case_category_weight ccw
on dc.category_name=ccw.category_name                                                
where                                                                              
(dc.EMAIL_QUEUE_NAME like 'aws-support%'
or dc.EMAIL_QUEUE_NAME = 'acme-aws-cn@amazon.cn'
or dc.EMAIL_QUEUE_NAME = 'acme-aws-enterprise-cn@amazon.cn'
or dc.EMAIL_QUEUE_NAME = 'acme-aws-business-cn@amazon.cn'
)                                                   
and dc.MERCHANT_CUSTOMER_ID NOT IN (Select account_id from filtered_account)               
-- and dc.case_status_name='Resolved'                                                         
and CONVERT_TZ(oc.COMM_DATE_UTC,'+00:00','+08:00') >= str_to_date('20200101','%Y%m%d')      
and oc.owner_agent_login <> 'arizona'                                                      
and oc.owner_agent_login <> 'pma-auto-reminders'                                           
and oc.owner_agent_login <> ''                                                             
group by 1                                                                                 
) temp  
inner join aws_support_cases_bjs.o_case_communications oc1
on temp.max_comm_id=oc1.comm_id                                                                       
where working_month is not null                                                            
#and temp.case_id in ('1462533524','1463080344','1462617684','1462546144','1461838854')                                    
#and owner_agent_login='jingamz'                                                            
#and working_month='202101'                                                                
#group by 1,2,3
) case_info
where 
1=1
and working_month>=202101
and owner <> 'Tester';
----

-- CCR
use aws_support_cases_bjs;
select
temp.correspondence_agent_id,
avg(rating) raw_avg_ccr,
count(temp.rating) raw_ccr_count,
sum(case when temp.review_result_count=1 then temp.rating else 0 end)/sum(temp.review_result_count) After_Filter_avg_ccr,
sum(temp.review_result_count) After_Filter_ccr_count
from
(select 
ccr.*,
review.category_id,
review.reason_id,
#review.comment,
# category_id =1 说明低分是需要被计算的
case when category_id <> 1 then 0 else 1 end review_result_count,
#case when (category_id <> 1 or category_id is null) then rating else 0 end review_result_rating,
date_format(CONVERT_TZ(ccr.update_date,'+00:00','+08:00'),'%Y%m') working_month_ccr,
dc.SEVERITY,
dc.CATEGORY_NAME,
case when substring(dc.email_queue_name,1,17)='aws-support-tier1' then 'Dev'               
     when substring(dc.email_queue_name,1,17)='aws-support-tier2' then 'Bus'               
     when substring(dc.email_queue_name,1,17)='aws-support-tier3' then 'ES'                
     end as support_tier,
date_format(CONVERT_TZ(ccr.update_date,'+00:00','+08:00'),'%Y%m') working_month,
dc.account_id,
dc.REASON,
dc.CASE_TYPE_NAME
from aws_support_cases_bjs.case_correspondence_rating_cn ccr 
left join aws_support_cases_bjs.d_employee_history_cn cl                                                               
on ccr.correspondence_agent_id=cl.login
left join lse.manager_ccrdata review
on ccr.correspondence_id=review.comm_id
left join aws_support_cases_bjs.d_case_details_cn dc
on ccr.case_id=dc.case_id
where 
CONVERT_TZ(ccr.update_date,'+00:00','+08:00') >= str_to_date('20210101','%Y%m%d')
and 
(dc.EMAIL_QUEUE_NAME like 'aws-support%'
or dc.EMAIL_QUEUE_NAME = 'acme-aws-cn@amazon.cn'
or dc.EMAIL_QUEUE_NAME = 'acme-aws-enterprise-cn@amazon.cn'
or dc.EMAIL_QUEUE_NAME = 'acme-aws-business-cn@amazon.cn'
)                                                   
and dc.MERCHANT_CUSTOMER_ID NOT IN (Select account_id from filtered_account)
#and CONVERT_TZ(ccr.update_date,'+00:00','+08:00') < str_to_date('20200401','%Y%m%d')
#and correspondence_agent_id='jingamz'
#and ccr.case_id = '1447963234' 
order by ccr.update_date desc
) temp
group by 1
;
