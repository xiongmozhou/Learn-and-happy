select 
    '2019-03-10' dt,
    uv.day_count,
    sum(if(comment_count>0,1,0)) hot,
    cast(sum(if(comment_count>0,1,0))/uv.day_count*100 as decimal(10,2)) new_m_ratio
from
    (select
        sum(comment_count) comment_count,
        user_id
    from
        dws_user_action dwa
    where
        dt='2019-03-10'
    Group by user_id
) t1,ads_uv_count uv
group by uv.day_count;


select date_format(create_date,'yyyy-MM') create_mo , count(*)  from dws_new_mid_day
where date_format(create_date,'yyyy-MM')=date_format('2019-03-10','yyyy-MM')
group by date_format(create_date,'yyyy-MM');






