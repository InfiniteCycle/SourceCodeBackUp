with t0 as (select a.entity_id, basin, extract('year' from first_prod_date) as first_prod_year,
           rank() over (partition by a.entity_id order by prod_date) as n_mth,
           prod_date, log(liq/DATE_PART('days',DATE_TRUNC('month', prod_date) + '1 MONTH'::INTERVAL - '1 DAY'::INTERVAL) +1 ) as liq
           from di.pden_desc a join di.pden_prod b on a.entity_id = b.entity_id
           where basin in ('WILLISTON')
           and liq_cum >0 and ALLOC_PLUS IN ('Y','X') and first_prod_date >= '1980-01-01' and liq >= 0 and prod_date <= date_trunc('month', current_date) - interval '3 month'
           order by entity_id, prod_date),

           t1 as (
           select a.entity_id, a.basin, a.first_prod_year, b.n_mth, b.prod_date as prod_date, a.liq as prev_liq, b.liq,
           round(100*(b.liq - a.liq),2) as dcl
           from t0 a join t0 b on a.entity_id = b.entity_id and a.n_mth = b.n_mth - 1
           where a.liq >= 0 and b.liq >= 0 and round(100*(b.liq - a.liq),2) is not null
           --and round(100*(b.liq - a.liq)/case when a.liq = 0 then null else a.liq end,2) <= 100
           order by a.entity_id, first_prod_year, b.prod_date),

           t2 as (
           select basin, first_prod_year, n_mth, (round(avg(dcl),2) + round(stddev(dcl) ,2)) as high, (round(avg(dcl),2) - round(stddev(dcl) ,2))  as low
           from t1
           group by basin, first_prod_year, n_mth
           order by 1, 2, 3)



           insert into dev.zxw_nd_dcl_log
           select a.basin, a.first_prod_year, a.n_mth, round(avg(dcl),2) as avg_dcl
           from t1 a join t2 b on a.basin = b.basin and a.first_prod_year = b.first_prod_year and a.n_mth = b.n_mth
           where a.dcl <= b. high and a.dcl >= b.low
           group by a.basin, a.first_prod_year, a.n_mth
           order by 1, 2, 3;