/*
 FRESH CUSTOMERS MONTHLY DASHBOARD :: CUSTOMER, AOV, ORDER FREQUENCY
 */

-- DATE RANGE :: Fresh started on 20180830
--            :: Track them from 20181001 (2018 OCT W40)
with date_range as (
    select d.yyyy,
           d.yyyy || d.mm as yyyymm,
           dense_rank() over (order by yyyymm) as month_no,
           case when d.mm::int = 1 then 'Jan'
                when d.mm::int = 2 then 'Feb'
                when d.mm::int = 3 then 'Mar'
                when d.mm::int = 4 then 'Apr'
                when d.mm::int = 5 then 'May'
                when d.mm::int = 6 then 'Jun'
                when d.mm::int = 7 then 'Jul'
                when d.mm::int = 8 then 'Aug'
                when d.mm::int = 9 then 'Sep'
                when d.mm::int = 10 then 'Oct'
                when d.mm::int = 11 then 'Nov'
                when d.mm::int = 12 then 'Dec'
           end as month_nm,
           d.iyyyy || d.iyyyy_wk as yyyywk, -- mon~sun
           'W' || d.iyyyy_wk as week_nm,
           dense_rank() over (order by yyyywk) as week_no,
           d.dt,
           case when d.d::int = 2 then 'MON'
                when d.d::int = 3 then 'TUE'
                when d.d::int = 4 then 'WED'
                when d.d::int = 5 then 'THU'
                when d.d::int = 6 then 'FRI'
                when d.d::int = 7 then 'SAT'
                when d.d::int = 1 then 'SUN'
           end as day_nm
    from bimart.dim_date d
    where yyyywk >= 201840
    and date(d.dates) <= current_date - 1
)

/*
 Rocket Fresh Service Items (B2C Only)
 -- PL + Retail
 -- Include FLC SKUs
 -- Include Rocket Market (Lifesaver) VIs
 -- Exclude 'Eats Mart' SKUs, B2B VIs
 */

,rocket_fresh as (
    -- Fresh Overnight VI (mainly using this part for customer analysis)
    select distinct VENDORITEMID
    from (
             SELECT DISTINCT B.VENDORITEMID,
                             CASE WHEN C.vendoritemid IS NULL THEN 'N' ELSE 'Y' END  AS B2B,
                             CASE
                                 WHEN A.po_status_code IN ('WAREHOUSE_WHOLESALE') THEN 'Y'
                                 ELSE 'N' END                                        AS WAREHOUSE_WHOLESALE,
                             CASE WHEN A.SKUNAME LIKE '%이츠마트%' THEN 'Y' ELSE 'N' END AS EATS_MART
             FROM ODS.SKUS AS SKU
                      JOIN BIMART.dwd_sku_x_pl_margin A
                           ON a.skuseq = SKU.EXTERNALID
                      JOIN ODS.VENDORITEM_EXTERNALSKUS B
                           ON SKU.EXTERNALID = b.EXTERNALSKUID
                      left join (select distinct vendoritemid
                                 from bimart.ddd_product_vendor_item
                                 where targeted_business = 'B2B'
                                   and vendor_id in ('A00010028')) c
                                on b.vendoritemid = c.vendoritemid
             where sku.DELIVERYTYPE = 'OVERNIGHT'
               and B2B = 'N' -- exclude B2B VIs
               and EATS_MART = 'N' -- exclude eats mart VIs
               --and WAREHOUSE_WHOLESALE = 'N' -- 마감세일 SKUs 는 제외하지 않음
         )
        as A

    UNION ALL

    -- rocket market VI (로켓프레시 배지가 달려있지만, 프레시 외 카테고리 상품들)
    SELECT DISTINCT CV.VENDORITEMID
    FROM ODS.COUMARKET_VENDORITEMS AS CV
    where cv.isenabled = 1

)

-- total monthly customers
,raw_data as (
    select d.yyyy,
           d.yyyymm,
           d.month_no,
           d.month_nm,
           s.member_srl,
           count (distinct s.orderid) as order_cnt,
           sum (s.sale_amt) as unitsold,
           sum (s.sale_price) as gmv
    from bimart.dsf_sale_daily_x_pl_margin s
        join rocket_fresh rf on rf.vendoritemid = s.vendoritemid
        join date_range d on d.dt = s.sale_basis_dy
    group by 1, 2, 3, 4, 5
)

-- monthly seg
,base_data as (
    select rd.yyyymm
            , rd.member_srl
            , max (case when rd2.member_srl is not null
                                and rd2.month_no = rd.month_no - 1 then 1 else 0 end) as retained
            , max (case when rd2.member_srl is not null
                                and rd2.month_no < rd.month_no - 1 then 1 else 0 end) as reactivated
    from raw_data rd
    left join raw_data rd2 on rd2.month_no < rd.month_no
                                    and rd2.member_srl = rd.member_srl
    group by 1, 2
)

,seg_data as (
    select b.yyyymm, b.member_srl,
            case when b.retained = 1 then 1
                 when b.reactivated = 1 then 2
                 else 3
            end as seg
    from base_data b
)

select yyyy, yyyymm, month_nm, '0. Total' as gubun,
       count(distinct member_srl) as member_cnt,
       sum(order_cnt) as order_cnt,
       sum(unitsold) as unitsold,
       sum(gmv) as gmv
from raw_data
group by 1, 2, 3, 4

union all

select a.yyyy, a.yyyymm, a.month_nm,
       case when b.seg = 3 then '1-1. New'
            when b.seg = 1 then '1-2. Retained'
            when b.seg = 2 then '1-3. Reactivated'
       end as gubun,
       count(distinct a.member_srl) as member_cnt,
       sum(a.order_cnt) as order_cnt,
       sum(a.unitsold) as unitsold,
       sum(a.gmv) as gmv
from raw_data a
join seg_data b on b.yyyymm = a.yyyymm and b.member_srl = a.member_srl
group by 1, 2, 3, 4