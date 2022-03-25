with fresh_sku as (
    select distinct externalid as skuseq
    from ods.skus
    where deliverytype = 'OVERNIGHT'
)

select --d.yyyy || d.mm               as yyyymm,
       d.yyyy,
       count(distinct s.member_srl) as member_cnt,
       count(distinct s.orderid)    as order_cnt,
       sum(s.sale_skuprice)         as gmv,
       sum(s.sku_quantity)          as unitsold
from bimart.dsf_skusale_daily_x_pl_margin s
         join fresh_sku f on f.skuseq = s.skuseq
         join bimart.dim_date d on d.dt = s.sale_basis_dy
where d.dt between 20190101 and 20220315
--where d.dt between 20200101 and 20211231
  and s.kind = 1
  --and s.sale_skuprice > 0
group by 1