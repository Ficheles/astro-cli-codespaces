
  
    

create or replace transient table LAB_PIPELINE.core.dim_tempo
    
    
    
    as (

-- Dimensão de tempo construída a partir da coluna din_instante unida de todos os meses
with distinct_instantes as (
	select distinct din_instante
	from LAB_PIPELINE.staging.stg_usina_disp
	where din_instante is not null
)

select
	md5(cast(coalesce(cast(din_instante as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as id_dim_tempo,
	din_instante as instante,
	extract(year from instante)::int as ano,
	extract(month from instante)::int as mes,
	extract(day from instante)::int as dia	
from distinct_instantes
order by din_instante
    )
;


  