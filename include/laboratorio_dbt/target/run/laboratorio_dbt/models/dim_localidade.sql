
  
    

create or replace transient table LAB_PIPELINE.core.dim_localidade
    
    
    
    as (

-- Dimensão de localidade: união de meses e atributos únicos
with distinct_localidades as (
	select distinct
		nom_subsistema,
		nom_estado
	from LAB_PIPELINE.staging.stg_usina_disp
	where nom_subsistema is not null
)

select
	md5(cast(coalesce(cast(nom_subsistema as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nom_estado as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as id_dim_localidade,
	nom_subsistema,
	nom_estado
from distinct_localidades

order by nom_subsistema
    )
;


  