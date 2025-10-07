

-- Dimensão de usina construída a partir da união de todos os meses ingeridos
-- Normaliza nomes de colunas e remove duplicatas mantendo só atributos únicos
with distinct_usinas as (
	select distinct
		nom_usina,
		nom_tipocombustivel,
		ceg
	from LAB_PIPELINE.staging.stg_usina_disp
	where nom_usina is not null
)

select
	md5(cast(coalesce(cast(nom_usina as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nom_tipocombustivel as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ceg as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as id_dim_usina,
	nom_usina,
	nom_tipocombustivel,
	ceg
from distinct_usinas

order by nom_usina