-- Create a standing table 

CREATE TABLE standing_crops(
    stan_key SERIAL PRIMARY KEY,
    cod_dep INTEGER,
    Departamento VARCHAR(),
    cod_mun INTEGER,
    Municipio VARCHAR(),
    grupo_cultivo VARCHAR(),
    subgrupo_cultivo VARCHAR(),
    cultivo VARCHAR(),
    sistema_productivo VARCHAR(),
    year INTEGER,
    periodo VARCHAR(),
    area_sembrada_ha INTEGER,
    area_cosechada_ha INTEGER,
    produccion_t double precision,
    rendimiento_t_h double precision,
    estado_fisico_prod VARCHAR(),
    nombre_cientifico VARCHAR(),
    ciclo_cultivo VARCHAR(),
    annual_avg_temp double precision,
    annual_avg_preci_mm double precision,
    Topografia VARCHAR(),
    Drenaje VARCHAR(),
    ph_agua_suelo_2_5_1_0 DECIMAL(10,2),
    materia_org_nica_mo_porcent DECIMAL(10,2),
    f_sforo_p_bray_ii_mg_kg DECIMAL(10,2),
    azufre_s_fosfato_monocalcico_mg_kg DECIMAL(10,2),
    calcio_ca_intercambiable_cmol_kg DECIMAL(10,2),
    magnesio_mg_intercambiable_cmol_kg DECIMAL(10,2),
    potasio_k_intercambiable_cmol_kg DECIMAL(10,2),
    sodio_na_intercambiable_cmol_kg DECIMAL(10,2),
    capacidad_de_intercambio_cationico_cice_suma_de_bases_cmol_kg DECIMAL(10,2),
    conductividad_el_ctrica_ce_relacion_2_5_1_0_ds_m DECIMAL(10,2),
    hierro_fe_disponible_olsen_mg_kg DECIMAL(10,4),
    cobre_cu_disponible_mg_kg DECIMAL(10,4),
    manganeso_mn_disponible_olsen_mg_kg DECIMAL(10,4),
    zinc_zn_disponible_olsen_mg_kg DECIMAL(10,4),
    boro_b_disponible_mg_kg DECIMAL(10,2)

);