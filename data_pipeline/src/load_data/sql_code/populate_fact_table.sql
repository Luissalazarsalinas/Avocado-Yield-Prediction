-- Populate Fact Table
INSERT INTO Factcrops(
    cultivo,
    Municipio,
    Topografia,
    year,
    annual_avg_temp,
    annual_avg_preci_mm,
    ph_agua_suelo_2_5_1_0,
    materia_org_nica_mo_porcent,
    f_sforo_p_bray_ii_mg_kg,
    azufre_s_fosfato_monocalcico_mg_kg,
    calcio_ca_intercambiable_cmol_kg,
    magnesio_mg_intercambiable_cmol_kg,
    potasio_k_intercambiable_cmol_kg,
    sodio_na_intercambiable_cmol_kg,
    capacidad_de_intercambio_cationico_cice_suma_de_bases_cmol_kg,
    conductividad_el_ctrica_ce_relacion_2_5_1_0_ds_m,
    hierro_fe_disponible_olsen_mg_kg,
    cobre_cu_disponible_mg_kg,
    manganeso_mn_disponible_olsen_mg_kg,
    zinc_zn_disponible_olsen_mg_kg,
    boro_b_disponible_mg_kg
)
SELECT
    cultivo,
    Municipio,
    Topografia,
    year,
    annual_avg_temp,
    annual_avg_preci_mm,
    ph_agua_suelo_2_5_1_0,
    materia_org_nica_mo_porcent,
    f_sforo_p_bray_ii_mg_kg,
    azufre_s_fosfato_monocalcico_mg_kg,
    calcio_ca_intercambiable_cmol_kg,
    magnesio_mg_intercambiable_cmol_kg,
    potasio_k_intercambiable_cmol_kg,
    sodio_na_intercambiable_cmol_kg,
    capacidad_de_intercambio_cationico_cice_suma_de_bases_cmol_kg,
    conductividad_el_ctrica_ce_relacion_2_5_1_0_ds_m,
    hierro_fe_disponible_olsen_mg_kg,
    cobre_cu_disponible_mg_kg,
    manganeso_mn_disponible_olsen_mg_kg,
    zinc_zn_disponible_olsen_mg_kg,
    boro_b_disponible_mg_kg
FROM standing_crops;

-- Update id keys 

UPDATE TABLE Factcrops
SET 
    crop_id = d1.crop_id,
    location_id = d2.location_id,
    soil_id = d3.soil_id,
    date_id = d4.date_id
FROM 
    Dimcrops AS d1,
    Dimlocation AS d2,
    Dimsoil AS d3,
    Dimdate AS d4
WHERE
    Factcrops.cultivo = d1.cultivo
    AND Factcrops.Municipio = d2.Municipio
    AND Factcrops.Topografia = d3.Topografia
    AND Factcrops.year = d4.year;

-- Drop columns
ALTER TABLE Factcrops DROP COLUMN cultivo;
ALTER TABLE Factcrops DROP COLUMN Municipio;
ALTER TABLE Factcrops DROP COLUMN Topografia;
ALTER TABLE Factcrops DROP COLUMN year;
