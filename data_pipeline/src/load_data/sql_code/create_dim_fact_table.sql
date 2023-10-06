-- Create dimentionals and fact tables 

CREATE TABLE Dimcrops(
    crop_id SERIAL PRIMARY KEY,
    grupo_cultivo VARCHAR(100),
    subgrupo_cultivo VARCHAR(100),
    cultivo VARCHAR(50),
    sistema_productivo VARCHAR(50),
    estado_fisico_prod VARCHAR(20),
    nombre_cientifico VARCHAR(20),
    ciclo_cultivo VARCHAR(20)
);

CREATE TABLE Dimlocation(
    location_id SERIAL PRIMARY KEY,
    cod_dep INTEGER,
    Departamento VARCHAR(40),
    cod_mun INTEGER,
    Municipio VARCHAR(50)
);

CREATE TABLE Dimsoil(
    soil_id SERIAL PRIMARY KEY,
    Topografia VARCHAR(30),
    Drenaje VARCHAR(30)
);

CREATE TABLE Dimdate(
    date_id SERIAL PRIMARY KEY,
    year INTEGER,
    periodo VARCHAR(14)
);

CREATE TABLE Factcrops(
    crop_id INTEGER REFERENCES Dimcrops(crop_id) ON DELETE CASCADE,
    location_id INTEGER REFERENCES Dimlocation(location_id) ON DELETE CASCADE,
    soil_id INTEGER REFERENCES Dimsoil(soil_id) ON DELETE CASCADE,
    date_id INTEGER REFERENCES Dimdate(date_id) ON DELETE CASCADE,
    cultivo VARCHAR(50),
    Municipio VARCHAR(50),
    Topografia VARCHAR(30),
    year INTEGER,
    annual_avg_temp double precision,
    annual_avg_preci_mm double precision,
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
    boro_b_disponible_mg_kg DECIMAL(10,2),

    CONSTRAINT pk_Factcrops PRIMARY KEY(crop_id, location_id,soil_id, date_id)
);