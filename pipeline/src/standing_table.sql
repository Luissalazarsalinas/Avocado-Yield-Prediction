-- Create a standing table 

CREATE TABLE standing_crops(
    stan_key SERIAL PRIMARY KEY,
    cod_dep INTEGER,
    Departamento VARCHAR(),
    Municipio VARCHAR(),
    grupo_cultivo VARCHAR(),
    subgrupo_cultivo VARCHAR(),
    cultivo VARCHAR(),
    sistema_productivo VARCHAR(),
    year INTEGER,
    area_sembrada_ha VARCHAR(),
    area_cosechada_ha VARCHAR(),
    rendimiento_t_h
