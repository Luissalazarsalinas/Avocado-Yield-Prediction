-- Populate dim tables

-- Crops dim table
INSERT INTO Dimcrops(
    grupo_cultivo,
    subgrupo_cultivo,
    cultivo,
    sistema_productivo,
    estado_fisico_prod,
    nombre_cientifico,
    ciclo_cultivo
) 
SELECT  
    grupo_cultivo,
    subgrupo_cultivo,
    cultivo,
    sistema_productivo,
    estado_fisico_prod,
    nombre_cientifico,
    ciclo_cultivo
FROM standing_crops;

-- Dim location
INSERT INTO Dimlocation(
    cod_dep,
    Departamento,
    cod_mun,
    Municipio
) 
SELECT  
    cod_dep,
    Departamento,
    cod_mun,
    Municipio
FROM standing_crops;

-- Dim soil
INSERT INTO Dimsoil(
    Topografia,
    Drenaje
) 
SELECT  
    Topografia,
    Drenaje
FROM standing_crops;

-- Dim date
INSERT INTO Dimdate(
    year,
    periodo 
) 
SELECT  
    year,
    periodo 
FROM standing_crops;
