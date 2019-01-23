drop table kkk;
create temp table kkk as SELECT consdocdeu,COUNT(*) FROM traslados WHERE estado = 'A' GROUP BY 1 HAVING COUNT(*)>1;
alter table traslados add constraslados serial;
drop table dejar
create temp table dejar as SELECT *FROM traslados where consdocdeu in (SELECT consdocdeu FROM kkk) and estado='A' and consdocdeu = docdeu.consdocdeu and traslados.cod_abogado=docdeu.cod_abogado and traslados.consconditras= docdeu.consconditras and traslados.fecha_traslado = docdeu.fecha_corte
drop table borrar
create temp table borrar as SELECT *FROM traslados where consdocdeu in (SELECT consdocdeu FROM kkk) and estado='A' and constraslados not in (select constraslados from dejar)

drop table fechas_dejar
create temp table fechas_dejar AS SELECT constraslados,estado,fecha_traslado ,fecha_cierre ,consdocdeu ,cod_abogado ,consconditras ,total_Gesi+(fecha_cierre-fecha_traslado) as total_gescierre,
ROW_NUMBER() OVER(PARTITION BY consdocdeu,cod_abogado,consconditras ORDER BY fecha_traslado asc)           AS orden_traslado   
,LEAD(fecha_traslado) OVER (PARTITION BY consdocdeu ORDER BY fecha_traslado ASC) AS fecha_traslado_sig   
from traslados where consdocdeu in (select consdocdeu from kkk) and traslados.estado='A' order by consdocdeu,fecha_traslado

UPDATE traslados t SET estado = 'C',
vlr_unificacion = (vlr_actual-vlr_abon),
cuot_unificacion = (cuot_actual-cuot_abon),
vlr_reti = (vlr_actual-vlr_abon),
cuot_reti = (cuot_actual-cuot_abon),
vlr_pag = 0,
cuot_cierre = (CASE WHEN d.noctasvenc IS null THEN 0 ELSE d.noctasvenc END ),
vlr_cierre = d.valorvencido,
fecha_cierre = fechas_dejar.fecha_traslado_sig,
cod_gest = 42
FROM docdeu d
WHERE t.estado = 'A'
AND t.consdocdeu = borrar.consdocdeu
AND t.consdocdeu = d.consdocdeu
and t.constraslados=fechas_dejar.constraslados
and fechas_dejar.fecha_traslado_sig is not null


INSERT INTO segui (    consegui,   concontrol,   nit,   nrodoc,   codcob,   fechasegui,   grabador,   codclasges,   nota,   fechvenci,   lote,   consdocdeu,   noctasvenc ,   codcob_ant,   regional,   cod_abogado,   cod_agente)
SELECT NEXTVAL('segui_consegui_seq'),concontrol,nit,nrodoc,4002,fechas_dejar.fecha_traslado_sig,'admon','OTROS','RETIRO OBLIGACIÓN Altura: '||fechas_dejar.total_gescierre||' CON EL TRASLADO '||fechas_dejar.consconditras,fechvenci,lote,consdocdeu ,case when noctasvenc is null then 0 else noctasvenc end,codcob,regional,cod_abogado,cod_agente 
FROM docdeu where  docdeu.consdocdeu = borrar.consdocdeu and borrar.constraslados = fechas_dejar.constraslados and fechas_dejar.fecha_traslado_sig is not null

commit


-- TRASLADO DE GESTION ABIERTO MAS DE UNA VEZ
drop table kkk

create table kkk as 
SELECT
    fecha_traslado
    ,consdocdeu
    ,cod_abogado
    ,consconditras
    ,COUNT(*)
FROM traslados
WHERE estado = 'A'
GROUP BY 1,2,3,4
HAVING COUNT(*)>1
;

SELECT * FROM kkk
drop table detalle
create temp table detalle as SELECT consdocdeu,fecha_traslado,fecha_cierre,cod_abogado,estado,constraslados,total_gesi,consconditras,ROW_NUMBER () OVER (ORDER BY consdocdeu) as id FROM traslados where consdocdeu =  kkk.consdocdeu
drop table cerrar
create temp table cerrar as SELECT * FROM detalle where id % 2 = 1

UPDATE traslados t SET estado = 'C',
vlr_unificacion = (vlr_actual-vlr_abon),
cuot_unificacion = (cuot_actual-cuot_abon),
vlr_reti = (vlr_actual-vlr_abon),
cuot_reti = (cuot_actual-cuot_abon),
vlr_pag = 0,
cuot_cierre = (CASE WHEN d.noctasvenc IS null THEN 0 ELSE d.noctasvenc END ),
vlr_cierre = d.valorvencido,
fecha_cierre = fecha_traslado,
cod_gest = 42
FROM docdeu d
WHERE t.estado = 'A'
AND t.consdocdeu = d.consdocdeu
and t.constraslados = cerrar.constraslados

select estado from traslados where constraslados= cerrar.constraslados
SELECT * FROM cerrar

begin

INSERT INTO segui
(
   consegui,
   concontrol,
   nit,
   nrodoc,
   codcob,
   fechasegui,
   grabador,
   codclasges,
   nota,
   fechvenci,
   lote,
   consdocdeu,
   noctasvenc ,
   codcob_ant,
   regional,
   cod_abogado,
   cod_agente
)
SELECT
NEXTVAL('segui_consegui_seq'),
concontrol,
nit,
nrodoc,
4002,
now(),
'admon',
'OTROS',
'RETIRO OBLIGACIÓN Altura: '||cerrar.total_gesi||' CON EL TRASLADO '||cerrar.consconditras,
fechvenci,
lote,
consdocdeu ,
case when noctasvenc is null then 0 else noctasvenc end,
codcob,
regional,
cod_abogado,
cod_agente
FROM docdeu
where consdocdeu = cerrar.consdocdeu

commit





-- TRASLADOS ABIERTO CON OBLIGACIONES CERRADAS
create temp table sss as SELECT t.*
FROM traslados t
LEFT JOIN docdeu d
ON(t.consdocdeu = d.consdocdeu)
WHERE t.estado = 'A'
  AND d.estado = 'C'
  ;

UPDATE traslados t SET estado = 'C',
vlr_unificacion = (vlr_actual-vlr_abon),
cuot_unificacion = (cuot_actual-cuot_abon),
vlr_reti = (vlr_actual-vlr_abon),
cuot_reti = (cuot_actual-cuot_abon),
vlr_pag = 0,
cuot_cierre = (CASE WHEN d.noctasvenc IS null THEN 0 ELSE d.noctasvenc END ),
vlr_cierre = d.valorvencido,
fecha_cierre = date(now()),
cod_gest = 42
FROM docdeu d
WHERE t.estado = 'A'
AND t.consdocdeu = d.consdocdeu
and t.consdocdeu = sss.consdocdeu;
;
