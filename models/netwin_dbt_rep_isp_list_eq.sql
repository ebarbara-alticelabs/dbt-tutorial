{{ config(materialized='table') }}

WITH source_data as (
  SELECT
    ins_equip.id_bd_equipamento                                          AS ID_ELEMENT,
    ins_loc.ID                                                           AS ID_PI,
    ins_loc.name                                                         AS NAME_PI,
    ins_loc.DESCRIPTION                                                  AS DESCRIPTION_PI,
    ins_loc.cat_entity_id                                                AS ID_TIPO_PI,
    ins_loc.cat_entity_i18n_id                                           AS TIPO_PI,
    ins_bas.id_bd_bastidor                                               AS ID_BASTIDOR,
    ins_bas.codigo                                                       AS BASTIDOR,
    cat_bas.nome_bastidor                                                AS TIPO_BASTIDOR,
    ins_bas.fiada                                                        AS FIADA_BASTIDOR,
    ins_bas.posicao                                                      AS POSICAO_BASTIDOR,
    cat_subbas.nome_subbastidor                                          AS TIPO_SUBBASTIDOR,
    ins_subbas.codigo                                                    AS CODIGO_SUBBASTIDOR,
    ins_subbas.posicao                                                   AS POSICAO_SUBBASTIDOR,
    cat_tipo_ne.id_bd_tipo_equipamento                                   AS ID_TIPO_EQUIPAMENTO,
    cat_tipo_ne.nome                                                     AS TIPO_EQUIPAMENTO,
    cat_tipo_ne2.id_bd_tipo_equipamento                                  AS ID_SUBTIPO_EQUIPAMENTO,
    cat_tipo_ne2.nome                                                    AS SUBTIPO_EQUIPAMENTO,
    ins_equip.id_bd_equipamento                                          AS ID_EQUIPAMENTO,
    ins_equip.identificacao                                              AS EQUIPAMENTO,
    ins_equip.designacao                                                 AS DESIGNACAO_EQUIPAMENTO,
    ins_equip.designacao_alternativa                                     AS ABREV_EQUIPAMENTO,
    cat_equip.id_bd_tipo_equip                                           AS ID_MODELO_EQUIPAMENTO,
    cat_equip.nome_equip                                                 AS MODELO_EQUIPAMENTO,
    cat_tec.id_tecnologia                                                AS ID_TECNOLOGIA,
    cat_tec.nome_tecnologia                                              AS TECNOLOGIA,
    ins_fab.id                                                           AS ID_FABRICANTE_EQUIPAMENTO,
    ins_fab.name                                                         AS FABRICANTE_EQUIPAMENTO,
    cat_forn.nome                                                        AS FORNECEDOR,
    proprietario.designacao_cliente                                      AS PROPRIETARIO,
    ins_equip.versao                                                     AS VERSAO,
    ins_equip.software_instalado                                         AS SOFT_INSTALADO,
    ins_equip.data_fabrico                                               AS DATA_AQUISICAO_EQUIP,
    ins_equip.data_instalacao                                            AS DATA_INSTALACAO_EQUIP,
    CASE 
      WHEN csus.name IN ('UNINSTALLED','REMOVED')
      THEN ins_equip.USAGE_STATE_DATE 
    END                                                                  AS DATA_SAIDA_SERVICO_EQUIP,
    csus.id                                                              AS ID_ESTADO_CICLO_VIDA_EQUIP,
    csus.name                                                            AS ESTADO_CICLO_VIDA_EQUIP,
    ins_equip.USAGE_STATE_DATE                                           AS DATA_ESTADO_CICLO_VIDA_EQUIP,
    csos.name                                                            AS ESTADO_OPERACIONAL_EQUIP,
    ins_equip.OPERATIONAL_STATE_DATE                                     AS DATA_ESTADO_OPERACIONAL_EQUIP,
    ins_equip.ip                                                         AS IP,
    ins_equip.subnetmask                                                 AS SUBMASK,
    ins_equip.gateway                                                    AS GATEWAY,
    replace(ins_equip.observacoes, chr(13) || chr(10), ' ')              AS OBSERVACOES_EQUIP,
    ins_rel.relacao_id_tipo                                              AS RELACAO_ID_TIPO,
    ins_rel.relacao_tipo                                                 AS RELACAO_TIPO,
    ins_rel.relacao_id_regra                                             AS RELACAO_ID_REGRA,
    ins_rel.relacao_regra                                                AS RELACAO_REGRA,
    nvl(ins_rel.relacao_existe, 'S')                                     AS RELACAO_EXISTE,
    ins_rel.equip2_id                                                    AS RELACIONADO_ID,
    ins_rel.equip2_nome                                                  AS RELACIONADO_NOME,
    ins_rel.equip2_descricao                                             AS RELACIONADO_DESCRICAO,
    ins_rel.equip2_abrev                                                 AS RELACIONADO_ABREV,
    ins_rel.equip2_id_tipo                                               AS RELACIONADO_ID_TIPO,
    ins_rel.equip2_tipo                                                  AS RELACIONADO_TIPO,
    ins_rel.equip2_id_estado                                             AS RELACIONADO_ID_ESTADO,
    ins_rel.equip2_estado                                                AS RELACIONADO_ESTADO,
    ins_rel.equip2_data_estado                                           AS RELACIONADO_DATA_ESTADO,
    ins_equip.UNIQUE_ID                                                  AS UNIQUE_ID
  FROM isp_ins_equipamento           ins_equip,
    isp_cat_modelo_equip          cat_equip,
    manufacturer                  ins_fab,
    isp_cat_fornecedor            cat_forn,
    cliente                       proprietario,
    isp_cat_tecnologia            cat_tec,
    isp_ins_unidade_funcional     uf,
    isp_ins_subb_un_funcional     sub_uf,
    isp_ins_subbastidor           ins_subbas,
    isp_cat_subbastidor           cat_subbas,
    isp_ins_bastidor              ins_bas,
    isp_cat_bastidor              cat_bas,
    isp_cat_tipo_equipamento      cat_tipo_ne,
    isp_cat_tipo_equipamento      cat_tipo_ne2,
    isp_ins_logic_grp_elem        ins_grupo_ele,
    REP_REPORT_LOCATIONS_WITH_CELL                      ins_loc,
    v_report_isp_lista_relacoes   ins_rel,
    ISP_CAT_TIPO_GRUPO_ELEMENTOS  tge,
    cat_state csus,
    cat_state csos
 WHERE ins_equip.id_bd_tipo_equip           = cat_equip.id_bd_tipo_equip(+)
   AND ins_equip.id_bd_equipamento          = ins_grupo_ele.valor(+)
   AND ins_equip.sigla_fabricante           = ins_fab.acronym(+)
   AND ins_equip.id_fornecedor              = cat_forn.id(+)
   AND cat_equip.sigla_tecnologia           = cat_tec.sigla_tecnologia(+)
   AND uf.id_bd_equipamento(+)              = ins_equip.id_bd_equipamento
   AND sub_uf.id_bd_unidade_funcional(+)    = uf.id_bd_unidade_funcional
   AND sub_uf.id_bd_subbastidor             = ins_subbas.id_bd_subbastidor(+)
   AND ins_subbas.id_bd_tipo_subbastidor    = cat_subbas.id_bd_tipo_subbastidor(+)
   AND ins_subbas.id_bd_bastidor            = ins_bas.id_bd_bastidor(+)
   AND ins_bas.id_bd_tipo_bastidor          = cat_bas.id_bd_tipo_bastidor(+)
   AND ins_equip.id_bd_tipo_ne              = cat_tipo_ne.id_bd_tipo_equipamento
   AND ins_equip.id_bd_subtipo_ne           = cat_tipo_ne2.id_bd_tipo_equipamento(+)
   AND ins_equip.id_bd_pi                   = ins_loc.id
   AND ins_equip.id_bd_cliente              = proprietario.id_bd_cliente (+)
   AND ins_equip.id_bd_equipamento          = ins_rel.equip1_id (+)
   AND cat_tipo_ne.visivel                  = 0
   AND (ins_grupo_ele.id_bd_tipo_grupo_elem = tge.id
    OR ins_grupo_ele.id_bd_tipo_grupo_elem  IS NULL)
   AND tge.UNIQUEKEY = 'EQUIPMENT'
   AND csus.id                              = ins_equip.USAGE_STATE_ID
   AND csos.id                              = ins_equip.OPERATIONAL_STATE_ID
)

select *
from source_data