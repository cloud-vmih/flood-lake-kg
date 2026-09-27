"""Shared source-field contracts for curated hydrological vectors."""

from typing import Final

BASINATLAS_RAW_FIELDS: Final[tuple[str, ...]] = (
    "HYBAS_ID",
    "NEXT_DOWN",
    "NEXT_SINK",
    "MAIN_BAS",
    "DIST_SINK",
    "DIST_MAIN",
    "SUB_AREA",
    "UP_AREA",
    "PFAF_ID",
    "SORT",
    "ele_mt_sav",
    "ele_mt_smn",
    "ele_mt_smx",
    "slp_dg_sav",
    "sgr_dk_sav",
    "lka_pc_sse",
    "dor_pc_pva",
    "rev_mc_usu",
    "for_pc_sse",
    "crp_pc_sse",
    "glc_pc_s22",
    "wet_pc_sg1",
    "wet_pc_sg2",
    "inu_pc_slt",
    "gwt_cm_sav",
    "run_mm_syr",
    "dis_m3_pyr",
    "dis_m3_pmx",
    "pop_ct_ssu",
    "ppd_pk_sav",
)

