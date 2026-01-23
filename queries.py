# queries.py
# simple dictionary of sql strings, each capped with NEWEST_N rows for sampling

NEWEST_N = 10

QUERIES = {
    "meter_orifice_entries": f"""
        SELECT 
            IDRECPARENT,
            IDREC,
            DTTM,
            VOLENTERGAS,
            DURONOR 
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEntry
        ;
    """,

    "unit_comp_params": f"""
        SELECT 
            IDRECPARENT,
            DTTM,
            PRESTUB,
            PRESCAS,
            SZCHOKE 
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompParam

        ;
    """,

    "well_ratios": f"""
        SELECT 
            IDRECPARENT, 
            DTTM,
            CGR,
            WGR 
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompRatios
        ;
    """,

    "meter_orifice_ecf": f"""
        SELECT 
            IDRECPARENT,
            DTTM,
            EFFLUENTFACTOR 
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEcf
        ;
    """,

    "well_daily_gathered": f"""
        SELECT 
            IDRECPARENT,
            DTTM,
            RATEHCLIQ,
            RATEGAS 
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompGathMonthDayCalc
        ;
    """,

    "unit_alloc_monthday": f"""
        SELECT 
            IDRECCOMP,DTTM,
            VOLNEWPRODALLOCGAS,VOLNEWPRODALLOCCOND,
            VOLPRODALLOCWATER,VOLNEWPRODALLOCNGL,
            VolProdGathGas,VOLPRODGATHHCLIQ 
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitAllocMonthDay
        ;
    """,

    "comp_gath_monthday_calc": f"""
        SELECT 
            IDRECCOMP, 
            DTTM, 
            VOLWATER,
            RATEGAS,
            RATEHCLIQ
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompGathMonthDayCalc
        ;
    """,
}










