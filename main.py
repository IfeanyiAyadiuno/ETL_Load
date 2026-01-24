

# main.py
from snowflake_connector import SnowflakeConnector
from access_io import (
    connect_access,
    get_pressures_id_for_gas,
    get_well_name_for_gas,
    delete_cda_window,
    insert_cda_rows,
    update_cda_pressure_params,
    update_cda_wgr,
    update_cda_ecf,
    update_cda_cgr,
    debug_cda_window,
    update_cda_condensate_wh,
    update_cda_alloc_monthday,
)
from cda_build_step2 import build_gaswh_hours_slice
from cda_build_pressure_params import build_pressure_params_slice
from cda_build_wgr import build_wgr_slice
from cda_build_ecf import build_ecf_slice
from cda_build_cgr import build_cgr_slice
from cda_build_alloc_monthday import build_alloc_monthday_slice 



def main():
    gas_id = "886EFB8207A04F329A3C2A8E87687FD5"
    start_date = "2024-08-13"
    end_date_excl = "2024-11-20"  # exclusive end

    sf = SnowflakeConnector()

    # STEP A: base pull (GasWH + OnProdHours)
    df_base = build_gaswh_hours_slice(sf, gas_id, start_date, end_date_excl)
    print("\n=== BASE (GasWH + Hours) ===")
    print(df_base)

    with connect_access() as conn:
        cur = conn.cursor()

        # STEP B: lookup PressuresIDREC + Well Name in Access (PCE_WM)
        pressures_id = get_pressures_id_for_gas(cur, gas_id)
        if not pressures_id:
            raise RuntimeError(f"Could not find PressuresIDREC for GasIDREC={gas_id}")

        well_name = get_well_name_for_gas(cur, gas_id)
        if not well_name:
            raise RuntimeError(f"Could not find Well Name for GasIDREC={gas_id}")

        print("PressuresIDREC:", pressures_id)
        print("Well Name:", well_name)

        # attach WM lookup values into base rows (so CDA has them)
        df_base["PressuresIDREC"] = pressures_id
        df_base["Well Name"] = well_name

        # STEP C: refresh + insert base rows
        deleted = delete_cda_window(cur, gas_id, start_date, end_date_excl)
        print("Deleted existing rows:", deleted)

        inserted = insert_cda_rows(cur, df_base)
        conn.commit()
        print("Inserted rows:", inserted)

        debug_cda_window(cur, gas_id, start_date, end_date_excl)

        # STEP D: pressure params update (Tubing/Casing/Choke)
        df_params = build_pressure_params_slice(sf, pressures_id, start_date, end_date_excl)
        print("\n=== PRESSURE PARAMS SLICE ===")
        print(df_params)

        updated_params = update_cda_pressure_params(cur, df_params)
        conn.commit()
        print("Updated pressure cells:", updated_params)

        debug_cda_window(cur, gas_id, start_date, end_date_excl)

        # STEP E: WGR update
        df_wgr = build_wgr_slice(sf, pressures_id, start_date, end_date_excl)
        print("\n=== WGR SLICE ===")
        print(df_wgr)

        updated_wgr = update_cda_wgr(cur, df_wgr)
        conn.commit()
        print("Updated WGR cells:", updated_wgr)

        debug_cda_window(cur, gas_id, start_date, end_date_excl)

        # STEP F: ECF update (matches GasIDREC + date)
        df_ecf = build_ecf_slice(sf, gas_id, start_date, end_date_excl)
        print("\n=== ECF SLICE ===")
        print(df_ecf)

        updated_ecf = update_cda_ecf(cur, df_ecf)
        conn.commit()
        print("Updated ECF cells:", updated_ecf)

        debug_cda_window(cur, gas_id, start_date, end_date_excl)

        # STEP G: CGR update (CGR = RATEHCLIQ / RATEGAS, matches GasIDREC + date)
        df_cgr = build_cgr_slice(sf, pressures_id, start_date, end_date_excl)
        print("\n=== CGR SLICE ===")
        print(df_cgr)

        updated_cgr = update_cda_cgr(cur, df_cgr)
        conn.commit()
        print("Updated CGR cells:", updated_cgr)

        updated_cond = update_cda_condensate_wh(cur)
        print("Updated Condensate WH cells:", updated_cond)

        
        print("\n=== ALLOC MONTHDAY (GathGas + NewProdCond + NGL) ===")
        df_alloc = build_alloc_monthday_slice(sf, pressures_id, start_date, end_date_excl)
        print(df_alloc.head(10))
        updated_alloc = update_cda_alloc_monthday(cur, df_alloc)
        print("Updated AllocMonthDay cells:", updated_alloc)


        debug_cda_window(cur, gas_id, start_date, end_date_excl, limit=10)

        
        
        debug_cda_window(cur, gas_id, start_date, end_date_excl)

    sf.close()


if __name__ == "__main__":
    main()
