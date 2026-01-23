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
    debug_cda_window,
)
from cda_build_step2 import build_gaswh_hours_slice
from cda_build_pressure_params import build_pressure_params_slice
from cda_build_wgr import build_wgr_slice


def main():
    gas_id = "886EFB8207A04F329A3C2A8E87687FD5"
    start_date = "2024-10-13"
    end_date_excl = "2024-10-16"  # exclusive end

    sf = SnowflakeConnector()

    # STEP A: base pull
    df_base = build_gaswh_hours_slice(sf, gas_id, start_date, end_date_excl)
    print("\n=== BASE (GasWH + Hours) ===")
    print(df_base)

    with connect_access() as conn:
        cur = conn.cursor()

        # STEP B: lookup PressuresIDREC in Access WM
        pressures_id = get_pressures_id_for_gas(cur, gas_id)
        if not pressures_id:
            raise RuntimeError(f"Could not find PressuresIDREC for GasIDREC={gas_id}")
        

        well_name = get_well_name_for_gas(cur, gas_id)
        if not well_name:
            raise RuntimeError(f"Could not find Well Name for GasIDREC={gas_id}")
        
        print("PressuresIDREC:", pressures_id)
        print("Well Name:", well_name)
       
        # attach pressure id into the rows we're inserting
        df_base["PressuresIDREC"] = pressures_id
        df_base["Well Name"] = well_name

        # STEP C: refresh + insert base rows
        deleted = delete_cda_window(cur, gas_id, start_date, end_date_excl)
        print("Deleted existing rows:", deleted)

        inserted = insert_cda_rows(cur, df_base)
        conn.commit()
        print("Inserted rows:", inserted)

        debug_cda_window(cur, gas_id, start_date, end_date_excl)

        # STEP D: pressure params update
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

    sf.close()


if __name__ == "__main__":
    main()
