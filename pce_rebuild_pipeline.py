"""
Shared post-production-rebuild steps used after PCE_Production insert/rebuild.

Order (unchanged across full rebuild, quick update, and type-curve import):
1. Materialize PCE_TC into PCE_Production
2. Sync WM UWI to Production + Allocation_Factors
3. Refresh NGL ratios from Allocation_Factors
4. Sync WM metadata (pad, enersight, month) to Production
5. Rebuild PCE_FRCST_PRD

Routine update runs a separate full-table sequence rebuild after its window insert
(see prodview_update_gui.run_quick_update); full rebuild already calculates
sequences when building from CDA.
"""

from __future__ import annotations

from datetime import date
from typing import Callable, Optional, Tuple

import log_format as lf


def run_post_production_rebuild_steps(
    log: Callable[[str], None],
    *,
    conn=None,
    date_window: Optional[Tuple[date, date]] = None,
    cancel_event=None,
    include_ngl: bool = True,
    include_uwi_sync: bool = True,
    include_wm_metadata: bool = True,
    include_frcst_rebuild: bool = True,
) -> bool:
    """
    Run the standard tail pipeline after gathered production rows are written.

    Returns False if cancelled during NGL refresh; True otherwise.
    """
    from db_connection import get_sql_conn
    from production_update import (
        _refresh_ngl_from_allocation_factors,
        sync_production_wm_metadata_combined_sql,
        sync_wm_uwi_to_downstream_sql,
    )
    from sync_typecurves_to_production import sync_tc_to_production

    log(lf.step("Materializing PCE_TC into PCE_Production..."))
    try:
        sync_tc_to_production(log_callback=log, conn=conn)
    except Exception as e:
        log(lf.warn(f"PCE_TC → PCE_Production sync: {e}"))

    if include_uwi_sync:
        log(lf.step("Syncing WM UWI to Production and Allocation_Factors…"))
        if conn is not None:
            cur = conn.cursor()
            sync_wm_uwi_to_downstream_sql(cur)
            conn.commit()
        else:
            with get_sql_conn() as uwi_conn:
                cur = uwi_conn.cursor()
                sync_wm_uwi_to_downstream_sql(cur)
                uwi_conn.commit()

    if include_ngl:
        log(lf.step("Refreshing NGL ratios from Allocation_Factors..."))
        if not _refresh_ngl_from_allocation_factors(
            log=log,
            cancel_event=cancel_event,
            conn=conn,
            date_window=date_window,
        ):
            log(lf.warn("Cancelled during NGL ratio refresh."))
            return False

    if include_wm_metadata:
        log(lf.step("Syncing WM metadata (pad, enersight, month) to Production…"))

        def _run_metadata(cur, window: Optional[Tuple[date, date]]) -> None:
            sync_production_wm_metadata_combined_sql(cur, pad_date_window=window)

        if conn is not None:
            cur = conn.cursor()
            _run_metadata(cur, date_window)
            conn.commit()
        else:
            with get_sql_conn() as meta_conn:
                cur = meta_conn.cursor()
                _run_metadata(cur, date_window)
                meta_conn.commit()

    if include_frcst_rebuild:
        try:
            from pce_frcst_prd_rebuild import rebuild_pce_frcst_prd

            rebuild_pce_frcst_prd(log=log, conn=conn)
        except Exception as e:
            log(lf.warn(f"PCE_FRCST_PRD rebuild: {e}"))

    return True
