# well_master_cda_worker.py — background CDA populate after new well import

from PyQt5.QtCore import QThread, pyqtSignal


class CdaPopulateWorker(QThread):
    """Background worker to populate PCE_CDA for specific wells."""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(dict)

    def __init__(self, mapping_df, start_date, end_date, parent=None):
        super().__init__(parent)
        self.mapping_df = mapping_df
        self.start_date = start_date
        self.end_date = end_date

    def run(self):
        from prodview_update_gui import populate_wells_cda
        result = populate_wells_cda(
            self.mapping_df,
            self.start_date,
            self.end_date,
            progress_callback=self.progress_signal.emit,
            log_callback=self.log_signal.emit,
        )
        self.finished_signal.emit(result)
