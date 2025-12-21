import sys
import os
import zipfile
from PySide6.QtWidgets import (QApplication, QMainWindow, QVBoxLayout, QWidget,
                               QPushButton, QLabel, QFileDialog, QMessageBox,
                               QProgressBar, QSpinBox, QLineEdit)
from PySide6.QtCore import QDir, QThread, Signal, QObject, QSettings
from processing import reanme_msv, DataWrangler_MS_data_conversion_v1,AMDIS_batch_data_formatterv1, msconvert_python


class Worker(QObject):
    progress = Signal(int)
    message = Signal(str)
    finished = Signal()

    def __init__(self, zip_path, extract_dir, start_idx, ms_convert_path):
        super().__init__()
        self.zip_path = zip_path
        self.extract_dir = extract_dir
        self.start_idx = start_idx
        self.ms_convert_path = ms_convert_path

        #make directories
        os.makedirs(extract_dir + "/1-msv", exist_ok=True)
        os.makedirs(extract_dir + "/3-mlt", exist_ok=True)
        os.makedirs(extract_dir + "/5-mzmlv2", exist_ok=True)
        os.makedirs(extract_dir + "/6-mzxml", exist_ok=True)

    def run(self):
        try:
            # Extract with progress
            with zipfile.ZipFile(self.zip_path, 'r') as zip_ref:
                total_files = len(zip_ref.namelist())
                processed_files = 0

                for file in zip_ref.namelist():
                    zip_ref.extract(file, self.extract_dir + "/1-msv")
                    processed_files += 1
                    progress = int((processed_files / total_files) * 100)
                    self.progress.emit(progress)
                    self.message.emit(f"Extracting: {file}")

            # Process files in msv
            files = QDir(self.extract_dir + "/1-msv").entryList(QDir.Files)
            total_files = len(files) * 3

            pf_count = reanme_msv.rename_msv_files(self.extract_dir + "/1-msv", self.start_idx, self.progress, self.message, 0, total_files)
            pf_count = DataWrangler_MS_data_conversion_v1.batch_processing_MS(self.extract_dir + "/1-msv", self.extract_dir + "/3-mlt", self.progress, self.message, pf_count, total_files)
            pf_count = AMDIS_batch_data_formatterv1.batch_process_mzml(input_root = self.extract_dir + "/1-msv", output_root = self.extract_dir + "/5-mzmlv2", progress_signal = self.progress, message_signal = self.message, pstart = pf_count, total_files = total_files)
            msconvert_python.convert_mzml_to_mzxml(self.extract_dir + "/5-mzmlv2", self.extract_dir + "/6-mzxml", self.ms_convert_path, self.progress, self.message, pf_count, total_files)



            self.finished.emit()
        except Exception as e:
            self.message.emit(f"Error: {str(e)}")
            self.finished.emit()

class ZipExtractorApp(QMainWindow):
    SETTINGS_KEY = "msconvert_path"
    def __init__(self):
        super().__init__()
        self.setWindowTitle("UNM data conversion")
        self.setGeometry(100, 100, 400, 300)

        #init settings
        self.settings = QSettings("UNM", "city data converter")

        #put msconvert path on gui
        self.path_edit = QLineEdit()
        self.path_edit.setPlaceholderText("Path to msconvert executable")

        # Load saved path if available
        saved_path = self.settings.value(self.SETTINGS_KEY)
        if saved_path:
            self.path_edit.setText(saved_path)

        # Main widget and layout
        self.main_widget = QWidget()
        self.setCentralWidget(self.main_widget)
        self.layout = QVBoxLayout()
        self.main_widget.setLayout(self.layout)


        # Widgets
        self.zip_label = QLabel("No ZIP file selected")
        self.select_button = QPushButton("Select ZIP File")
        self.extract_process_button = QPushButton("Extract and Process")
        self.extract_process_button.setEnabled(False)
        self.progress_bar = QProgressBar()
        self.status_label = QLabel("Ready")
        self.layout.addWidget(self.path_edit)

        # Add widgets to layout
        self.layout.addWidget(self.zip_label)
        self.layout.addWidget(self.select_button)
        self.layout.addWidget(self.extract_process_button)
        self.layout.addWidget(self.progress_bar)
        self.layout.addWidget(self.status_label)

        # start index spinbox
        self.label = QLabel("Enter an integer value for start index:")
        self.layout.addWidget(self.label)
        self.idxspinbox = QSpinBox()
        self.idxspinbox.setMinimum(1)
        self.idxspinbox.setValue(1)
        self.layout.addWidget(self.idxspinbox)


        # Connect signals
        self.select_button.clicked.connect(self.select_zip_file)
        self.extract_process_button.clicked.connect(self.start_extraction)

        # Instance variables
        self.zip_file_path = ""
        self.extract_dir = ""
        self.worker = None
        self.thread = None

    def select_zip_file(self):
        """Open file dialog to select ZIP file"""
        file_dialog = QFileDialog()
        file_dialog.setFileMode(QFileDialog.ExistingFile)
        file_dialog.setNameFilter("ZIP files (*.zip)")

        if file_dialog.exec():
            self.zip_file_path = file_dialog.selectedFiles()[0]
            self.zip_label.setText(f"Selected: {os.path.basename(self.zip_file_path)}")
            self.extract_process_button.setEnabled(True)

    def start_extraction(self):
        """Start the extraction and processing in a separate thread"""
        if not self.zip_file_path:
            QMessageBox.warning(self, "Error", "No ZIP file selected!")
            return

        # Ask for extraction directory
        dir_dialog = QFileDialog()
        dir_dialog.setFileMode(QFileDialog.Directory)
        dir_dialog.setOption(QFileDialog.ShowDirsOnly, True)

        if dir_dialog.exec():
            self.extract_dir = dir_dialog.selectedFiles()[0]
            self.progress_bar.setValue(0)
            self.status_label.setText("Preparing...")

            # Create and start worker thread
            self.thread = QThread()
            self.worker = Worker(self.zip_file_path, self.extract_dir, self.idxspinbox.value(), self.get_msconvert_path())
            self.worker.moveToThread(self.thread)

            # Connect signals
            self.worker.progress.connect(self.update_progress)
            self.worker.message.connect(self.update_status)
            self.worker.finished.connect(self.thread.quit)
            self.worker.finished.connect(self.worker.deleteLater)
            self.worker.finished.connect(self.thread.deleteLater)
            self.worker.finished.connect(self.on_finished)

            self.thread.started.connect(self.worker.run)
            self.thread.start()

            self.extract_process_button.setEnabled(False)

    def update_progress(self, value):
        """Update progress bar"""
        self.progress_bar.setValue(value)

    def update_status(self, message):
        """Update status label"""
        self.status_label.setText(message)

    def on_finished(self):
        """Called when processing is complete"""
        self.extract_process_button.setEnabled(True)
        QMessageBox.information(self, "Complete", "Extraction and processing finished!")

    def save_settings(self):
        """Save the current path to settings file"""
        self.settings.setValue(self.SETTINGS_KEY, self.path_edit.text())

    def get_msconvert_path(self):
        """Get the current path (without saving)"""
        return self.path_edit.text()

    #overide
    def closeEvent(self, event):
        """Save settings when widget is closed"""
        self.save_settings()
        super().closeEvent(event)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ZipExtractorApp()
    window.show()
    sys.exit(app.exec())