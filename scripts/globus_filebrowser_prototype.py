import sys
import os
from pathlib import Path
from typing import Optional, List

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QComboBox, QLineEdit, QPushButton, QTableWidget, QTableWidgetItem,
    QHeaderView, QMessageBox, QLabel, QAbstractItemView
)
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QIcon

from phoglobushelpers.PhoGlobusHelper import GlobusConnector

class GlobusFileBrowser(QMainWindow):
    def __init__(self, connector: GlobusConnector):
        super().__init__()
        self.connector = connector
        self.bookmarks = []
        self.current_endpoint_id = ""
        self.current_path = ""
        
        self.setWindowTitle("Globus Native File Browser")
        self.resize(900, 600)
        
        self.setup_ui()
        self.load_bookmarks()

    def setup_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # --- Top Navigation Bar ---
        nav_layout = QHBoxLayout()
        
        self.bookmark_combo = QComboBox()
        self.bookmark_combo.currentIndexChanged.connect(self.on_bookmark_changed)
        nav_layout.addWidget(QLabel("Endpoint:"))
        nav_layout.addWidget(self.bookmark_combo, stretch=1)
        
        main_layout.addLayout(nav_layout)
        
        path_layout = QHBoxLayout()
        self.btn_up = QPushButton("⬆ Up")
        self.btn_up.clicked.connect(self.go_up)
        path_layout.addWidget(self.btn_up)
        
        self.path_edit = QLineEdit()
        self.path_edit.returnPressed.connect(self.on_path_entered)
        path_layout.addWidget(QLabel("Path:"))
        path_layout.addWidget(self.path_edit, stretch=1)
        
        self.btn_refresh = QPushButton("Refresh")
        self.btn_refresh.clicked.connect(self.refresh_files)
        path_layout.addWidget(self.btn_refresh)
        
        main_layout.addLayout(path_layout)
        
        # --- File Table ---
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["Name", "Type", "Size", "Last Modified"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.doubleClicked.connect(self.on_table_double_clicked)
        
        main_layout.addWidget(self.table)

    def load_bookmarks(self):
        self.bookmark_combo.clear()
        try:
            bookmark_list = self.connector.get_bookmarks()
            self.bookmarks = bookmark_list.DATA
            for b in self.bookmarks:
                self.bookmark_combo.addItem(b.name, b)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load bookmarks:\n{e}")

    def on_bookmark_changed(self, index):
        if index < 0:
            return
        bookmark = self.bookmarks[index]
        self.current_endpoint_id = bookmark.endpoint_id
        self.current_path = bookmark.path if bookmark.path else "/"
        self.path_edit.setText(self.current_path)
        self.refresh_files()

    def on_path_entered(self):
        self.current_path = self.path_edit.text()
        self.refresh_files()

    def go_up(self):
        if not self.current_path or self.current_path == "/":
            return
        # Remove trailing slash for path manipulation
        path = self.current_path.rstrip('/')
        if not path:
            parent_path = "/"
        else:
            parent_path = str(Path(path).parent)
            # Ensure it ends with slash if it's not root, or just let Globus handle it
            if not parent_path.endswith('/'):
                parent_path += '/'
        
        self.current_path = parent_path
        self.path_edit.setText(self.current_path)
        self.refresh_files()

    def refresh_files(self):
        if not self.current_endpoint_id or not self.current_path:
            return
            
        self.table.setRowCount(0)
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            # Using list_files.
            # We pass filter=None to get both files and directories, as should_list_recursively
            # defaults to False so it only queries the single directory.
            file_list = self.connector.list_files(
                endpoint=self.current_endpoint_id,
                path=self.current_path,
                filter=None 
            )
            files = file_list.DATA
            
            # Sort: Directories first, then alphabetically
            files.sort(key=lambda x: (0 if x.type.value == 'dir' else 1, x.name.lower()))
            
            self.table.setRowCount(len(files))
            for row, f in enumerate(files):
                # Name
                item_name = QTableWidgetItem(f.name)
                # Store type in UserRole so we can retrieve it on double click
                item_name.setData(Qt.UserRole, f.type.value)
                self.table.setItem(row, 0, item_name)
                
                # Type
                item_type = QTableWidgetItem("Folder" if f.type.value == 'dir' else "File")
                self.table.setItem(row, 1, item_type)
                
                # Size
                size_str = self.format_size(f.size) if f.size is not None else ""
                item_size = QTableWidgetItem(size_str)
                item_size.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self.table.setItem(row, 2, item_size)
                
                # Date
                date_str = f.last_modified[:19].replace('T', ' ') if f.last_modified else ""
                item_date = QTableWidgetItem(date_str)
                self.table.setItem(row, 3, item_date)
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to list files:\n{e}")
        finally:
            QApplication.restoreOverrideCursor()

    def on_table_double_clicked(self, index):
        row = index.row()
        item = self.table.item(row, 0)
        name = item.text()
        item_type = item.data(Qt.UserRole)
        
        if item_type == 'dir':
            # Navigate into directory
            if not self.current_path.endswith('/'):
                self.current_path += '/'
            self.current_path += name + '/'
            self.path_edit.setText(self.current_path)
            self.refresh_files()

    @staticmethod
    def format_size(size_bytes: int) -> str:
        if size_bytes == 0:
            return "0 B"
        size_name = ("B", "KB", "MB", "GB", "TB")
        import math
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_name[i]}"


def main():
    app = QApplication(sys.argv)
    
    # Initialize Globus Connector
    # If the user is not authenticated or token is expired, this will prompt in terminal
    print("Initializing Globus connection...")
    try:
        connector = GlobusConnector.login_and_get_transfer_client()
    except Exception as e:
        print(f"Failed to initialize Globus connection: {e}")
        sys.exit(1)
        
    window = GlobusFileBrowser(connector)
    window.show()
    
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
