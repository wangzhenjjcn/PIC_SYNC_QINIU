import json
import os
import sys
from pathlib import Path
from typing import Optional
import queue
from datetime import datetime

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QFileDialog,
    QPlainTextEdit,
    QSpinBox,
    QMessageBox,
    QCheckBox,
    QDialog,
    QFormLayout,
    QDialogButtonBox,
)
from PySide6.QtGui import QAction

from sync.config import AppConfig, load_config, save_config, default_config_path, app_data_dir
from sync.device_id import ensure_device_id
from sync.scheduler import SyncEngine
from sync.crypto_util import encrypt_to_base64, decrypt_from_base64
from dataclasses import asdict
import time


class SettingsDialog(QDialog):
    def __init__(self, cfg: AppConfig, parent=None):
        super().__init__(parent)
        self.setWindowTitle("首选项")
        self.cfg = cfg

        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.ak_edit = QLineEdit(self.cfg.qiniu_access_key or "")
        self.sk_edit = QLineEdit(self.cfg.qiniu_secret_key or "")
        self.sk_edit.setEchoMode(QLineEdit.Password)
        self.bucket_edit = QLineEdit(self.cfg.qiniu_bucket or "")
        self.domain_edit = QLineEdit(self.cfg.qiniu_domain or "")
        self.region_edit = QLineEdit(self.cfg.qiniu_region or "")

        self.interval_spin = QSpinBox()
        self.interval_spin.setRange(1, 120)
        self.interval_spin.setValue(self.cfg.scan_interval_minutes or 5)

        self.force_cb = QCheckBox("强制上传(忽略锁)")
        self.force_cb.setChecked(bool(getattr(self.cfg, "force_upload_ignore_lock", False)))

        form.addRow("AccessKey:", self.ak_edit)
        form.addRow("SecretKey:", self.sk_edit)
        form.addRow("Bucket:", self.bucket_edit)
        form.addRow("域名(含协议):", self.domain_edit)
        form.addRow("Region(可选):", self.region_edit)
        form.addRow("扫描间隔(分钟):", self.interval_spin)
        form.addRow("上传选项:", self.force_cb)
        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.save_and_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def save_and_accept(self):
        self.cfg.qiniu_access_key = self.ak_edit.text().strip()
        self.cfg.qiniu_secret_key = self.sk_edit.text().strip()
        self.cfg.qiniu_bucket = self.bucket_edit.text().strip()
        self.cfg.qiniu_domain = self.domain_edit.text().strip()
        self.cfg.qiniu_region = self.region_edit.text().strip()
        self.cfg.scan_interval_minutes = int(self.interval_spin.value())
        self.cfg.force_upload_ignore_lock = bool(self.force_cb.isChecked())
        save_config(default_config_path(), self.cfg)
        self.accept()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("AI图片上传专用程序-001")
        self.resize(900, 600)

        self.config_path = default_config_path()
        self.config: AppConfig = load_config(self.config_path)
        ensure_device_id(self.config)
        save_config(self.config_path, self.config)

        self._log_queue: "queue.SimpleQueue[str]" = queue.SimpleQueue()
        self._state_queue: "queue.SimpleQueue[str]" = queue.SimpleQueue()
        self.engine = SyncEngine(self.enqueue_log, self.enqueue_state)
        self._log_file_path = self._resolve_log_path()
        self._ensure_log_dir()

        # Menu
        settings_action = QAction("首选项...", self)
        settings_action.triggered.connect(self.open_settings)
        menubar = self.menuBar()
        settings_menu = menubar.addMenu("设置")
        settings_menu.addAction(settings_action)

        # Config import/export
        export_action = QAction("导出配置字符串...", self)
        export_action.triggered.connect(self.export_config_string)
        import_action = QAction("导入配置字符串...", self)
        import_action.triggered.connect(self.import_config_string)
        settings_menu.addAction(export_action)
        settings_menu.addAction(import_action)

        central = QWidget(self)
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        # Device ID
        device_layout = QHBoxLayout()
        device_layout.addWidget(QLabel("设备标识符:"))
        self.device_label = QLineEdit(self.config.device_id or "")
        self.device_label.setReadOnly(True)
        device_layout.addWidget(self.device_label)
        layout.addLayout(device_layout)

        # Local dir
        dir_layout = QHBoxLayout()
        dir_layout.addWidget(QLabel("本地同步文件夹:"))
        self.dir_edit = QLineEdit(self.config.local_dir or "")
        dir_btn = QPushButton("选择...")
        dir_btn.clicked.connect(self.choose_dir)
        dir_layout.addWidget(self.dir_edit)
        dir_layout.addWidget(dir_btn)
        layout.addLayout(dir_layout)

        # Remote subdir
        subdir_layout = QHBoxLayout()
        subdir_layout.addWidget(QLabel("子文件夹(可留空):"))
        self.subdir_edit = QLineEdit(self.config.qiniu_subdir or "")
        subdir_layout.addWidget(self.subdir_edit)
        layout.addLayout(subdir_layout)

        # Controls
        controls = QHBoxLayout()
        self.start_btn = QPushButton("开始同步")
        self.stop_btn = QPushButton("停止")
        self.stop_btn.setEnabled(False)
        controls.addWidget(self.start_btn)
        controls.addWidget(self.stop_btn)
        layout.addLayout(controls)

        # Status + Log
        self.status_label = QLabel("状态: 空闲")
        layout.addWidget(self.status_label)

        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        layout.addWidget(self.log_view, 1)

        # Timer for periodic UI heartbeat (optional)
        self.ui_timer = QTimer(self)
        self.ui_timer.setInterval(200)
        self.ui_timer.timeout.connect(self.refresh_ui)
        self.ui_timer.start()

        # Wire events
        self.start_btn.clicked.connect(self.on_start_clicked)
        self.stop_btn.clicked.connect(self.stop_sync)

        self.append_log("应用已启动")

        # Status bar
        sb = self.statusBar()
        self.sb_countdown = QLabel("下次: --s")
        self.sb_state = QLabel("状态: 空闲")
        self.sb_current = QLabel("当前: -")
        self.sb_counts = QLabel("")
        sb.addPermanentWidget(self.sb_state)
        sb.addPermanentWidget(self.sb_current)
        sb.addPermanentWidget(self.sb_counts)
        sb.addPermanentWidget(self.sb_countdown)

        # Countdown state
        self._next_seconds_remaining = 0
        self._last_countdown_tick = time.time()
        self._last_counts = {"u": 0, "d": 0, "del": 0}
        self._is_running = False
        self._start_mode = "start"  # start | refresh

    def _resolve_log_path(self) -> str:
        base = app_data_dir()
        return str(base / "app.log")

    def _ensure_log_dir(self):
        try:
            Path(self._log_file_path).parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

    def enqueue_log(self, text: str):
        try:
            self._log_queue.put(text)
        except Exception:
            pass

    def enqueue_state(self, state: str):
        try:
            self._state_queue.put(state)
        except Exception:
            pass

    def open_settings(self):
        dlg = SettingsDialog(self.config, self)
        if dlg.exec() == QDialog.Accepted:
            # Reflect any displayable settings if needed
            pass

    def export_config_string(self):
        try:
            data = asdict(self.config)
            payload = json.dumps(data, ensure_ascii=False)
            token = encrypt_to_base64(payload.encode("utf-8"))

            dlg = QDialog(self)
            dlg.setWindowTitle("导出配置字符串")
            v = QVBoxLayout(dlg)
            info = QLabel("请妥善保管导出字符串，包含敏感信息。")
            v.addWidget(info)
            editor = QPlainTextEdit(token)
            editor.setReadOnly(True)
            v.addWidget(editor)
            buttons = QDialogButtonBox(QDialogButtonBox.Ok)
            buttons.accepted.connect(dlg.accept)
            v.addWidget(buttons)
            dlg.exec()
        except Exception as e:
            QMessageBox.critical(self, "错误", f"导出失败:\n{e}")

    def import_config_string(self):
        try:
            dlg = QDialog(self)
            dlg.setWindowTitle("导入配置字符串")
            v = QVBoxLayout(dlg)
            tip = QLabel("粘贴导出的加密字符串：")
            v.addWidget(tip)
            editor = QPlainTextEdit()
            v.addWidget(editor)
            buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
            buttons.accepted.connect(dlg.accept)
            buttons.rejected.connect(dlg.reject)
            v.addWidget(buttons)
            if dlg.exec() == QDialog.Accepted:
                token = editor.toPlainText().strip()
                if not token:
                    QMessageBox.warning(self, "提示", "请输入导入字符串")
                    return
                plaintext = decrypt_from_base64(token)
                data = json.loads(plaintext.decode("utf-8"))
                # 合并为 AppConfig 并保存
                self.config = AppConfig(**data)
                ensure_device_id(self.config)
                save_config(self.config_path, self.config)
                # 刷新 UI 可见字段
                self.device_label.setText(self.config.device_id or "")
                self.dir_edit.setText(self.config.local_dir or "")
                self.subdir_edit.setText(getattr(self.config, "qiniu_subdir", None) or "")
                QMessageBox.information(self, "成功", "配置已导入并保存")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"导入失败:\n{e}")

    def choose_dir(self):
        path = QFileDialog.getExistingDirectory(self, "选择本地同步文件夹")
        if path:
            self.dir_edit.setText(path)
            # log path remains in app data dir; do not move it into sync folder
            self._ensure_log_dir()

    def start_sync(self):
        try:
            # Validate and save config
            self.config.local_dir = self.dir_edit.text().strip()
            # normalize subdir: strip slashes and blanks
            subdir = (self.subdir_edit.text() or "").strip().strip("/\\")
            self.config.qiniu_subdir = subdir or None

            ensure_device_id(self.config)
            save_config(self.config_path, self.config)
            self.device_label.setText(self.config.device_id or "")

            if not self.config.local_dir:
                QMessageBox.warning(self, "提示", "请先选择本地同步文件夹")
                return

            Path(self.config.local_dir).mkdir(parents=True, exist_ok=True)
            self._ensure_log_dir()

            self.start_btn.setEnabled(True)
            self.stop_btn.setEnabled(True)
            self._is_running = True
            # 启动后即切换为立刻刷新
            self._set_start_mode("refresh")
            self.status_label.setText("状态: 正在同步...")
            self.append_log("启动同步调度器")

            self.engine.start(self.config)
        except Exception as e:
            self.append_log(f"启动同步失败: {e}")
            QMessageBox.critical(self, "错误", f"启动同步失败:\n{e}")

    def stop_sync(self):
        self.engine.stop()
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self._is_running = False
        self._set_start_mode("start")
        self.start_btn.setText("开始同步")
        self.status_label.setText("状态: 已停止")
        self.append_log("已停止同步")

    def on_start_clicked(self):
        if self._start_mode == "start":
            self.start_sync()
        else:
            # refresh now (保持按钮可点击状态)
            try:
                self.engine.kick()
            except Exception as e:
                QMessageBox.critical(self, "错误", f"无法立刻刷新:\n{e}")

    def _set_start_mode(self, mode: str):
        self._start_mode = mode
        if mode == "start":
            self.start_btn.setText("开始同步")
        elif mode == "refresh":
            self.start_btn.setText("立刻刷新")

    def append_log(self, text: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {text}"
        self.log_view.appendPlainText(line)
        try:
            with open(self._log_file_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    def on_sync_state_change(self, state: str):
        try:
            if state.startswith("COUNTS|"):
                # format: COUNTS|u=..|d=..|del=..
                parts = dict(p.split("=", 1) for p in state.split("|")[1:] if "=" in p)
                u = parts.get("u", "0")
                d = parts.get("d", "0")
                de = parts.get("del", "0")
                self.sb_counts.setText(f"统计 U:{u} D:{d} DEL:{de}")
                try:
                    self._last_counts = {"u": int(u), "d": int(d), "del": int(de)}
                except Exception:
                    self._last_counts = {"u": 0, "d": 0, "del": 0}
                return
            if state.startswith("CURRENT|"):
                rel = state.split("|", 1)[1] if "|" in state else ""
                self.sb_current.setText(f"当前: {rel or '-'}")
                # 有当前文件操作时，禁用“立刻刷新”；结束后恢复
                if self._is_running:
                    if rel:
                        self.start_btn.setEnabled(False)
                    else:
                        self._set_start_mode("refresh")
                        self.start_btn.setEnabled(True)
                return
            if state.startswith("NEXT|"):
                sec_s = state.split("|", 1)[1] if "|" in state else "0"
                try:
                    self._next_seconds_remaining = max(0, int(sec_s))
                except Exception:
                    self._next_seconds_remaining = 0
                self._last_countdown_tick = time.time()
                self.sb_countdown.setText(f"下次: {self._next_seconds_remaining}s")
                return
            if state == "NO_DIFF":
                # 进入等待期且无差异：切换为“立刻刷新”并可点击
                if self._is_running:
                    self._set_start_mode("refresh")
                    self.start_btn.setEnabled(True)
                return
        except Exception:
            pass

        # default status update
        self.status_label.setText(f"状态: {state}")
        self.sb_state.setText(f"状态: {state}")
        if state in ("扫描与同步中",):
            # 扫描期间也显示且允许点击“立刻刷新”
            if self._is_running:
                self._set_start_mode("refresh")
                self.start_btn.setEnabled(True)
        if state == "空闲" and self._is_running:
            # 等待期间总是显示且允许“立刻刷新”
            self._set_start_mode("refresh")
            self.start_btn.setEnabled(True)
        if state == "错误" and self._is_running:
            # 出错时也允许用户触发立刻刷新以重试
            self._set_start_mode("refresh")
            self.start_btn.setEnabled(True)
        self.append_log(f"状态变更: {state}")

    def refresh_ui(self):
        # Drain state queue
        drained_state = None
        try:
            while True:
                drained_state = self._state_queue.get_nowait()
        except Exception:
            pass
        if drained_state is not None:
            self.on_sync_state_change(drained_state)
        # Countdown tick (1s resolution)
        try:
            now = time.time()
            if self._next_seconds_remaining > 0 and now - self._last_countdown_tick >= 1.0:
                dec = int(now - self._last_countdown_tick)
                self._next_seconds_remaining = max(0, self._next_seconds_remaining - dec)
                self._last_countdown_tick = now
                self.sb_countdown.setText(f"下次: {self._next_seconds_remaining}s")
        except Exception:
            pass
        # Drain logs
        try:
            while True:
                msg = self._log_queue.get_nowait()
                self.append_log(msg)
        except Exception:
            pass


def _global_excepthook(exctype, value, tb):
    try:
        base = app_data_dir()
        log_path = str(base / "app.log")
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        import traceback
        with open(log_path, "a", encoding="utf-8") as f:
            f.write("\n=== 未捕获异常 ===\n")
            traceback.print_exception(exctype, value, tb, file=f)
    except Exception:
        pass
    # Also print to stderr
    import traceback
    traceback.print_exception(exctype, value, tb)


def main():
    sys.excepthook = _global_excepthook
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main() 