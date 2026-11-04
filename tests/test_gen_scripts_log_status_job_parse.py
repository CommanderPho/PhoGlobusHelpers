"""Unit tests for per-log Slurm job ID / status / Time Used parsing."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from phoglobushelpers.gen_scripts_log_status import (
    _format_duration_hms,
    _match_keys_from_slurm_header,
    _slurm_display_name_from_name,
    _slurm_job_id_from_name,
    build_slurm_out_lookup,
    lookup_slurm_info_for_log,
    parse_debug_log_job_info,
)

_DETROIT = ZoneInfo("America/Detroit")


_CLEAN_LOG_STEM = (
    "debug_2026-11-02_11-11-22.gl3223.arc-ts.umich.edu.kdiba.gor01.one."
    "2006-6-08_14-26-15._withNormalComputedReplays-qclu_1246789-frateThresh_2.0_tbin_25ms_Clean"
)


def test_slurm_job_id_from_name():
    name = "slurm_gor01_2006-6-08_14-26-15_q1246789_fr_2_tbin_25ms_Clean-65233372.out"
    assert _slurm_job_id_from_name(name) == "65233372"
    assert _slurm_job_id_from_name("not_a_slurm.log") is None


def test_slurm_display_name_from_name():
    name = "slurm_gor01_2006-6-08_14-26-15_q1246789_fr_2_tbin_25ms_Clean-65233372.out"
    assert (
        _slurm_display_name_from_name(name)
        == "gor01_2006-6-08_14-26-15_q1246789_fr_2_tbin_25ms_Clean"
    )


def test_format_duration_hms():
    assert _format_duration_hms(4214) == "01:10:14"
    assert _format_duration_hms(0) == "00:00:00"
    assert _format_duration_hms(None) is None


def test_match_keys_from_slurm_header():
    header = (
        'build_logger(full_logger_string="2026-11-02_11-11-22.gl3223.arc-ts.umich.edu.'
        "kdiba.gor01.one.2006-6-08_14-26-15._withNormalComputedReplays-qclu_1246789-"
        'frateThresh_2.0_tbin_25ms_Clean", file_logging_dir: /tmp):\n'
        f'will log to "/gpfs/x/{_CLEAN_LOG_STEM}.log"\n'
        f"final_log_file_path: /gpfs/x/{_CLEAN_LOG_STEM}.log\n"
    )
    keys = _match_keys_from_slurm_header(header)
    assert _CLEAN_LOG_STEM in keys


def test_parse_completed_duration_from_log_timestamps():
    log = (
        "========================== runBatch STARTING ==========================\n"
        "[TIMING START] batch_load_session @ 2026-11-02T11:50:22\n"
        "<<< on_complete_success_execution_session(...): COMPLETED ======\n"
        "finished run: SessionBatchProgress.COMPLETED\n"
        "errors: None\n"
        "2026-11-02 12:58:11,785 logger]  [INFO ]\n"
        " ALL DONE!\n"
    )
    # out_mtime at/before log end (UTC) should not inflate duration.
    log_end_utc = datetime(2026, 11, 2, 12, 58, 11, tzinfo=_DETROIT).astimezone(timezone.utc)
    info = parse_debug_log_job_info(
        log,
        log_name=f"{_CLEAN_LOG_STEM}.log",
        slurm_info={
            "job_id": "65233372",
            "job_name": "gor01_2006-6-08_14-26-15_q1246789_fr_2_tbin_25ms_Clean",
            "out_mtime": log_end_utc,
        },
    )
    assert info["job_status"] == "Completed"
    assert info["job_id"] == "65233372"
    # 11:50:22 → 12:58:11 local = 01:07:49
    assert info["time_used"] == "01:07:49"


def test_hybrid_end_uses_later_out_mtime():
    log = (
        "[TIMING START] batch_load_session @ 2026-11-02T11:50:22\n"
        "finished run: SessionBatchProgress.COMPLETED\n"
        "errors: None\n"
        "2026-11-02 12:58:11,785 logger]  [INFO ]\n"
        " ALL DONE!\n"
    )
    # ~2.5 min after log end in Detroit local → closer to OOD 01:10:14
    out_mtime = datetime(2026, 11, 2, 13, 0, 36, tzinfo=_DETROIT).astimezone(timezone.utc)
    info = parse_debug_log_job_info(
        log,
        log_name=f"{_CLEAN_LOG_STEM}.log",
        slurm_info={"job_id": "65233372", "job_name": "Clean", "out_mtime": out_mtime},
    )
    assert info["job_status"] == "Completed"
    assert info["time_used"] == "01:10:14"


def test_failed_on_traceback():
    log = (
        "[TIMING START] batch_load_session @ 2026-11-02T11:50:22\n"
        "Traceback (most recent call last):\n"
        "  File \"x.py\", line 1, in <module>\n"
        "ValueError: boom\n"
    )
    info = parse_debug_log_job_info(log, log_name="debug_fail.log")
    assert info["job_status"] == "Failed"


def test_completed_wins_over_midrun_traceback():
    log = (
        "[TIMING START] batch_load_session @ 2026-11-02T16:54:13\n"
        "Traceback (most recent call last):\n"
        "  File \"x.py\", line 1, in <module>\n"
        "RuntimeError: optional figure failed\n"
        "finished run: SessionBatchProgress.COMPLETED\n"
        "errors: None\n"
        "2026-11-02 17:00:23,160 logger]  [INFO ]\n"
        " ALL DONE!\n"
    )
    info = parse_debug_log_job_info(log, log_name="debug_Figure.log")
    assert info["job_status"] == "Completed"
    assert info["time_used"] == "00:06:10"


def test_running_without_completion():
    now = datetime(2026, 11, 2, 12, 0, 0, tzinfo=_DETROIT).astimezone(timezone.utc)
    log = "[TIMING START] batch_load_session @ 2026-11-02T11:50:22\nworking...\n"
    log_mtime = datetime(2026, 11, 2, 11, 55, 0, tzinfo=_DETROIT).astimezone(timezone.utc)
    info = parse_debug_log_job_info(
        log, log_name="debug_run.log", log_mtime=log_mtime, now=now
    )
    assert info["job_status"] == "Running"
    assert info["time_used"] == "00:09:38"


def test_build_slurm_out_lookup_and_match(tmp_path: Path):
    parent = tmp_path / "run_kdiba_gor01_one_2006-6-08_14-26-15"
    parent.mkdir()
    out_name = (
        "slurm_gor01_2006-6-08_14-26-15_q1246789_fr_2_tbin_25ms_Clean-65233372.out"
    )
    out_path = parent / out_name
    out_path.write_text(
        'build_logger(full_logger_string="'
        + _CLEAN_LOG_STEM.removeprefix("debug_")
        + '", file_logging_dir: /tmp):\n'
        f'will log to "{parent.as_posix()}/{_CLEAN_LOG_STEM}.log"\n'
        f"final_log_file_path: {parent.as_posix()}/{_CLEAN_LOG_STEM}.log\n",
        encoding="utf-8",
    )
    file_df = pd.DataFrame(
        [
            {
                "type": "file",
                "parent_path": str(parent),
                "name": out_name,
                "last_modified": datetime(2026, 11, 2, 13, 0, 36, tzinfo=timezone.utc),
                "size": 1999,
            }
        ]
    )
    lookup = build_slurm_out_lookup(file_df)
    info = lookup_slurm_info_for_log(lookup, str(parent), f"{_CLEAN_LOG_STEM}.log")
    assert info is not None
    assert info["job_id"] == "65233372"
    assert info["job_name"].endswith("_Clean")
