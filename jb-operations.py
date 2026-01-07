from __future__ import annotations

import argparse
import datetime as dt
import re
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple, List

import pandas as pd
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import PatternFill, Alignment, Font
from openpyxl.worksheet.table import Table, TableStyleInfo

LOC_CODE_MAP = {
    # 文字列でも変換可能にするための対応表
    "三鷹": "JB01",
    "Mitaka": "JB01",
    "中野": "JB07",
    "Nakano": "JB07",
    "御茶ノ水": "JB18",
    "Ochanomizu": "JB18",
    "千葉": "JB39",
    "Chiba": "JB39",
}


# -------I/O helpers----------
def _require_columns(df: pd.DataFrame, required: List[str], sheet: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"[{sheet}] 必須列が見つかりません: {missing}\n"
            f"現在の列: {list(df.columns)}"
        )


def to_station_code(x):
    # 文字列駅をナンバリングに変換
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return s
    if re.fullmatch(r"JB\d{2}", s):
        return s
    return LOC_CODE_MAP.get(s, s)


# 塗りつぶし色
FILL_MITAKA = PatternFill("solid", fgColor="1F4E79")  # 紺
FILL_NAKANO = PatternFill("solid", fgColor="9DC3E6")  # 水色
FILL_OCHA = PatternFill("solid", fgColor="F4B084")  # オレンジ
FILL_CHIBA = PatternFill("solid", fgColor="8064A2")  # 紫
FILL_OTHER = PatternFill("solid", fgColor="FFF2CC")  # 黄色


def station_fill(value):
    # セル値から背景色を決める（4駅=指定色、それ以外=黄色）
    if value is None:
        return None
    code = to_station_code(value)
    if code == "JB01":
        return FILL_MITAKA
    elif code == "JB07":
        return FILL_NAKANO
    elif code == "JB18":
        return FILL_OCHA
    elif code == "JB39":
        return FILL_CHIBA
    else:
        return FILL_OTHER


def read_master(master_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    ops = pd.read_excel(master_path, sheet_name="operations")
    forms = pd.read_excel(master_path, sheet_name="formations")

    # 想定する最低限の列
    _require_columns(
        ops,
        required=[
            "operation_id",
            "required",
            "is_inspection_B",
            "start_loc",
            "end_loc",
        ],
        sheet="operations",
    )  # 運用

    _require_columns(
        forms,
        required=[
            "formation_id",
            "init_location",
            "init_days_since_inspectionA",
            "init_days_since_inspectionB",
        ],
        sheet="formations",
    )  # 編成

    # IDは文字列に寄せる（Excel側で数字扱いでも安定）
    ops["start_loc"] = ops["start_loc"].map(to_station_code)
    ops["end_loc"] = ops["end_loc"].map(to_station_code)
    forms["init_location"] = forms["init_location"].map(to_station_code)

    return ops, forms


# ---------- Core logic (baseline allocator) ----------


@dataclass
class FormationState:
    loc: Any
    daysA: int
    daysB: int


def make_baseline_schedule(
    ops: pd.DataFrame,
    forms: pd.DataFrame,
    days: int = 30,
    start_date: Optional[str] = None,
    default_idle_op: str = "IDOL_Mitaka",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    制約なしの割当：
      - required=1 の運用を毎日埋める
      - start_loc にいる編成がいれば優先して割当
      - いなければ適当に割当し deadhead=1 を立てる（回送で辻褄合わせた扱い）
      - 余り編成は現在位置に対応する IDOL_* を割当（なければ default_idle_op）
    """

    required_ops = ops[ops["required"] == 1].copy().reset_index(drop=True)

    # 予備待機（required=0 かつ 検査Bじゃないもの）から start_loc→operation_id を作る
    idle_ops = ops[(ops["required"] == 0) & (ops["is_inspection_B"] == 0)].copy()
    idle_by_start: Dict[Any, str] = dict(
        zip(idle_ops["start_loc"], idle_ops["operation_id"])
    )

    # operation_id→行の引き当て用（終点などを取る）
    op_by_id = {row.operation_id: row for row in ops.itertuples(index=False)}

    # 編成状態
    state: Dict[str, FormationState] = {}
    for r in forms.itertuples(index=False):
        state[str(r.formation_id)] = FormationState(
            loc=r.init_location,
            daysA=int(r.init_days_since_inspectionA),
            daysB=int(r.init_days_since_inspectionB),
        )

    formation_ids = list(forms["formation_id"].astype(str))

    # 日ラベル
    if start_date:
        start = dt.date.fromisoformat(start_date)
        day_labels = [start + dt.timedelta(days=i) for i in range(days)]
    else:
        day_labels = [f"D{d:02d}" for d in range(1, days + 1)]

    rows: List[Dict[str, Any]] = []

    for di in range(days):
        day = day_labels[di]
        available = formation_ids.copy()

        # required運用を順に割当
        for op in required_ops.itertuples(index=False):
            op_id = str(op.operation_id)
            start_loc = op.start_loc

            # start_loc にいる編成を探す
            pick = None
            for fid in available:
                if state[fid].loc == start_loc:
                    pick = fid
                    break

            deadhead = False
            if pick is None:
                # いなければ適当（制約なしなので許容）
                pick = available[0]
                deadhead = True

            available.remove(pick)

            rows.append(
                dict(
                    day=day,
                    formation_id=pick,
                    operation_id=op_id,
                    status="RUN",
                    op_start_loc=op.start_loc,
                    op_end_loc=op.end_loc,
                    deadhead=int(deadhead),
                    daysA_before=state[pick].daysA,
                    daysB_before=state[pick].daysB,
                )
            )

            # 終点へ移動
            state[pick].loc = op.end_loc

        # 余り編成は待機運用へ
        for fid in available:
            loc = state[fid].loc
            idle_op = idle_by_start.get(loc, default_idle_op)
            idle_row = op_by_id.get(idle_op, None)

            rows.append(
                dict(
                    day=day,
                    formation_id=fid,
                    operation_id=idle_op,
                    status="IDLE",
                    op_start_loc=(idle_row.start_loc if idle_row else loc),
                    op_end_loc=(idle_row.end_loc if idle_row else loc),
                    deadhead=0,
                    daysA_before=state[fid].daysA,
                    daysB_before=state[fid].daysB,
                )
            )

            # 待機運用にも end_loc があれば反映
            if idle_row:
                state[fid].loc = idle_row.end_loc

        # 日数カウンタ更新（制約なしでも出力に入れておくと後で便利）
        for fid in formation_ids:
            state[fid].daysA += 1
            state[fid].daysB += 1

    schedule = (
        pd.DataFrame(rows).sort_values(["day", "formation_id"]).reset_index(drop=True)
    )

    # ガント風：編成×日 の行列
    gantt_ops = schedule.pivot(
        index="formation_id", columns="day", values="operation_id"
    )

    # 位置ガント：start/end を別pivotで持つ（Excel側で1日2列にする）
    gantt_start = schedule.pivot(
        index="formation_id", columns="day", values="op_start_loc"
    )
    gantt_end = schedule.pivot(index="formation_id", columns="day", values="op_end_loc")

    return (
        schedule,
        gantt_ops.reset_index(),
        gantt_start.reset_index(),
        gantt_end.reset_index(),
    )


# ---------- Excel export ----------


def add_sheet_from_df(
    wb: openpyxl.Workbook,
    name: str,
    df: pd.DataFrame,
    table_name: Optional[str] = None,
    freeze: str = "A2",
    station_cols: Optional[List[str]] = None,
) -> None:
    ws = wb.create_sheet(name)

    for r in dataframe_to_rows(df, index=False, header=True):
        ws.append(r)

    # header style
    header_fill = PatternFill("solid", fgColor="1F4E79")
    header_font = Font(color="FFFFFF", bold=True)
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")

    # col widths (rough)
    for col in ws.columns:
        max_len = 0
        col_letter = col[0].column_letter
        for c in col[:200]:
            v = "" if c.value is None else str(c.value)
            max_len = max(max_len, len(v))
        ws.column_dimensions[col_letter].width = min(max(10, max_len + 2), 40)

    ws.freeze_panes = freeze
    ws.auto_filter.ref = ws.dimensions

    if table_name:
        tab = Table(displayName=table_name, ref=ws.dimensions)
        style = TableStyleInfo(
            name="TableStyleMedium9",
            showFirstColumn=False,
            showLastColumn=False,
            showRowStripes=True,
            showColumnStripes=False,
        )
        tab.tableStyleInfo = style
        ws.add_table(tab)

    # 駅カラムに色付け（指定された列だけ）
    if station_cols:
        header = [c.value for c in ws[1]]
        col_idx = {str(v): i + 1 for i, v in enumerate(header) if v is not None}
        for col_name in station_cols:
            if col_name not in col_idx:
                continue
            j = col_idx[col_name]
            for r in range(2, ws.max_row + 1):
                cell = ws.cell(r, j)
                fill = station_fill(cell.value)
                if fill:
                    cell.value = to_station_code(cell.value)
                    cell.fill = fill


def add_gantt_loc_sheet(
    wb: openpyxl.Workbook,
    name: str,
    gantt_start: pd.DataFrame,
    gantt_end: pd.DataFrame,
) -> None:
    ws = wb.create_sheet(name)
    days = list(gantt_start.columns)

    # Row1は2列分書き込む。D01→None→D02→Noneの順
    ws.cell(1, 1, "formation_id")
    c = 2
    for d in days:
        ws.cell(1, c, d)
        ws.cell(1, c + 1, None)
        c += 2

    # Row2はstartとendを交互に書く
    ws.cell(2, 1, None)
    c = 2
    for _ in days:
        ws.cell(2, c, "start")
        ws.cell(2, c + 1, "end")
        c += 2

    # Header style（2段とも）
    header_fill = PatternFill("solid", fgColor="1F4E79")
    header_font = Font(color="FFFFFF", bold=True)
    for r in (1, 2):
        for cell in ws[r]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")

    # Data rows
    start_df = gantt_start.copy()
    end_df = gantt_end.copy()
    start_df.index = start_df.index.astype(str)
    end_df.index = end_df.index.astype(str)

    out_r = 3  # 3行目から
    for fid in start_df.index:
        ws.cell(out_r, 1, fid)
        c = 2
        for d in days:
            sv = to_station_code(start_df.loc[fid, d])
            ev = to_station_code(end_df.loc[fid, d])
            ws.cell(out_r, c, sv)
            ws.cell(out_r, c + 1, ev)

            # 色付け（start/endとも）
            for cc, v in [(c, sv), (c + 1, ev)]:
                fill = station_fill(v)
                if fill:
                    ws.cell(out_r, cc).fill = fill
            c += 2
        out_r += 1

    # 幅調整とfreeze
    ws.freeze_panes = "B3"  # ウィンドウ枠の固定
    ws.column_dimensions["A"].width = 14
    for col in range(2, ws.max_column + 1):
        ws.column_dimensions[openpyxl.utils.get_column_letter(col)].width = 8


# Excel出力
def export_baseline_excel(
    out_path: str,
    schedule: pd.DataFrame,
    gantt_ops: pd.DataFrame,
    gantt_start: pd.DataFrame,
    gantt_end: pd.DataFrame,
    ops: pd.DataFrame,
    forms: pd.DataFrame,
) -> None:
    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    add_sheet_from_df(
        wb,
        "schedule_long",
        schedule,
        table_name="ScheduleLong",
        freeze="A2",
        station_cols=["op_start_loc", "op_end_loc"],
    )
    add_sheet_from_df(wb, "gantt_ops", gantt_ops, table_name="GanttOps", freeze="B2")

    add_gantt_loc_sheet(wb, "gantt_loc", gantt_start, gantt_end)
    add_sheet_from_df(
        wb,
        "master_operations",
        ops,
        table_name="MasterOps",
        freeze="A2",
        station_cols=["start_loc", "end_loc"],
    )
    add_sheet_from_df(
        wb,
        "master_formations",
        forms,
        table_name="MasterForms",
        freeze="A2",
        station_cols=["init_location"],
    )

    wb.save(out_path)


# ---------- CLI ----------


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--master", default="data/master_data.xlsx", help="master_data.xlsx のパス"
    )
    ap.add_argument(
        "--out", default="outputs/baseline_output.xlsx", help="出力Excelパス"
    )
    ap.add_argument("--days", type=int, default=30, help="何日分作るか")
    ap.add_argument(
        "--start-date",
        default=None,
        help="YYYY-MM-DD を指定すると日付ラベルになる（未指定なら D01..）",
    )
    ap.add_argument(
        "--default-idle-op",
        default="IDOL_Mitaka",
        help="待機運用が引けない時のデフォルト operation_id",
    )

    args = ap.parse_args()

    ops, forms = read_master(args.master)
    schedule, gantt_ops, gantt_start, gantt_end = make_baseline_schedule(
        ops,
        forms,
        days=args.days,
        start_date=args.start_date,
        default_idle_op=args.default_idle_op,
    )
    export_baseline_excel(args.out, schedule, gantt_ops, gantt_start, gantt_end, ops, forms)
    print(f"OK: {args.out}")
    print(f"schedule_long: {schedule.shape[0]} rows, {schedule.shape[1]} cols")


if __name__ == "__main__":
    main()
