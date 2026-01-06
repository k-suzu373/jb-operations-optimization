from __future__ import annotations

import argparse
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple, List

import pandas as pd
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import PatternFill, Alignment, Font
from openpyxl.worksheet.table import Table, TableStyleInfo


# -------I/O helpers----------
def _require_columns(df: pd.DataFrame, required: List[str], sheet: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"[{sheet}] 必須列が見つかりません: {missing}\n"
            f"現在の列: {list(df.columns)}"
        )


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
    ops["operation_id"] = ops["operation_id"].astype(str)
    forms["formation_id"] = forms["formation_id"].astype(str)

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
    gantt_endloc = schedule.pivot(
        index="formation_id", columns="day", values="op_end_loc"
    )

    return schedule, gantt_ops.reset_index(), gantt_endloc.reset_index()


# ---------- Excel export ----------


def add_sheet_from_df(
    wb: openpyxl.Workbook,
    name: str,
    df: pd.DataFrame,
    table_name: Optional[str] = None,
    freeze: str = "A2",
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


def export_baseline_excel(
    out_path: str,
    schedule: pd.DataFrame,
    gantt_ops: pd.DataFrame,
    gantt_endloc: pd.DataFrame,
    ops: pd.DataFrame,
    forms: pd.DataFrame,
) -> None:
    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    add_sheet_from_df(
        wb, "schedule_long", schedule, table_name="ScheduleLong", freeze="A2"
    )
    add_sheet_from_df(wb, "gantt_ops", gantt_ops, table_name="GanttOps", freeze="B2")
    add_sheet_from_df(
        wb, "gantt_endloc", gantt_endloc, table_name="GanttEndLoc", freeze="B2"
    )

    add_sheet_from_df(wb, "master_operations", ops, table_name="MasterOps", freeze="A2")
    add_sheet_from_df(
        wb, "master_formations", forms, table_name="MasterForms", freeze="A2"
    )

    ws = wb.create_sheet("README")
    ws["A1"] = "baseline_output.xlsx について"
    ws["A1"].font = Font(bold=True, size=14)
    lines = [
        "これは『制約・最適化なし』で、master_data.xlsx から 30日分の割当表を出すためのベースライン出力です。",
        "",
        "sheet: schedule_long",
        "  - 1行=1編成×1日。RUN=運用、IDLE=予備待機。",
        "  - deadhead=1 の場合、開始位置が合わず『回送で辻褄合わせた』扱い（今回は制約をかけないため許容）。",
        "",
        "sheet: gantt_ops",
        "  - 編成×日 の行列（ガント風）で operation_id だけを並べています。",
        "",
        "sheet: gantt_endloc",
        "  - 編成×日 の行列で、その日の終了位置 (op_end_loc) を並べています。",
        "",
        "次のステップでは、このベースラインに対して制約（位置整合・固定翌日運用・検査A/B等）を順に追加します。",
    ]
    for i, t in enumerate(lines, start=3):
        ws[f"A{i}"] = t
        ws[f"A{i}"].alignment = Alignment(wrap_text=True)
    ws.column_dimensions["A"].width = 110

    wb.save(out_path)


# ---------- CLI ----------


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="data/master_data.xlsx", help="master_data.xlsx のパス")
    ap.add_argument("--out", default="outputs/baseline_output.xlsx", help="出力Excelパス")
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
    schedule, gantt_ops, gantt_endloc = make_baseline_schedule(
        ops,
        forms,
        days=args.days,
        start_date=args.start_date,
        default_idle_op=args.default_idle_op,
    )
    export_baseline_excel(args.out, schedule, gantt_ops, gantt_endloc, ops, forms)
    print(f"OK: {args.out}")
    print(f"schedule_long: {schedule.shape[0]} rows, {schedule.shape[1]} cols")


if __name__ == "__main__":
    main()
