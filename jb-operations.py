from __future__ import annotations
import argparse
import datetime as dt
import re
import random
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple, List
import pandas as pd
import openpyxl
from ortools.sat.python import cp_model
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import PatternFill, Alignment, Font, Border, Side
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
FILL_MITAKA = PatternFill("solid", fgColor="4F82EA")  # 紺
FILL_NAKANO = PatternFill("solid", fgColor="9DC3E6")  # 水色
FILL_OCHA = PatternFill("solid", fgColor="F4B084")  # オレンジ
FILL_CHIBA = PatternFill("solid", fgColor="da70d6")  # 紫
FILL_OTHER = PatternFill("solid", fgColor="FFF2CC")  # 黄色
FONT_WHITE = Font(color="FFFFFF")


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


def set_white_font(cell, value):
    """三鷹(JB01)だけ文字を白にする"""
    if to_station_code(value) == "JB01":
        cell.font = FONT_WHITE


def read_master(master_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    ops = pd.read_excel(master_path, sheet_name="operations")
    forms = pd.read_excel(master_path, sheet_name="formations")
    rules = pd.read_excel(master_path, sheet_name="rules")

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

    _require_columns(
        rules,
        required=[
            "max_days_since_inspectionA",
            "max_days_since_inspectionB",
        ],
        sheet="rules",
    )

    # IDは文字列に寄せる（Excel側で数字扱いでも安定）
    ops["start_loc"] = ops["start_loc"].map(to_station_code)
    ops["end_loc"] = ops["end_loc"].map(to_station_code)
    forms["init_location"] = forms["init_location"].map(to_station_code)

    rules_row = rules.iloc[0].to_dict()
    return (
        ops,
        forms,
        {
            "max_days_since_inspectionA": int(rules_row["max_days_since_inspectionA"]),
            "max_days_since_inspectionB": int(rules_row["max_days_since_inspectionB"]),
        },
    )


# ---------- Core logic (baseline allocator) ----------


@dataclass
class FormationState:
    loc: Any
    daysA: int
    daysB: int
    prev_op_id: Optional[str] = None  # 前日に担当した運用ID


NON_DEADHEAD_LOC_JUMPS = {
    ("JB33", "JB30"),
    ("JB33", "JB35"),
}  # 回送扱いしない駅の組み合わせ

NON_DEADHEAD_OP_CHAINS = {
    ("53B", "55B"),
}  # 回送扱いしない運用の組み合わせ

FIXED_NEXT_OP = {
    "47B": "49B",
    "53B": "55B",
    "09B": "11B",
    "81B": "83B",
}  # 翌日固定になる運用の組み合わせ


def make_baseline_schedule(
    ops: pd.DataFrame,
    forms: pd.DataFrame,
    max_days_since_inspectionA: int,
    max_days_since_inspectionB: int,
    days: int = 30,
    start_date: Optional[str] = None,
    default_idle_op: str = "IDOL_Mitaka",
    seed: int = 30,
    tsudanuma_code: str = "JB33",
    priority_start_codes: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    制約なしの割当：
      - required=1 の運用を毎日埋める
      - start_loc にいる編成がいれば優先して割当
      - いなければ適当に割当し deadhead=1 を立てる（回送で辻褄合わせた扱い）
      - 余り編成は現在位置に対応する IDOL_* を割当（なければ default_idle_op）
    """

    ops = ops.copy()
    ops["operation_id"] = ops["operation_id"].astype(str)
    required_ops = ops[ops["required"] == 1].copy().reset_index(drop=True)
    required_ops["operation_id"] = required_ops["operation_id"].astype(str)

    if priority_start_codes is None:
        priority_start_codes = ["JB01", "JB07", "JB18", "JB39"]  # 津田沼以外の優先駅

    inspection_a_start_codes = {"JB01", "JB07", "JB33"}

    # 予備待機（required=0 かつ 検査Bじゃないもの）から start_loc→operation_id を作る
    idle_ops = ops[(ops["required"] == 0) & (ops["is_inspection_B"] == 0)].copy()
    idle_by_start: Dict[Any, str] = dict(
        zip(idle_ops["start_loc"], idle_ops["operation_id"])
    )

    # operation_id→行の引き当て用（終点などを取る）
    op_by_id = {str(row.operation_id): row for row in ops.itertuples(index=False)}

    # 編成状態
    state: Dict[str, FormationState] = {}
    for r in forms.itertuples(index=False):
        state[str(r.formation_id)] = FormationState(
            loc=r.init_location,
            daysA=int(r.init_days_since_inspectionA),
            daysB=int(r.init_days_since_inspectionB),
            prev_op_id=None,
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
        available = formation_ids.copy()  # 使える編成
        rng = random.Random(seed + di)  # 日ごとに乱数固定
        assigned_inspection_b_ops: set[str] = set()

        # required運用の未割当リスト（その日ごとに消し込む）
        req_today = required_ops.copy()

        def record_assignment(
            formation_id: str,
            op_id: str,
            op_start_loc: Any,
            op_end_loc: Any,
            is_inspection_B: int,
            status: str,
            remove_required: bool,
        ) -> None:
            """1編成に1運用を割当して、rows追加・state更新・availableから除外・req_todayから除外"""
            nonlocal req_today  # 関数外の変数を更新するためnonlocalをつける
            prev_loc = to_station_code(state[formation_id].loc)
            start_loc = to_station_code(op_start_loc)
            prev_op = state[formation_id].prev_op_id

            # 基本：前日の終点 != 当日の始点 なら回送
            deadhead = int(prev_loc != start_loc)

            # 例外1：JB33終 → 翌日JB30/JB35始 は回送扱いしない
            if deadhead and (prev_loc, start_loc) in NON_DEADHEAD_LOC_JUMPS:
                deadhead = 0

            # 例外2：前日53B → 翌日55B は回送扱いしない（位置が違っていても）
            if deadhead and (str(prev_op), op_id) in NON_DEADHEAD_OP_CHAINS:
                deadhead = 0

            daysA_before = state[formation_id].daysA
            daysB_before = state[formation_id].daysB
            overdueA = int(daysA_before >= max_days_since_inspectionA + 1)
            overdueB = int(daysB_before >= max_days_since_inspectionB + 1)
            did_inspection_A = int(
                daysA_before in (6, 7) and start_loc in inspection_a_start_codes
            )
            did_inspection_B = int(daysB_before >= 20 and int(is_inspection_B) == 1)

            rows.append(
                dict(
                    day=day,
                    formation_id=formation_id,
                    operation_id=op_id,
                    status=status,
                    op_start_loc=op_start_loc,
                    op_end_loc=op_end_loc,
                    deadhead=deadhead,
                    daysA_before=daysA_before,
                    daysB_before=daysB_before,
                    overdueA=overdueA,
                    overdueB=overdueB,
                    did_inspection_A=did_inspection_A,
                    did_inspection_B=did_inspection_B,
                )
            )

            # 状態更新
            state[formation_id].loc = op_end_loc
            state[formation_id].prev_op_id = op_id
            if did_inspection_A:
                state[formation_id].daysA = 0
            if did_inspection_B:
                state[formation_id].daysB = 0
            if formation_id in available:
                available.remove(formation_id)
            if int(is_inspection_B) == 1:
                if op_id in assigned_inspection_b_ops:
                    raise ValueError(f"[{day}] 検査B運用が重複: {op_id}")
                assigned_inspection_b_ops.add(op_id)
            if remove_required:
                # req_todayからop_idを一件だけ消す
                req_today = req_today[req_today["operation_id"] != op_id].reset_index(
                    drop=True
                )

        def assign_one(formation_id: str, op_row) -> None:
            record_assignment(
                formation_id=formation_id,
                op_id=str(op_row.operation_id),
                op_start_loc=op_row.start_loc,
                op_end_loc=op_row.end_loc,
                is_inspection_B=int(op_row.is_inspection_B),
                status="RUN",
                remove_required=True,
            )

        # 0) 翌日固定運用（47B→49B 等）を最優先で割当
        fixed_targets = {}
        req_ids = set(req_today["operation_id"].astype(str))
        for formation_id in list(available):
            prev = state[formation_id].prev_op_id
            if prev in FIXED_NEXT_OP:
                next_op = FIXED_NEXT_OP[prev]
                if next_op in fixed_targets.values():
                    raise ValueError(f"[{day}] 翌日固定が競合: {next_op}")
                if next_op not in req_ids:
                    raise ValueError(
                        f"[{day}] 翌日固定の対象運用がrequiredに存在しない: {prev}→{next_op}"
                    )
                op_row = op_by_id.get(next_op)
                if op_row is None:
                    raise ValueError(
                        f"[{day}] 翌日固定の対象運用がmaster_dataに存在しない: {next_op}"
                    )
                fixed_targets[formation_id] = op_row

        # 固定割当（順序は軽くランダムでもOK。再現性はseedで担保）
        fixed_items = list(fixed_targets.items())
        rng.shuffle(fixed_items)
        for formation_id, op_row in fixed_items:
            assign_one(formation_id, op_row)

        # 1) 検査Bの割当（CP-SATで期限の近い編成を優先）
        b_ops = ops[ops["is_inspection_B"] == 1].copy()
        if not b_ops.empty:
            b_ops = b_ops[
                ~b_ops["operation_id"].astype(str).isin(assigned_inspection_b_ops)
            ]
        eligible_formations = [
            formation_id
            for formation_id in available
            if state[formation_id].daysB >= 20
        ]
        if not b_ops.empty and eligible_formations:
            model = cp_model.CpModel()
            b_op_ids = list(b_ops["operation_id"].astype(str))
            y: Dict[Tuple[str, str], cp_model.IntVar] = {}
            for formation_id in eligible_formations:
                for op_id in b_op_ids:
                    y[(formation_id, op_id)] = model.NewBoolVar(
                        f"y_{formation_id}_{op_id}"
                    )
            for formation_id in eligible_formations:
                model.Add(
                    sum(y[(formation_id, op_id)] for op_id in b_op_ids) <= 1
                )
            for op_id in b_op_ids:
                model.Add(
                    sum(y[(formation_id, op_id)] for formation_id in eligible_formations)
                    <= 1
                )
            # daysB_before が大きい編成ほど優先（期限が近い編成を優先割当）
            model.Maximize(
                sum(
                    y[(formation_id, op_id)] * state[formation_id].daysB
                    for formation_id in eligible_formations
                    for op_id in b_op_ids
                )
            )
            solver = cp_model.CpSolver()
            status = solver.Solve(model)
            if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
                assignments = []
                for formation_id in eligible_formations:
                    for op_id in b_op_ids:
                        if solver.Value(y[(formation_id, op_id)]) == 1:
                            assignments.append((formation_id, op_id))
                for formation_id, op_id in assignments:
                    op_row = op_by_id.get(op_id)
                    if op_row is None:
                        raise ValueError(
                            f"[{day}] 検査Bの対象運用がmaster_dataに存在しない: {op_id}"
                        )
                    assign_one(formation_id, op_row)

        # 2) 検査Aの優先割当（daysA_beforeが7→6の順で、指定駅始発へ寄せる）
        def assign_a_priority(target_days: int) -> None:
            candidates = [
                formation_id
                for formation_id in available
                if state[formation_id].daysA == target_days
            ]
            if not candidates:
                return
            rng.shuffle(candidates)
            for start_code in inspection_a_start_codes:
                ops_here = req_today[req_today["start_loc"] == start_code].copy()
                if ops_here.empty:
                    continue
                ops_here_list = list(ops_here.itertuples(index=False))
                rng.shuffle(ops_here_list)
                for op_row in ops_here_list:
                    if not candidates:
                        return
                    loc_matched = [
                        fid for fid in candidates if state[fid].loc == start_code
                    ]
                    pick = loc_matched[0] if loc_matched else candidates[0]
                    candidates.remove(pick)
                    assign_one(pick, op_row)

        assign_a_priority(7)
        assign_a_priority(6)

        # 1) 優先駅（JB01/JB07/JB18/JB39）にいる車両は「その駅始発」の運用へ
        for code in priority_start_codes:
            # その駅にいる編成
            formation_ids_here = [
                formation_id
                for formation_id in available
                if state[formation_id].loc == code
            ]
            if not formation_ids_here:
                continue

            # その駅始発のrequired運用
            ops_here = req_today[req_today["start_loc"] == code].copy()
            if ops_here.empty:
                continue

            # ある分だけ割り当て
            rng.shuffle(formation_ids_here)
            ops_here_list = list(ops_here.itertuples(index=False))
            rng.shuffle(ops_here_list)
            for formation_id, op_row in zip(formation_ids_here, ops_here_list):
                assign_one(formation_id, op_row)

        # 2) 津田沼（tsudanuma_code）にいる車両は「残りrequired」からランダム割当
        formation_ids_tsudanuma = [
            formation_id
            for formation_id in available
            if state[formation_id].loc == tsudanuma_code
        ]
        if formation_ids_tsudanuma and len(req_today) > 0:
            rng.shuffle(formation_ids_tsudanuma)
            remaining_ops = list(req_today.itertuples(index=False))
            rng.shuffle(remaining_ops)
            for formation_id, op_row in zip(formation_ids_tsudanuma, remaining_ops):
                assign_one(formation_id, op_row)

        # 3) 残りの required は従来通り：位置一致優先、ダメなら deadhead 許容
        remaining_req = list(req_today.itertuples(index=False))
        rng.shuffle(remaining_req)
        for op_row in remaining_req:
            candidates = [fid for fid in available if state[fid].loc == op_row.start_loc]
            pick = rng.choice(candidates) if candidates else rng.choice(available)
            assign_one(pick, op_row)

        # 余り編成は待機運用へ
        for formation_id in list(available):
            loc = state[formation_id].loc
            idle_op = idle_by_start.get(loc, default_idle_op)
            idle_row = op_by_id.get(idle_op, None)

            record_assignment(
                formation_id=formation_id,
                op_id=str(idle_op),
                op_start_loc=(idle_row.start_loc if idle_row else loc),
                op_end_loc=(idle_row.end_loc if idle_row else loc),
                is_inspection_B=int(idle_row.is_inspection_B) if idle_row else 0,
                status="IDLE",
                remove_required=False,
            )

        # 日数カウンタ更新（検査成立なら0に戻し、その後に日末で+1）
        for formation_id in formation_ids:
            state[formation_id].daysA += 1
            state[formation_id].daysB += 1

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
        gantt_start,
        gantt_end,
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
    # フィルターだけ付ける
    ws.auto_filter.ref = ws.dimensions

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
                    set_white_font(cell, cell.value)


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
    for formation_id in start_df.index:
        ws.cell(out_r, 1, formation_id)
        c = 2
        for d in days:
            sv = to_station_code(start_df.loc[formation_id, d])
            ev = to_station_code(end_df.loc[formation_id, d])
            ws.cell(out_r, c, sv)
            ws.cell(out_r, c + 1, ev)

            # 色付け（start/endとも）
            for cc, v in [(c, sv), (c + 1, ev)]:
                fill = station_fill(v)
                if fill:
                    cell = ws.cell(out_r, cc)
                    cell.fill = fill
                    set_white_font(cell, v)
            c += 2
        out_r += 1

    # 幅調整とfreeze
    ws.freeze_panes = "B3"  # ウィンドウ枠の固定
    ws.column_dimensions["A"].width = 14
    for col in range(2, ws.max_column + 1):
        ws.column_dimensions[get_column_letter(col)].width = 8


def add_formation_triplet_sheet(
    wb: openpyxl.Workbook,
    name: str,
    schedule: pd.DataFrame,
    days: list[str],
    formations: list[str],
) -> None:
    ws = wb.create_sheet(name)

    # Row1: 編成ヘッダ（3列結合）
    ws.cell(1, 1, None)
    col = 2
    for formation_id in formations:
        ws.cell(1, col, formation_id)
        ws.merge_cells(start_row=1, start_column=col, end_row=1, end_column=col + 2)
        col += 3

    # Row2: start/end/operation
    ws.cell(2, 1, None)
    col = 2
    for _ in formations:
        ws.cell(2, col, "start")
        ws.cell(2, col + 1, "end")
        ws.cell(2, col + 2, "operation")
        col += 3

    # ヘッダスタイル（2段）
    header_fill = PatternFill("solid", fgColor="1F4E79")
    header_font = Font(color="FFFFFF", bold=True)
    for r in (1, 2):
        for cell in ws[r]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")

    # 参照しやすいように pivot
    schedule_copy = schedule.copy()
    schedule_copy["day"] = schedule_copy["day"].astype(str)
    schedule_copy["formation_id"] = schedule_copy["formation_id"].astype(str)

    p_start = schedule_copy.pivot(
        index="day", columns="formation_id", values="op_start_loc"
    ).reindex(index=days, columns=formations)
    p_end = schedule_copy.pivot(
        index="day", columns="formation_id", values="op_end_loc"
    ).reindex(index=days, columns=formations)
    p_op = schedule_copy.pivot(
        index="day", columns="formation_id", values="operation_id"
    ).reindex(index=days, columns=formations)
    p_dead = schedule_copy.pivot(
        index="day", columns="formation_id", values="deadhead"
    ).reindex(index=days, columns=formations)

    # Data rows
    out_r = 3  # 3行目から
    for day in days:
        ws.cell(out_r, 1, day)
        ws.cell(out_r, 1).alignment = Alignment(horizontal="center", vertical="center")

        col = 2
        for formation_id in formations:
            sv = to_station_code(p_start.loc[day, formation_id])
            ev = to_station_code(p_end.loc[day, formation_id])
            ov = p_op.loc[day, formation_id]
            dv = p_dead.loc[day, formation_id]

            c_start = ws.cell(out_r, col, sv)
            c_end = ws.cell(out_r, col + 1, ev)
            c_op = ws.cell(out_r, col + 2, (None if pd.isna(ov) else str(ov)))

            # start/end は駅色を塗る（既存ルールに合わせる）
            for cell, v in [(c_start, sv), (c_end, ev)]:
                fill = station_fill(v)
                if fill:
                    cell.fill = fill
                cell.alignment = Alignment(horizontal="center", vertical="center")

            # operation は中央寄せ
            c_op.alignment = Alignment(horizontal="center", vertical="center")

            # deadhead=1 のとき：startセルを「赤字＋太字」
            if pd.notna(dv) and int(dv) == 1:
                # 既存フォント設定は引き継いで、色と太字だけ上書き
                c_start.font = c_start.font.copy(color="FF0000", bold=True)

            col += 3
        out_r += 1

    # --- 3列セット（start/end/op）の境界を太線にする（各ブロック最終列の右側） ---
    THICK = Side(style="thick", color="000000")
    row_max = ws.max_row
    start_col = 2
    cols_per = 3
    for i in range(len(formations)):
        boundary_col = start_col + cols_per * (i + 1) - 1
        for r in range(1, row_max + 1):
            cell = ws.cell(r, boundary_col)
            b = cell.border
            cell.border = Border(
                left=b.left,
                right=THICK,
                top=b.top,
                bottom=b.bottom,
                diagonal=b.diagonal,
                diagonal_direction=b.diagonal_direction,
                outline=b.outline,
                vertical=b.vertical,
                horizontal=b.horizontal,
            )

    # --- 行方向（横方向）に細い罫線：各セルの bottom を thin にする ---
    THIN = Side(style="thin", color="000000")
    col_max = ws.max_column
    for r in range(1, row_max + 1):
        for c in range(1, col_max + 1):
            cell = ws.cell(r, c)
            b = cell.border
            cell.border = Border(
                left=b.left,
                right=b.right,
                top=b.top,
                bottom=THIN,
                diagonal=b.diagonal,
                diagonal_direction=b.diagonal_direction,
                outline=b.outline,
                vertical=b.vertical,
                horizontal=b.horizontal,
            )

    # 先頭行の上端も線が欲しい場合（任意）
    for c in range(1, col_max + 1):
        cell = ws.cell(1, c)
        b = cell.border
        cell.border = Border(
            left=b.left,
            right=b.right,
            top=THIN,
            bottom=b.bottom,
            diagonal=b.diagonal,
            diagonal_direction=b.diagonal_direction,
            outline=b.outline,
            vertical=b.vertical,
            horizontal=b.horizontal,
        )

    # 罫線っぽく見せる（列幅と固定）
    ws.freeze_panes = "B3"
    ws.column_dimensions["A"].width = 8
    for col in range(2, ws.max_column + 1):
        ws.column_dimensions[get_column_letter(col)].width = 10


def add_inspection_triplet_sheet(
    wb: openpyxl.Workbook,
    name: str,
    schedule: pd.DataFrame,
    days: list[str],
    formations: list[str],
) -> None:
    ws = wb.create_sheet(name)

    ws.cell(1, 1, None)
    col = 2
    for formation_id in formations:
        ws.cell(1, col, formation_id)
        ws.merge_cells(start_row=1, start_column=col, end_row=1, end_column=col + 2)
        col += 3

    ws.cell(2, 1, None)
    col = 2
    for _ in formations:
        ws.cell(2, col, "operation")
        ws.cell(2, col + 1, "init_days_since_inspectionA")
        ws.cell(2, col + 2, "init_days_since_inspectionB")
        col += 3

    header_fill = PatternFill("solid", fgColor="1F4E79")
    header_font = Font(color="FFFFFF", bold=True)
    for r in (1, 2):
        for cell in ws[r]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")

    schedule_copy = schedule.copy()
    schedule_copy["day"] = schedule_copy["day"].astype(str)
    schedule_copy["formation_id"] = schedule_copy["formation_id"].astype(str)

    p_op = schedule_copy.pivot(
        index="day", columns="formation_id", values="operation_id"
    ).reindex(index=days, columns=formations)
    p_daysA = schedule_copy.pivot(
        index="day", columns="formation_id", values="daysA_before"
    ).reindex(index=days, columns=formations)
    p_daysB = schedule_copy.pivot(
        index="day", columns="formation_id", values="daysB_before"
    ).reindex(index=days, columns=formations)

    out_r = 3
    for day in days:
        ws.cell(out_r, 1, day)
        ws.cell(out_r, 1).alignment = Alignment(horizontal="center", vertical="center")

        col = 2
        for formation_id in formations:
            ov = p_op.loc[day, formation_id]
            av = p_daysA.loc[day, formation_id]
            bv = p_daysB.loc[day, formation_id]

            c_op = ws.cell(out_r, col, (None if pd.isna(ov) else str(ov)))
            c_a = ws.cell(out_r, col + 1, (None if pd.isna(av) else int(av)))
            c_b = ws.cell(out_r, col + 2, (None if pd.isna(bv) else int(bv)))

            for cell in (c_op, c_a, c_b):
                cell.alignment = Alignment(horizontal="center", vertical="center")

            col += 3
        out_r += 1

    THICK = Side(style="thick", color="000000")
    row_max = ws.max_row
    start_col = 2
    cols_per = 3
    for i in range(len(formations)):
        boundary_col = start_col + cols_per * (i + 1) - 1
        for r in range(1, row_max + 1):
            cell = ws.cell(r, boundary_col)
            b = cell.border
            cell.border = Border(
                left=b.left,
                right=THICK,
                top=b.top,
                bottom=b.bottom,
                diagonal=b.diagonal,
                diagonal_direction=b.diagonal_direction,
                outline=b.outline,
                vertical=b.vertical,
                horizontal=b.horizontal,
            )

    THIN = Side(style="thin", color="000000")
    col_max = ws.max_column
    for r in range(1, row_max + 1):
        for c in range(1, col_max + 1):
            cell = ws.cell(r, c)
            b = cell.border
            cell.border = Border(
                left=b.left,
                right=b.right,
                top=b.top,
                bottom=THIN,
                diagonal=b.diagonal,
                diagonal_direction=b.diagonal_direction,
                outline=b.outline,
                vertical=b.vertical,
                horizontal=b.horizontal,
            )

    for c in range(1, col_max + 1):
        cell = ws.cell(1, c)
        b = cell.border
        cell.border = Border(
            left=b.left,
            right=b.right,
            top=THIN,
            bottom=b.bottom,
            diagonal=b.diagonal,
            diagonal_direction=b.diagonal_direction,
            outline=b.outline,
            vertical=b.vertical,
            horizontal=b.horizontal,
        )

    ws.freeze_panes = "B3"
    ws.column_dimensions["A"].width = 8
    for col in range(2, ws.max_column + 1):
        ws.column_dimensions[get_column_letter(col)].width = 10


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
    # day/formation の順序をここで固定
    days = sorted(schedule["day"].astype(str).unique(), key=lambda s: (len(s), s))
    formations = sorted(schedule["formation_id"].astype(str).unique())
    add_formation_triplet_sheet(wb, "gantt_triplet", schedule, days, formations)
    add_inspection_triplet_sheet(
        wb, "gantt_inspection_triplet", schedule, days, formations
    )
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
    ap.add_argument(
        "--seed", type=int, default=42, help="津田沼ランダム割当の乱数seed（再現性用）"
    )
    ap.add_argument(
        "--tsudanuma-code",
        default="JB33",
        help="津田沼の駅コード（master_dataに合わせる）",
    )

    args = ap.parse_args()

    ops, forms, rules = read_master(args.master)
    schedule, gantt_ops, gantt_start, gantt_end = make_baseline_schedule(
        ops,
        forms,
        max_days_since_inspectionA=rules["max_days_since_inspectionA"],
        max_days_since_inspectionB=rules["max_days_since_inspectionB"],
        days=args.days,
        start_date=args.start_date,
        default_idle_op=args.default_idle_op,
        seed=args.seed,
        tsudanuma_code=args.tsudanuma_code,
    )
    export_baseline_excel(
        args.out, schedule, gantt_ops, gantt_start, gantt_end, ops, forms
    )
    print(f"OK: {args.out}")
    print(f"schedule_long: {schedule.shape[0]} rows, {schedule.shape[1]} cols")


if __name__ == "__main__":
    main()
