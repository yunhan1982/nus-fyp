from datetime import datetime, timezone
from typing import Optional, List, Dict, Callable, Any

# Constants
INFINITY = datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

# Rectangle class to represent a timeslice
class Rectangle:
    def __init__(self, data: Dict[str, Any], tt_from: datetime, tt_to: datetime, vt_from: datetime, vt_to: datetime, index: int):
        self.data = data
        self.tt_from = tt_from
        self.tt_to = tt_to
        self.vt_from = vt_from
        self.vt_to = vt_to
        self.index = index

    def __repr__(self):
        return (f"Rectangle(data={self.data}, ttInterval=[{self.tt_from}, {self.tt_to}], "
                f"vtInterval=[{self.vt_from}, {self.vt_to}], index={self.index})")

# Update actions
class UpdateAction:
    pass

class Insert(UpdateAction):
    def __init__(self, data: Dict[str, Any], vt_from: datetime, vt_to: datetime):
        self.data = data
        self.vt_from = vt_from
        self.vt_to = vt_to

    def __repr__(self):
        return f"Insert(data={self.data}, vtInterval=[{self.vt_from}, {self.vt_to}])"

class Invalidate(UpdateAction):
    def __init__(self, rect: Rectangle):
        self.rect = rect

    def __repr__(self):
        return f"Invalidate(index={self.rect.index})"

class AdjustVTInterval(UpdateAction):
    def __init__(self, rect: Rectangle, vt_from: datetime, vt_to: datetime):
        self.rect = rect
        self.vt_from = vt_from
        self.vt_to = vt_to

    def __repr__(self):
        return f"AdjustVTInterval(index={self.rect.index}, vtInterval=[{self.vt_from}, {self.vt_to}])"


# BitemporalSpace implementation
class BitemporalSpace:
    def __init__(self):
        self.rects: List[Rectangle] = []
        self.latest_tx_time: Optional[datetime] = None
        self.time_slice_count = 0
        
    def on_tx_time(self, tt: datetime) -> List[Rectangle]:
        active = [r for r in self.rects if r.tt_from <= tt < r.tt_to]
        # active = [r for r in self.rects if (r.tt_to == INFINITY and tt == INFINITY) and (r.tt_from <= tt < r.tt_to)]
        return sorted(active, key=lambda r: r.vt_from)
    
    def insert_point(self, item: Optional[Dict[str, Any]], vtf: datetime, filter_func: Callable[[Rectangle], bool] = lambda _: False) -> List[UpdateAction]:
        rects = self.on_tx_time(INFINITY)
        rect_option = next((r for r in rects if vtf < r.vt_to), None)

        def insert(vtt: datetime, cur_rect: Optional[Rectangle] = None) -> List[UpdateAction]:
            if not item:  # None case
                return []
            value = item
            next_rect = next((r for r in rects if r.vt_from == vtt and filter_func(r)), None)
            if cur_rect and filter_func(cur_rect) and next_rect:
                return [Invalidate(next_rect), Insert(value, vtf, next_rect.vt_to)]
            return [Insert(value, vtf, vtt)]


        if rect_option and rect_option.vt_from > vtf:
            return insert(rect_option.vt_from)
        elif rect_option:
            adjust = Invalidate(rect_option) if rect_option.vt_from == vtf else AdjustVTInterval(rect_option, rect_option.vt_from, vtf)
            return [adjust] + insert(rect_option.vt_to, rect_option)
        else:
            return insert(INFINITY)

    def execute(self, actions: List[UpdateAction], tt: datetime) -> None:
        new_rects = self.rects.copy()
        self.latest_tx_time = tt if not self.latest_tx_time or tt > self.latest_tx_time else self.latest_tx_time

        for action in actions:
            if isinstance(action, Insert):
                self.time_slice_count += 1
                new_rects.append(Rectangle(action.data, tt, INFINITY, action.vt_from, action.vt_to, self.time_slice_count))
            elif isinstance(action, Invalidate):
                idx = new_rects.index(action.rect)
                new_rects[idx] = Rectangle(action.rect.data, action.rect.tt_from, tt, action.rect.vt_from, action.rect.vt_to, action.rect.index)
            elif isinstance(action, AdjustVTInterval):
                idx = new_rects.index(action.rect)
                new_rects[idx] = Rectangle(action.rect.data, action.rect.tt_from, tt, action.rect.vt_from, action.rect.vt_to, action.rect.index)
                self.time_slice_count += 1
                new_rects.append(Rectangle(action.rect.data, tt, INFINITY, action.vt_from, action.vt_to, self.time_slice_count))
        self.rects = new_rects


    def transform(self, tt: datetime, work: Callable[['BitemporalSpace'], List[UpdateAction]]) -> 'BitemporalSpace':
        actions = work(self)
        new_space = BitemporalSpace()
        new_space.rects = self.rects.copy()
        new_space.latest_tx_time = self.latest_tx_time
        new_space.time_slice_count = self.time_slice_count
        new_space.execute(actions, tt)
        return new_space

    def __repr__(self):
        return f"BitemporalSpace(rects={self.rects}, latestTxTime={self.latest_tx_time})"
