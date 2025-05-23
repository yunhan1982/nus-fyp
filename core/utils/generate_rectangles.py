from datetime import datetime, timezone
from typing import List, Dict, Callable, Any, Tuple
import random
import uuid

from core.bitemporal_space import BitemporalSpace, UpdateAction
from .plot_bitemporal_space import plot_bitemporal_space


# Helper function to capture transformations
def capture(space: BitemporalSpace, tt: datetime, work: Callable[[BitemporalSpace], List[UpdateAction]]) -> BitemporalSpace:
    new_space = space.transform(tt, work)
    return new_space


def generate_rectangles(start_time: datetime, end_time: datetime, num_ids: int, num_points_per_id: int, generate_data: Callable[[int], Dict[str, Any]],) -> BitemporalSpace:

    # Function to generate rectangles with random timestamps
    def generate_rectangles_for_id(id: int) -> BitemporalSpace:
        space = BitemporalSpace()
        
        # Convert datetime to seconds for random generation
        start_ts = start_time.timestamp()
        end_ts = end_time.timestamp()
        
        # Generate random transaction and valid times
        transaction_timestamps = set()
        valid_timestamps = set()
        timestamps: List[Tuple[datetime, datetime]] = []

        # Divide the time range into segments
        total_time_range = end_ts - start_ts
        vt_segment_size = total_time_range / (num_points_per_id * 2)  # Smaller segments for more uniform distribution
        tt_segment_size = total_time_range / (num_points_per_id * 2)

        for i in range(num_points_per_id):
            # Calculate segment boundaries for valid time
            vt_segment_start = start_ts + (i * vt_segment_size)
            vt_segment_end = vt_segment_start + vt_segment_size
            
            # Generate valid time within its segment
            vt_ts = random.uniform(vt_segment_start, vt_segment_end)
            vt = datetime.fromtimestamp(vt_ts, tz=timezone.utc)
            
            # Calculate segment boundaries for transaction time
            # Ensure tt is always after vt
            tt_segment_start = max(vt_ts, start_ts + (i * tt_segment_size))
            tt_segment_end = start_ts + ((i + 1) * tt_segment_size)
            
            # Generate transaction time within its segment
            tt_ts = random.uniform(tt_segment_start, tt_segment_end)
            tt = datetime.fromtimestamp(tt_ts, tz=timezone.utc)
            
            if tt in transaction_timestamps or vt in valid_timestamps:
                continue
                
            transaction_timestamps.add(tt)
            valid_timestamps.add(vt)
            timestamps.append((tt, vt))

        # No need to sort as timestamps are already ordered
        data_id = uuid.uuid4()

        for i in range(len(timestamps)):
            tt, vt = timestamps[i]
            # Flexible data (random age and name)
            data = generate_data(data_id)
            # Insert the point
            space = capture(space, tt, lambda s: s.insert_point(data, vt))
        
        return space

    # Generate random rectangles
    timeslices = []
    for i in range(1, num_ids + 1):
        random_space = generate_rectangles_for_id(i)
        timeslices.extend(random_space.rects)

    return timeslices
    # print("\nGenerated random rectangles:")
    # print(random_space)
    # plot_bitemporal_space(random_space)  # Visualize the random rectangles

# Generate random rectangles
# start_time = datetime.fromisoformat("2024-05-01T00:00:00+00:00")
# end_time = datetime.fromisoformat("2024-06-30T00:00:00+00:00")
# num_ids = 1
# num_points_per_id = 10

# space = BitemporalSpace()
# space.rects = generate_rectangles(start_time, end_time, num_ids, num_points_per_id, generate_student_data)





# # Example usage with your specific times
# vt1 = datetime.fromisoformat("2024-05-16T00:00:00+00:00")
# vt2 = datetime.fromisoformat("2024-05-17T00:00:00+00:00")
# vt3 = datetime.fromisoformat("2024-05-18T00:00:00+00:00")
# vt4 = datetime.fromisoformat("2024-05-19T00:00:00+00:00")
# vt5 = datetime.fromisoformat("2024-05-20T00:00:00+00:00")

# tt1 = datetime.fromisoformat("2024-06-16T00:00:00+00:00")
# tt2 = datetime.fromisoformat("2024-06-17T00:00:00+00:00")
# tt3 = datetime.fromisoformat("2024-06-18T00:00:00+00:00")
# tt4 = datetime.fromisoformat("2024-06-19T00:00:00+00:00")
# tt5 = datetime.fromisoformat("2024-06-20T00:00:00+00:00")
# tt6 = datetime.fromisoformat("2024-06-21T00:00:00+00:00")

# # Replicate your original operations
# space = BitemporalSpace()
# space = capture(space, tt1, lambda s: s.insert_point({"age": 1, "name": "Joe"}, vt1))
# space = capture(space, tt2, lambda s: s.insert_point({"age": 2, "name": "Joe"}, vt1))
# space = capture(space, tt3, lambda s: s.insert_point({"age": 3, "name": "Joe"}, vt3))
# space = capture(space, tt5, lambda s: s.insert_point({"age": 4, "name": "Joe"}, vt5))
# space = capture(space, tt6, lambda s: s.insert_point({"age": -1, "name": "Joe"}, vt4))


# print("After specific inserts:")
# plot_bitemporal_space(space)  # Visualize the specific rectangles



