from datetime import datetime, timezone
from typing import List, Dict, Callable, Any, Tuple
import random

from bitemporal_space import BitemporalSpace, UpdateAction
from generate_student_data import generate_student_data
from plot_bitemporal_space import plot_bitemporal_space


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
        
        timestamps: List[Tuple[datetime, datetime]] = []
        # Generate random transaction and valid times
        transaction_timestamps = set()

        for i in range(num_points_per_id):
            # Random valid time (vt)
            vt_ts = random.uniform(start_ts, end_ts)
            vt = datetime.fromtimestamp(vt_ts, tz=timezone.utc)            
            # Random transaction time (tt), slightly later than vt
            tt_ts = random.uniform(vt_ts, end_ts)
            tt = datetime.fromtimestamp(tt_ts, tz=timezone.utc)
            if tt in transaction_timestamps:
                continue
            transaction_timestamps.add(tt)
            timestamps.append((tt, vt))

        timestamps.sort(key=lambda x: x[0])  # Sort by transaction time

        for i in range(len(timestamps)):
            tt, vt = timestamps[i]
            # Flexible data (random age and name)
            data = generate_data(id)
            
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
start_time = datetime.fromisoformat("2024-05-01T00:00:00+00:00")
end_time = datetime.fromisoformat("2024-06-30T00:00:00+00:00")
num_ids = 1
num_points_per_id = 10

# print(generate_rectangles(start_time, end_time, num_ids, num_points_per_id, generate_student_data))  # Visualize the random rectangles
space = BitemporalSpace()
space.rects = generate_rectangles(start_time, end_time, num_ids, num_points_per_id, generate_student_data)
# plot_bitemporal_space(space)  # Visualize the random rectangles





# Example usage with your specific times
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
