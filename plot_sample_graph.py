from datetime import datetime
from core.utils.plot_bitemporal_space import plot_bitemporal_space
from core.utils.generate_student_data import generate_student_data
from core.utils.generate_rectangles import generate_rectangles, capture
from core.bitemporal_space import BitemporalSpace


# Generate random rectangles
start_time = datetime.fromisoformat("2024-05-01T00:00:00+00:00")
end_time = datetime.fromisoformat("2024-06-30T00:00:00+00:00")
num_ids = 1
num_points_per_id = 10

space = BitemporalSpace()
space.rects = generate_rectangles(start_time, end_time, num_ids, num_points_per_id, generate_student_data)

print(space.rects)


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
# space = capture(space, tt1, lambda s: s.insert_point({"payload":{"age": 1, "name": "Joe"}}, vt1))
# space = capture(space, tt2, lambda s: s.insert_point({"payload":{"age": 2, "name": "Joe"}}, vt1))
# space = capture(space, tt3, lambda s: s.insert_point({"payload":{"age": 3, "name": "Joe"}}, vt3))
# space = capture(space, tt5, lambda s: s.insert_point({"payload":{"age": 4, "name": "Joe"}}, vt5))
# space = capture(space, tt6, lambda s: s.insert_point({"payload":{"age": -1, "name": "Joe"}}, vt4))

# print(space.rects)
# print("After specific inserts:")
plot_bitemporal_space(space)  # Visualize the specific rectangles



