import random
from typing import Dict, Any

def generate_student_data(id: int) -> Dict[str, Any]:
    return {
        "id": id,
        "age": random.randint(10, 20),
        "name": f"Student_{id}"
    }
