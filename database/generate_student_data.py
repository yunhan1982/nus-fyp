import random
from typing import Dict, Any

def generate_student_data(id: int) -> Dict[str, Any]:
    return {
        "id": id,
        "age": random.randint(10, 20),
        "name": f"Student_{id}",
        "attr1": random.randint(1, 40),
        "attr2": random.randint(1, 400),
        "attr3": random.randint(1, 800),
        "attr4": random.randint(1, 2000),
    }

