import math


def calculate_entropy(text: str) -> float:
    frequencies = {}
    for char in text:
        if char in frequencies:
            frequencies[char] += 1
        else:
            frequencies[char] = 1
    length = len(text)
    entropy = 0
    for char in frequencies:
        probability = frequencies[char] / length
        contribution = -probability * math.log2(probability)
        entropy += contribution
    return entropy
