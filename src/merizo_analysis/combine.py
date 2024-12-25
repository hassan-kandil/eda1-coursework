import math
from collections import Counter
import time

# Define the zero value for the accumulator: (float_sum, float_sum_of_squares, float_count, combined_dict)
zero_value = (0.0, 0.0, 0, {})

# Function to combine a single record with the accumulator
def seq_op(accum, value):
    float_sum, float_sum_of_squares, float_count, combined_dict = accum
    new_float, new_dict = value
    return (
        float_sum + new_float,
        float_sum_of_squares + new_float ** 2,
        float_count + 1,
        dict(Counter(combined_dict) + Counter(new_dict)),
    )

# Function to merge two accumulators
def comb_op(accum1, accum2):
    float_sum1, float_sum_of_squares1, float_count1, dict1 = accum1
    float_sum2, float_sum_of_squares2, float_count2, dict2 = accum2
    return (
        float_sum1 + float_sum2,
        float_sum_of_squares1 + float_sum_of_squares2,
        float_count1 + float_count2,
        dict(Counter(dict1) + Counter(dict2)),
    )

def extract_results(result):
    float_sum, float_sum_of_squares, float_count, combined_dict = result
    mean = float_sum / float_count

    population_variance = (float_sum_of_squares - (float_sum ** 2) / float_count) / float_count
    population_std_dev = math.sqrt(population_variance)

    return mean, population_std_dev, combined_dict



