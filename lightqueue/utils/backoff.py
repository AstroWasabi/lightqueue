"""
Calculates retry backoff delays with exponential growts and random jitter.
"""

import random

def calculate_backoff(
    retries: int,
    base: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
) -> float:
    """
    Computes exponential backoff delay:
        delay = min(max_delay, base *(2** retries)) + jitter
    """

    # exponential Calculation
    delay = base * (2**retries)

    # cap at maximum delay to avoid waiting hours
    delay = min(delay,max_delay)

    #Add random jitter between 0.0 and 1.0 seconds
    if jitter:
        delay += random.uniform(0.0,1.0)

    return delay
