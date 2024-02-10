
from datetime import datetime, timedelta
from redis import Redis

def leaky_bucket(r: Redis, key: str, capacity: int, leak_rate: float):
    # Get the current timestamp
    current_time = datetime.now()

    # Retrieve the last updated timestamp and the current number of tokens in the bucket
    last_updated = r.hget(key, 'last_updated')
    tokens = int(r.hget(key, 'tokens')) if r.hget(key, 'tokens') else 0

    # Calculate the time difference since the last update
    if last_updated:
        time_diff = (current_time - datetime.fromisoformat(last_updated)).total_seconds()
    else:
        time_diff = 0

    # Calculate the number of tokens that should be added based on the leak rate
    tokens_to_add = int(time_diff * leak_rate)

    # Update the last updated timestamp
    r.hset(key, 'last_updated', current_time.isoformat())

    # Add tokens to the bucket, but do not exceed the capacity
    tokens = min(tokens + tokens_to_add, capacity)
    r.hset(key, 'tokens', tokens)

    return tokens

def request_is_limited_leaky_bucket(r: Redis, key: str, capacity: int, leak_rate: float, request_tokens: int = 1):
    # Use the leaky_bucket function to update the bucket state
    tokens_in_bucket = leaky_bucket(r, key, capacity, leak_rate)

    # Check if there are enough tokens for the requested operation
    if tokens_in_bucket >= request_tokens:
        # Consume the tokens for the request
        r.hset(key, 'tokens', tokens_in_bucket - request_tokens)
        return False
    else:
        return True
