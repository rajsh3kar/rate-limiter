from datetime import datetime, timedelta
from redis import Redis

def leaky_bucket(r: Redis, key: str, capacity: int, leak_rate: float):
    
    current_time = datetime.now()

    
    last_updated = r.hget(key, 'last_updated')
    tokens = int(r.hget(key, 'tokens')) if r.hget(key, 'tokens') else 0

   
    last_updated_str = last_updated.decode('utf-8') if last_updated else None


    if last_updated_str:
        time_diff = (current_time - datetime.fromisoformat(last_updated_str)).total_seconds()
    else:
        time_diff = 0

   
    tokens_to_add = int(time_diff * leak_rate)

    
    r.hset(key, 'last_updated', current_time.isoformat())

   
    tokens = min(tokens + tokens_to_add, capacity)
    r.hset(key, 'tokens', tokens)

    return tokens

def request_is_limited_leaky_bucket(r: Redis, key: str, capacity: int, leak_rate: float, request_tokens: int = 1):
    
    tokens_in_bucket = leaky_bucket(r, key, capacity, leak_rate)

    
    if tokens_in_bucket >= request_tokens:
        
        r.hset(key, 'tokens', tokens_in_bucket - request_tokens)
        return False
    else:
        return True
