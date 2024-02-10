
from datetime import timedelta
from redis import Redis
from ratelimit import lb

def main():
    # Connect to Redis (make sure your Redis server is running)
    redis_client = Redis(host='localhost', port=6379, db=0)

    
    bucket_key = 'bucket1'
    bucket_capacity = 10
    leak_rate = 0.1  # leak rate

    
    for i in range(15):
        is_limited = lb.request_is_limited_leaky_bucket(redis_client, bucket_key, bucket_capacity, leak_rate, request_tokens=1)

        if not is_limited:
            print(f"Request {i + 1}: Allowed")
        else:
            print(f"Request {i + 1}: Limited")

if __name__ == "__main__":
    main()
