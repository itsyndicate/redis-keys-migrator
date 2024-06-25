import redis
import time
import logging

# Variables
DESTINATION_REDIS_HOST = "localhost"
DESTINATION_REDIS_PORT = 6380
DESTINATION_REDIS_DB_INDEX = 0
SOURCE_REDIS_HOST = "localhost"
SOURCE_REDIS_PORT = 6381
SOURCE_REDIS_DB_INDEX = 0
SCAN_MATCH_PATTERN = "*"
SCAN_BATCH_SIZE = 1000
PIPELINE_BATCH_SIZE = 1000
MAX_PROCESSED_EXISTING_KEYS_LIMIT = 1000
SLEEP_TIME = 1
ERROR_LOG_FILENAME = "redis-keys-migrator-error.log"
FILEMOD = "w"
MAX_ERRORED_KEYS_LIMIT = 1000

# Initialize logging
logging.basicConfig(
    level=logging.ERROR,
    format="\n[%(asctime)s - %(levelname)s - %(message)s]",
    filename=ERROR_LOG_FILENAME,
    filemode=FILEMOD)

# Connect to Redis instances
try:
    destination_redis = redis.Redis(
        host=DESTINATION_REDIS_HOST,
        port=DESTINATION_REDIS_PORT,
        db=DESTINATION_REDIS_DB_INDEX)
    source_redis = redis.Redis(
        host=SOURCE_REDIS_HOST,
        port=SOURCE_REDIS_PORT,
        db=SOURCE_REDIS_DB_INDEX)

    # Test connection
    destination_redis.ping()
    source_redis.ping()
    print("\nConnected to Redis instances successfully")

except redis.ConnectionError as e:
    print(f"\nError connecting to Redis : {e}")
    exit(1)



# Keys migration function
def process_key(pipeline, key, source_redis):
    # Get key type
    try:
        type_key = source_redis.type(key).decode('utf-8')

    except Exception as e:
        return f"Error getting type for key |{key}|: {e}"

    # Migrate key to destination Redis instance
    try:
        if type_key == 'string':
            value = source_redis.get(key)
            pipeline.set(key, value)

        elif type_key == 'list':
            values = source_redis.lrange(key, 0, -1)
            pipeline.rpush(key, *values)

        elif type_key == 'set':
            values = source_redis.smembers(key)
            pipeline.sadd(key, *values)

        elif type_key == 'hash':
            fields = source_redis.hgetall(key)
            pipeline.hset(key, mapping=fields)

        elif type_key == 'zset':
            values = source_redis.zrange(key, 0, -1, withscores=True)
            pipeline.zadd(key, dict(values))

        elif type_key == 'stream':
            entries = source_redis.xrange(key)
            for entry_id, entry_data in entries:
                pipeline.xadd(key, entry_data, id=entry_id)

        else:
            return f"Unknown key type for key |{key}| : {type_key}"

    except Exception as e:
        return f"Error processing key |{key}| of type {type_key}: {e}"

    return None



# Main function to handle keys processing
def main():
    restored_keys_count = 0
    existing_keys_count = 0
    processed_existing_keys_count = 0
    pipeline_commands_count = 0
    failed_keys_count = 0

    # Redis pipelining
    pipeline = destination_redis.pipeline()

    # Iterate the set of source keys using SCAN
    keys = source_redis.scan_iter(
        match=SCAN_MATCH_PATTERN,
        count=SCAN_BATCH_SIZE)

    for key in keys:
        # Key decoding
        try:
            key = key.decode('utf-8')

        except Exception as e:
            logging.error(f"Failed to decode key |{key}| : {e}")
            failed_keys_count += 1

            # Stop |for| loop, if the value of MAX_ERRORED_KEYS_LIMIT is reached
            if failed_keys_count >= MAX_ERRORED_KEYS_LIMIT:
                print(
                    f"\nMax errored keys limit of {MAX_ERRORED_KEYS_LIMIT} reached")
                break

            # End the current iteration
            else:
                continue

        # Check key existence in the destination Redis instance
        if not destination_redis.exists(key):
            message = process_key(pipeline, key, source_redis)

            # If |process_key| function returns a message => key migration failed
            if message:
                logging.error(message)
                failed_keys_count += 1

            else:
                restored_keys_count += 1
                pipeline_commands_count += 1

        else:
            existing_keys_count += 1
            processed_existing_keys_count += 1

        # Offload CPU usage, if the value of MAX_PROCESSED_EXISTING_KEYS_LIMIT is reached
        if processed_existing_keys_count >= MAX_PROCESSED_EXISTING_KEYS_LIMIT:
            processed_existing_keys_count = 0
            time.sleep(SLEEP_TIME)

        # Execute Redis pipeline
        if pipeline_commands_count >= PIPELINE_BATCH_SIZE:
            pipeline.execute()
            pipeline = destination_redis.pipeline()
            pipeline_commands_count = 0
            time.sleep(SLEEP_TIME) # CPU offloading

        # Stop |for| loop, if the value of MAX_ERRORED_KEYS_LIMIT is reached
        if failed_keys_count >= MAX_ERRORED_KEYS_LIMIT:
            print(
                f"\nMax errored keys limit of {MAX_ERRORED_KEYS_LIMIT} reached")
            break

    # Execute any remaining pipeline commands
    if pipeline_commands_count > 0:
        pipeline.execute()

    # Output execution results
    print(f"\nNumber of restored keys: {restored_keys_count}")
    print(f"\nNumber of skipped keys: {existing_keys_count}")
    print(f"\nNumber of failed keys: {failed_keys_count}")

    # Notify about any occurred errors
    if failed_keys_count > 0:
        print(
            f"\nErrors occurred during the execution. Check |{ERROR_LOG_FILENAME}| for details")


if __name__ == "__main__":
    main()
