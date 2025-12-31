from minio import Minio
from minio.error import S3Error
import uuid
import io
import random
import string

# מניח ש-MinIO רץ על localhost:9000 כמו שעשינו עם Docker
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "my-test-bucket"

# יוצרים לקוח (client) שמדבר עם MinIO
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False  # אנחנו על http ולא https
)

# function that checks if the bucket exists and creates one if not.
def ensure_bucket_exists():
    found = client.bucket_exists(BUCKET_NAME)
    if not found:
        print(f"Bucket {BUCKET_NAME} does not exist, creating it...")
        client.make_bucket(BUCKET_NAME)
    else:
        print(f"Bucket {BUCKET_NAME} already exists.")

# a function that creates a random object name 
def generate_random_name():
    return f"object-{uuid.uuid4().hex}.txt"

# a function that creates a random text as content for the object
def generate_random_data():
    letters = string.ascii_letters + string.digits
    random_text = "".join(random.choice(letters) for _ in range(50))
    full_text = f"Random data: {random_text}"
    return full_text.encode("utf-8")  # returns bytes because put_object wants bytes

# function that uploads a new object with random name and content and returns its name.
def upload_random_object():
    object_name = generate_random_name()
    data = generate_random_data()
    data_stream = io.BytesIO(data)
    size = len(data)

    client.put_object(
        BUCKET_NAME,
        object_name,
        data_stream,
        size,
        content_type="text/plain"
    )
    print(f"a new object was created: {object_name}")
    return object_name

# a function that prints every object in the bucket.
def list_objects():
    print(f"\nexisting objects in bucket {BUCKET_NAME}:")
    objects = list(client.list_objects(BUCKET_NAME, recursive=True))
    if not objects:
        print("there are no objects.")
        return []

    for obj in objects:
        print(f"- {obj.object_name} (size={obj.size} bytes)")
    return objects

# a function that reads the content of an existing object and prints it.
def read_object(object_name):
    print(f"\nreading the object: {object_name}")
    response = client.get_object(BUCKET_NAME, object_name)
    try:
        data = response.read()
        print("object content:")
        print(data.decode("utf-8", errors="replace"))
    finally:
        response.close()
        response.release_conn()

# 
# updates an object by uploading new content with the same name.
# if versioning is enabled on the bucket:
#   - each upload saves a new version.
#   - you can go back to the old version if you'd like to.
# if versioning is not enabled:
#   - the old version is overwritten and there is no history.
#
def update_object(object_name):
    print(f"\nupdates the object: {object_name}")
    new_text = f"UPDATED DATA for object {object_name} - {uuid.uuid4().hex}"
    data = new_text.encode("utf-8")
    data_stream = io.BytesIO(data)

    client.put_object(
        BUCKET_NAME,
        object_name,
        data_stream,
        len(data),
        content_type="text/plain"
    )
    print("the object has been updated with new content.")

# the function deletes an existing object from the bucket.
def delete_object(object_name):
    print(f"\ndeleting the object: {object_name}")
    client.remove_object(BUCKET_NAME, object_name)
    print("the object has been deleted.")

def main():
    try:
        # 1. checking that the bucket exists
        ensure_bucket_exists()

        # 2. creating a random object (with random name and content)
        created_object = upload_random_object()

        # 3. presenting the list of the objects
        objects = list_objects()

        # if there's at least one object we will take it to read and update
        if objects:
            target_name = objects[0].object_name

            # 4. reading the object
            read_object(target_name)

            # 5. updating the object (same name, new content)
            update_object(target_name)

            # reading again after the update
            read_object(target_name)

            # 6. removing the object we created earlier 
            delete_object(created_object)

            # presenting again the list of objects after the delete
            list_objects()
        else:
            print("there are no objects to work with.")

    except S3Error as e:
        print("there has been an error connecting with MinIO / S3:")
        print(e)

if __name__ == "__main__":
    main()
