# from RPA.Cloud.AWS import AWS
from ExtendedAWS import ExtendedAWS
import os
import pathlib

AWSlibrary = ExtendedAWS()


def main():
    AWSlibrary.init_s3_client(
        os.getenv("AWS_KEY_ID"), os.getenv("AWS_KEY_SECRET"), os.getenv("AWS_REGION")
    )
    files = AWSlibrary.list2_files(
        "testing-s3-changes", max_keys=3, filter="Contents[?contains(Key, '.png')]"
    )  # , prefix="r")

    for f in files:
        print(f)
    filedir = pathlib.Path(__file__).parent.resolve()
    # ExtraArgs={'Metadata': {'mykey': 'myvalue'}}
    upload_files = [
        {
            "filename": "./image.png",
            "object_name": "image.png",
            "ExtraArgs": {"ContentType": "image/png", "Metadata": {"importance": "1"}},
        },
        {
            "filename": "./doc.pdf",
            "object_name": "doc.pdf",
            "ExtraArgs": {"ContentType": "application/pdf"},
        },
    ]
    # AWSlibrary.upload_files("testing-s3-changes", files=upload_files)
    # AWSlibrary.create_bucket("uusi-public-bucketti", ACL="public-read-write")


if __name__ == "__main__":
    main()
