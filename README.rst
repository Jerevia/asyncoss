Async aliyun OSS library
=========================

Based on aiohttp and oss2, Require python3.6+

To get an object

.. code-block:: python

    result = await bucket.get_object(...

To read the content of object::

    body = await result.resp.read()


Install
----------------
.. code-block:: shell
    pip install asyncoss


Getting started
----------------

.. code-block:: python

    import asyncoss

    endpoint = 'http://oss-cn-beijing.aliyuncs.com' # Suppose that your bucket is in the Beijing region.

    auth = asyncoss.Auth('<Your AccessKeyID>', '<Your AccessKeySecret>')

    async def main(loop):
        # The object key in the bucket is story.txt
        async with asyncoss.Bucket(auth, endpoint, '<your bucket name>') as bucket:
            key = 'story.txt'

            # Upload
            await bucket.put_object(key, 'Ali Baba is a happy youth.')

            # Upload
            data = dict(a=1, b=2)
            await bucket.put_object(key, json.dumps(data), headers={'Content-Type': 'application/json'})

            # Download
            result = await bucket.get_object(key)
            print(result.headers)
            print(await result.resp.read())

            # Delete
            await bucket.delete_object(key)

            # Traverse all objects in the bucket
            async for object_info in asyncoss.ObjectIterator(bucket):
                print(object_info.key)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(go(main))
