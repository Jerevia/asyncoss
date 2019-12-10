Async Alibaba Cloud OSS SDK
===========================

Based on aiohttp and oss2, Require python3.6+

Installing
----------

.. code-block:: shell

    pip install asyncoss


Getting started
----------------

.. code-block:: python

    import asyncoss
    import asyncio

    endpoint = 'http://oss-cn-beijing.aliyuncs.com'

    auth = asyncoss.Auth('<Your AccessKeyID>', '<Your AccessKeySecret>')

    async def main():
        # The object key in the bucket is story.txt
        async with asyncoss.Bucket(auth, endpoint, '<your bucket name>') as bucket:
            key = 'story.txt'

            # Upload
            await bucket.put_object(key, 'Ali Baba is a happy youth.')

            # Download
            result = await bucket.get_object(key)
            await result.resp.read()

            # Delete
            await bucket.delete_object(key)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
