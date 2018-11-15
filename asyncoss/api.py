# -*- coding: utf-8 -*-
import shutil

from oss2 import defaults, utils, xml_utils
from oss2.compat import to_string, to_unicode, urlparse, urlquote
from asyncoss import models, exceptions
from asyncoss import http


class _Base(object):
    def __init__(self, auth, endpoint, is_cname, session, connect_timeout,
                 app_name='', enable_crc=False, loop=None):
        self.auth = auth
        self.endpoint = _normalize_endpoint(endpoint.strip())
        self.session = session or http.Session(loop=loop)
        self.timeout = defaults.get(connect_timeout, defaults.connect_timeout)
        self.app_name = app_name
        self.enable_crc = enable_crc

        self._make_url = _UrlMaker(self.endpoint, is_cname)

    async def _do(self, method, bucket_name, key, **kwargs):
        key = to_string(key)
        req = http.Request(method, self._make_url(bucket_name, key),
                           app_name=self.app_name,
                           **kwargs)
        self.auth._sign_request(req, bucket_name, key)
        resp = await self.session.do_request(req, timeout=self.timeout)

        if resp.status // 100 != 2:
            e = await exceptions.make_exception(resp)
            raise e

        content_length = models._hget(resp.headers, 'content-length', int)
        if content_length is not None and content_length == 0:
            await resp.read()

        return resp

    async def _parse_result(self, resp, parse_func, klass):
        result = klass(resp)
        body = await resp.read()
        parse_func(result, body)
        return result

    async def __aenter__(self):
        await self.session._aio_session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session._aio_session.__aexit__(exc_type, exc_val, exc_tb)

    async def close(self):
        await self.session._aio_session.close()


class Service(_Base):
    def __init__(self, auth, endpoint,
                 session=None,
                 connect_timeout=None,
                 app_name='',
                 loop=None):
        super().__init__(auth, endpoint, False, session, connect_timeout,
                         app_name=app_name, loop=loop)

    async def list_buckets(self, prefix='', marker='', max_keys=100):
        """根据前缀罗列用户的Bucket。

        :param str prefix: 只罗列Bucket名为该前缀的Bucket，空串表示罗列所有的Bucket
        :param str marker: 分页标志。首次调用传空串，后续使用返回值中的next_marker
        :param int max_keys: 每次调用最多返回的Bucket数目

        :return: 罗列的结果
        :rtype: oss2.models.ListBucketsResult
        """
        resp = await self._do('GET', '', '',
                              params={'prefix': prefix,
                                      'marker': marker,
                                      'max-keys': str(max_keys)})
        return await self._parse_result(resp, xml_utils.parse_list_buckets, models.ListBucketsResult)


class Bucket(_Base):
    """用于Bucket和Object操作的类，诸如创建、删除Bucket，上传、下载Object等。

    用法（假设Bucket属于杭州区域） ::

        >>> import oss2
        >>> auth = oss2.Auth('your-access-key-id', 'your-access-key-secret')
        >>> bucket = oss2.Bucket(auth, 'http://oss-cn-hangzhou.aliyuncs.com', 'your-bucket')
        >>> bucket.put_object('readme.txt', 'content of the object')
        <oss2.models.PutObjectResult object at 0x029B9930>

    :param auth: 包含了用户认证信息的Auth对象
    :type auth: oss2.Auth

    :param str endpoint: 访问域名或者CNAME
    :param str bucket_name: Bucket名
    :param bool is_cname: 如果endpoint是CNAME则设为True；反之，则为False。

    :param session: 会话。如果是None表示新开会话，非None则复用传入的会话
    :type session: oss2.Session

    :param float connect_timeout: 连接超时时间，以秒为单位。

    :param str app_name: 应用名。该参数不为空，则在User Agent中加入其值。
        注意到，最终这个字符串是要作为HTTP Header的值传输的，所以必须要遵循HTTP标准。
    """

    ACL = 'acl'
    CORS = 'cors'
    LIFECYCLE = 'lifecycle'
    LOCATION = 'location'
    LOGGING = 'logging'
    REFERER = 'referer'
    WEBSITE = 'website'
    LIVE = 'live'
    COMP = 'comp'
    STATUS = 'status'
    VOD = 'vod'
    SYMLINK = 'symlink'
    STAT = 'stat'
    BUCKET_INFO = 'bucketInfo'
    PROCESS = 'x-oss-process'

    def __init__(self, auth, endpoint, bucket_name,
                 is_cname=False,
                 session=None,
                 connect_timeout=None,
                 app_name='',
                 enable_crc=False,
                 loop=None):
        super().__init__(auth, endpoint, is_cname, session, connect_timeout,
                         app_name, enable_crc, loop=loop)

        self.bucket_name = bucket_name.strip()

    def sign_url(self, method, key, expires, headers=None, params=None):
        """生成签名URL。

        常见的用法是生成加签的URL以供授信用户下载，如为log.jpg生成一个5分钟后过期的下载链接::

            >>> bucket.sign_url('GET', 'log.jpg', 5 * 60)
            'http://your-bucket.oss-cn-hangzhou.aliyuncs.com/logo.jpg?OSSAccessKeyId=YourAccessKeyId\&Expires=1447178011&Signature=UJfeJgvcypWq6Q%2Bm3IJcSHbvSak%3D'

        :param method: HTTP方法，如'GET'、'PUT'、'DELETE'等
        :type method: str
        :param key: 文件名
        :param expires: 过期时间（单位：秒），链接在当前时间再过expires秒后过期

        :param headers: 需要签名的HTTP头部，如名称以x-oss-meta-开头的头部（作为用户自定义元数据）、
            Content-Type头部等。对于下载，不需要填。
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :param params: 需要签名的HTTP查询参数

        :return: 签名URL。
        """
        key = to_string(key)
        req = http.Request(method, self._make_url(self.bucket_name, key),
                           headers=headers,
                           params=params)
        return self.auth._sign_url(req, self.bucket_name, key, expires)

    def sign_rtmp_url(self, channel_name, playlist_name, expires):
        """生成RTMP推流的签名URL。
        常见的用法是生成加签的URL以供授信用户向OSS推RTMP流。

        :param channel_name: 直播频道的名称
        :param expires: 过期时间（单位：秒），链接在当前时间再过expires秒后过期
        :param playlist_name: 播放列表名称，注意与创建live channel时一致
        :param params: 需要签名的HTTP查询参数

        :return: 签名URL。
        """
        url = self._make_url(self.bucket_name, 'live').replace(
            'http://', 'rtmp://').replace('https://',
                                          'rtmp://') + '/' + channel_name
        params = {}
        params['playlistName'] = playlist_name
        return self.auth._sign_rtmp_url(url, self.bucket_name, channel_name, playlist_name, expires, params)

    async def list_objects(self, prefix='', delimiter='', marker='', max_keys=100):
        """根据前缀罗列Bucket里的文件。

        :param str prefix: 只罗列文件名为该前缀的文件
        :param str delimiter: 分隔符。可以用来模拟目录
        :param str marker: 分页标志。首次调用传空串，后续使用返回值的next_marker
        :param int max_keys: 最多返回文件的个数，文件和目录的和不能超过该值

        :return: :class:`ListObjectsResult <oss2.models.ListObjectsResult>`
        """
        resp = await self.__do_object('GET', '',
                                      params={'prefix': prefix,
                                              'delimiter': delimiter,
                                              'marker': marker,
                                              'max-keys': str(max_keys),
                                              'encoding-type': 'url'})
        return await self._parse_result(resp, xml_utils.parse_list_objects, models.ListObjectsResult)

    async def put_object(self, key, data,
                         headers=None,
                         progress_callback=None):
        """上传一个普通文件。

        用法 ::
            >>> bucket.put_object('readme.txt', 'content of readme.txt')
            >>> with open(u'local_file.txt', 'rb') as f:
            >>>     bucket.put_object('remote_file.txt', f)

        :param key: 上传到OSS的文件名

        :param data: 待上传的内容。
        :type data: bytes，str或file-like object

        :param headers: 用户指定的HTTP头部。可以指定Content-Type、Content-MD5、x-oss-meta-开头的头部等
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :param progress_callback: 用户指定的进度回调函数。可以用来实现进度条等功能。参考 :ref:`progress_callback` 。

        :return: :class:`PutObjectResult <oss2.models.PutObjectResult>`
        """
        headers = utils.set_content_type(http.CaseInsensitiveDict(headers), key)

        if progress_callback:
            data = utils.make_progress_adapter(data, progress_callback)

        if self.enable_crc:
            data = utils.make_crc_adapter(data)

        resp = await self.__do_object('PUT', key, data=data, headers=headers)
        result = models.PutObjectResult(resp)

        if self.enable_crc and result.crc is not None:
            utils.check_crc('put object', data.crc, result.crc, result.request_id)
        return result

    async def put_object_from_file(self, key, filename,
                                   headers=None,
                                   progress_callback=None):
        """上传一个本地文件到OSS的普通文件。

        :param str key: 上传到OSS的文件名
        :param str filename: 本地文件名，需要有可读权限

        :param headers: 用户指定的HTTP头部。可以指定Content-Type、Content-MD5、x-oss-meta-开头的头部等
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :param progress_callback: 用户指定的进度回调函数。参考 :ref:`progress_callback`

        :return: :class:`PutObjectResult <oss2.models.PutObjectResult>`
        """
        headers = utils.set_content_type(http.CaseInsensitiveDict(headers), filename)

        with open(to_unicode(filename), 'rb') as f:
            return await self.put_object(key, f, headers=headers, progress_callback=progress_callback)

    async def append_object(self, key, position, data,
                            headers=None,
                            progress_callback=None,
                            init_crc=None):
        """追加上传一个文件。

        :param str key: 新的文件名，或已经存在的可追加文件名
        :param int position: 追加上传一个新的文件， `position` 设为0；追加一个已经存在的可追加文件， `position` 设为文件的当前长度。
            `position` 可以从上次追加的结果 `AppendObjectResult.next_position` 中获得。

        :param data: 用户数据
        :type data: str、bytes、file-like object或可迭代对象

        :param headers: 用户指定的HTTP头部。可以指定Content-Type、Content-MD5、x-oss-开头的头部等
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :param progress_callback: 用户指定的进度回调函数。参考 :ref:`progress_callback`

        :return: :class:`AppendObjectResult <oss2.models.AppendObjectResult>`

        :raises: 如果 `position` 和当前文件长度不一致，抛出 :class:`PositionNotEqualToLength <oss2.exceptions.PositionNotEqualToLength>` ；
                 如果当前文件不是可追加类型，抛出 :class:`ObjectNotAppendable <oss2.exceptions.ObjectNotAppendable>` ；
                 还会抛出其他一些异常
        """
        headers = utils.set_content_type(http.CaseInsensitiveDict(headers), key)

        if progress_callback:
            data = utils.make_progress_adapter(data, progress_callback)

        if self.enable_crc and init_crc is not None:
            data = utils.make_crc_adapter(data, init_crc)

        resp = await self.__do_object('POST', key,
                                      data=data,
                                      headers=headers,
                                      params={'append': '', 'position': str(position)})
        result = models.AppendObjectResult(resp)

        if self.enable_crc and result.crc is not None and init_crc is not None:
            utils.check_crc('append', data.crc, result.crc)

        return result

    async def get_object(self, key,
                         byte_range=None,
                         headers=None,
                         progress_callback=None,
                         process=None,
                         params=None):
        """下载一个文件。

        用法 ::

            >>> result = await bucket.get_object('readme.txt')
            >>> print(result.read())
            'hello world'

        :param key: 文件名
        :param byte_range: 指定下载范围。参见 :ref:`byte_range`

        :param headers: HTTP头部
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :param progress_callback: 用户指定的进度回调函数。参考 :ref:`progress_callback`

        :param process: oss文件处理，如图像服务等。指定后process，返回的内容为处理后的文件。

        :return: file-like object

        :raises: 如果文件不存在，则抛出 :class:`NoSuchKey <oss2.exceptions.NoSuchKey>` ；还可能抛出其他异常
        """
        headers = http.CaseInsensitiveDict(headers)

        range_string = _make_range_string(byte_range)
        if range_string:
            headers['range'] = range_string

        params = {} if params is None else params
        if process:
            params.update({Bucket.PROCESS: process})

        resp = await self.__do_object('GET', key, headers=headers, params=params)
        return models.GetObjectResult(resp, progress_callback, self.enable_crc)

    async def get_object_to_file(self, key, filename,
                                 byte_range=None,
                                 headers=None,
                                 progress_callback=None,
                                 process=None):
        """下载一个文件到本地文件。

        :param key: 文件名
        :param filename: 本地文件名。要求父目录已经存在，且有写权限。
        :param byte_range: 指定下载范围。参见 :ref:`byte_range`

        :param headers: HTTP头部
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :param progress_callback: 用户指定的进度回调函数。参考 :ref:`progress_callback`

        :param process: oss文件处理，如图像服务等。指定后process，返回的内容为处理后的文件。

        :return: 如果文件不存在，则抛出 :class:`NoSuchKey <oss2.exceptions.NoSuchKey>` ；还可能抛出其他异常
        """
        with open(to_unicode(filename), 'wb') as f:
            result = await self.get_object(key, byte_range=byte_range, headers=headers,
                                           progress_callback=progress_callback,
                                           process=process)

            if result.content_length is None:
                shutil.copyfileobj(result, f)
            else:
                utils.copyfileobj_and_verify(result, f, result.content_length, request_id=result.request_id)

            return result

    async def head_object(self, key, headers=None):
        """获取文件元信息。

        HTTP响应的头部包含了文件元信息，可以通过 `RequestResult` 的 `headers` 成员获得。
        用法 ::

            >>> result = await bucket.head_object('readme.txt')
            >>> print(result.content_type)
            text/plain

        :param key: 文件名

        :param headers: HTTP头部
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :return: :class:`HeadObjectResult <oss2.models.HeadObjectResult>`

        :raises: 如果Bucket不存在或者Object不存在，则抛出 :class:`NotFound <oss2.exceptions.NotFound>`
        """
        resp = await self.__do_object('HEAD', key, headers=headers)
        return models.HeadObjectResult(resp)

    async def get_object_meta(self, key):
        """获取文件基本元信息，包括该Object的ETag、Size（文件大小）、LastModified，并不返回其内容。

        HTTP响应的头部包含了文件基本元信息，可以通过 `GetObjectMetaResult` 的 `last_modified`，`content_length`,`etag` 成员获得。

        :param key: 文件名

        :return: :class:`GetObjectMetaResult <oss2.models.GetObjectMetaResult>`

        :raises: 如果文件不存在，则抛出 :class:`NoSuchKey <oss2.exceptions.NoSuchKey>` ；还可能抛出其他异常
        """
        resp = await self.__do_object('GET', key, params={'objectMeta': ''})
        return models.GetObjectMetaResult(resp)

    async def object_exists(self, key):
        """如果文件存在就返回True，否则返回False。如果Bucket不存在，或是发生其他错误，则抛出异常。"""

        # 如果我们用head_object来实现的话，由于HTTP HEAD请求没有响应体，只有响应头部，这样当发生404时，
        # 我们无法区分是NoSuchBucket还是NoSuchKey错误。
        #
        # 2.2.0之前的实现是通过get_object的if-modified-since头部，把date设为当前时间24小时后，这样如果文件存在，则会返回
        # 304 (NotModified)；不存在，则会返回NoSuchKey。get_object会受回源的影响，如果配置会404回源，get_object会判断错误。
        #
        # 目前的实现是通过get_object_meta判断文件是否存在。

        try:
            await self.get_object_meta(key)
        except exceptions.NoSuchKey:
            return False

        return True

    async def copy_object(self, source_bucket_name, source_key, target_key, headers=None):
        """拷贝一个文件到当前Bucket。

        :param str source_bucket_name: 源Bucket名
        :param str source_key: 源文件名
        :param str target_key: 目标文件名

        :param headers: HTTP头部
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :return: :class:`PutObjectResult <oss2.models.PutObjectResult>`
        """
        headers = http.CaseInsensitiveDict(headers)
        headers['x-oss-copy-source'] = '/' + source_bucket_name + '/' + urlquote(source_key, '')

        resp = await self.__do_object('PUT', target_key, headers=headers)

        return models.PutObjectResult(resp)

    async def update_object_meta(self, key, headers):
        """更改Object的元数据信息，包括Content-Type这类标准的HTTP头部，以及以x-oss-meta-开头的自定义元数据。

        用户可以通过 :func:`head_object` 获得元数据信息。

        :param str key: 文件名

        :param headers: HTTP头部，包含了元数据信息
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :return: :class:`RequestResult <oss2.models.RequestResults>`
        """
        return await self.copy_object(self.bucket_name, key, key, headers=headers)

    async def delete_object(self, key):
        """删除一个文件。

        :param str key: 文件名

        :return: :class:`RequestResult <oss2.models.RequestResult>`
        """
        resp = await self.__do_object('DELETE', key)
        return models.RequestResult(resp)

    async def restore_object(self, key):
        """restore an object
            如果是第一次针对该object调用接口，返回RequestResult.status = 202；
            如果已经成功调用过restore接口，且服务端仍处于解冻中，抛异常RestoreAlreadyInProgress(status=409)
            如果已经成功调用过restore接口，且服务端解冻已经完成，再次调用时返回RequestResult.status = 200，且会将object的可下载时间延长一天，最多延长7天。
            如果object不存在，则抛异常NoSuchKey(status=404)；
            对非Archive类型的Object提交restore，则抛异常OperationNotSupported(status=400)

            也可以通过调用head_object接口来获取meta信息来判断是否可以restore与restore的状态
            代码示例::
            >>> meta = await bucket.head_object(key)
            >>> if meta.resp.headers['x-oss-storage-class'] == oss2.BUCKET_STORAGE_CLASS_ARCHIVE:
            >>>     bucket.restore_object(key)
            >>>         while True:
            >>>             meta = await bucket.head_object(key)
            >>>             if meta.resp.headers['x-oss-restore'] == 'ongoing-request="true"':
            >>>                 time.sleep(5)
            >>>             else:
            >>>                 break
        :param str key: object name
        :return: :class:`RequestResult <oss2.models.RequestResult>`
        """
        resp = await self.__do_object('POST', key, params={'restore': ''})
        return models.RequestResult(resp)

    async def put_object_acl(self, key, permission):
        """设置文件的ACL。

        :param str key: 文件名
        :param str permission: 可以是oss2.OBJECT_ACL_DEFAULT、oss2.OBJECT_ACL_PRIVATE、oss2.OBJECT_ACL_PUBLIC_READ或
            oss2.OBJECT_ACL_PUBLIC_READ_WRITE。

        :return: :class:`RequestResult <oss2.models.RequestResult>`
        """
        resp = await self.__do_object('PUT', key, params={'acl': ''}, headers={'x-oss-object-acl': permission})
        return models.RequestResult(resp)

    async def get_object_acl(self, key):
        """获取文件的ACL。

        :return: :class:`GetObjectAclResult <oss2.models.GetObjectAclResult>`
        """
        resp = await self.__do_object('GET', key, params={'acl': ''})
        return await self._parse_result(resp, xml_utils.parse_get_object_acl, models.GetObjectAclResult)

    async def batch_delete_objects(self, key_list):
        """批量删除文件。待删除文件列表不能为空。

        :param key_list: 文件名列表，不能为空。
        :type key_list: list of str

        :return: :class:`BatchDeleteObjectsResult <oss2.models.BatchDeleteObjectsResult>`
        """
        if not key_list:
            raise models.ClientError('key_list should not be empty')

        data = xml_utils.to_batch_delete_objects_request(key_list, False)
        resp = await self.__do_object('POST', '',
                                      data=data,
                                      params={'delete': '', 'encoding-type': 'url'},
                                      headers={'Content-MD5': utils.content_md5(data)})
        return await self._parse_result(resp, xml_utils.parse_batch_delete_objects, models.BatchDeleteObjectsResult)

    async def init_multipart_upload(self, key, headers=None):
        """初始化分片上传。

        返回值中的 `upload_id` 以及Bucket名和Object名三元组唯一对应了此次分片上传事件。

        :param str key: 待上传的文件名

        :param headers: HTTP头部
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :return: :class:`InitMultipartUploadResult <oss2.models.InitMultipartUploadResult>`
        """
        headers = utils.set_content_type(http.CaseInsensitiveDict(headers), key)

        resp = await self.__do_object('POST', key, params={'uploads': ''}, headers=headers)
        return await self._parse_result(resp, xml_utils.parse_init_multipart_upload, models.InitMultipartUploadResult)

    async def upload_part(self, key, upload_id, part_number, data, progress_callback=None, headers=None):
        """上传一个分片。

        :param str key: 待上传文件名，这个文件名要和 :func:`init_multipart_upload` 的文件名一致。
        :param str upload_id: 分片上传ID
        :param int part_number: 分片号，最小值是1.
        :param data: 待上传数据。
        :param progress_callback: 用户指定进度回调函数。可以用来实现进度条等功能。参考 :ref:`progress_callback` 。

        :param headers: 用户指定的HTTP头部。可以指定Content-MD5头部等
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :return: :class:`PutObjectResult <oss2.models.PutObjectResult>`
        """
        if progress_callback:
            data = utils.make_progress_adapter(data, progress_callback)

        if self.enable_crc:
            data = utils.make_crc_adapter(data)

        resp = await self.__do_object('PUT', key,
                                      params={'uploadId': upload_id, 'partNumber': str(part_number)},
                                      headers=headers,
                                      data=data)
        result = models.PutObjectResult(resp)

        if self.enable_crc and result.crc is not None:
            utils.check_crc('put', data.crc, result.crc)

        return result

    async def complete_multipart_upload(self, key, upload_id, parts, headers=None):
        """完成分片上传，创建文件。

        :param str key: 待上传的文件名，这个文件名要和 :func:`init_multipart_upload` 的文件名一致。
        :param str upload_id: 分片上传ID

        :param parts: PartInfo列表。PartInfo中的part_number和etag是必填项。其中的etag可以从 :func:`upload_part` 的返回值中得到。
        :type parts: list of `PartInfo <oss2.models.PartInfo>`

        :param headers: HTTP头部
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :return: :class:`PutObjectResult <oss2.models.PutObjectResult>`
        """
        data = xml_utils.to_complete_upload_request(sorted(parts, key=lambda p: p.part_number))
        resp = await self.__do_object('POST', key,
                                      params={'uploadId': upload_id},
                                      data=data,
                                      headers=headers)

        return models.PutObjectResult(resp)

    async def abort_multipart_upload(self, key, upload_id):
        """取消分片上传。

        :param str key: 待上传的文件名，这个文件名要和 :func:`init_multipart_upload` 的文件名一致。
        :param str upload_id: 分片上传ID

        :return: :class:`RequestResult <oss2.models.RequestResult>`
        """
        resp = await self.__do_object('DELETE', key,
                                      params={'uploadId': upload_id})
        return models.RequestResult(resp)

    async def list_multipart_uploads(self,
                                     prefix='',
                                     delimiter='',
                                     key_marker='',
                                     upload_id_marker='',
                                     max_uploads=1000):
        """罗列正在进行中的分片上传。支持分页。

        :param str prefix: 只罗列匹配该前缀的文件的分片上传
        :param str delimiter: 目录分割符
        :param str key_marker: 文件名分页符。第一次调用可以不传，后续设为返回值中的 `next_key_marker`
        :param str upload_id_marker: 分片ID分页符。第一次调用可以不传，后续设为返回值中的 `next_upload_id_marker`
        :param int max_uploads: 一次罗列最多能够返回的条目数

        :return: :class:`ListMultipartUploadsResult <oss2.models.ListMultipartUploadsResult>`
        """
        resp = await self.__do_object('GET', '',
                                      params={'uploads': '',
                                              'prefix': prefix,
                                              'delimiter': delimiter,
                                              'key-marker': key_marker,
                                              'upload-id-marker': upload_id_marker,
                                              'max-uploads': str(max_uploads),
                                              'encoding-type': 'url'})
        return await self._parse_result(resp, xml_utils.parse_list_multipart_uploads, models.ListMultipartUploadsResult)

    async def upload_part_copy(self, source_bucket_name, source_key, byte_range,
                               target_key, target_upload_id, target_part_number,
                               headers=None):
        """分片拷贝。把一个已有文件的一部分或整体拷贝成目标文件的一个分片。

        :param byte_range: 指定待拷贝内容在源文件里的范围。参见 :ref:`byte_range`

        :param headers: HTTP头部
        :type headers: 可以是dict，建议是oss2.CaseInsensitiveDict

        :return: :class:`PutObjectResult <oss2.models.PutObjectResult>`
        """
        headers = http.CaseInsensitiveDict(headers)
        headers['x-oss-copy-source'] = '/' + source_bucket_name + '/' + source_key

        range_string = _make_range_string(byte_range)
        if range_string:
            headers['x-oss-copy-source-range'] = range_string

        resp = await self.__do_object('PUT', target_key,
                                      params={'uploadId': target_upload_id,
                                              'partNumber': str(target_part_number)},
                                      headers=headers)

        return models.PutObjectResult(resp)

    async def list_parts(self, key, upload_id,
                         marker='', max_parts=1000):
        """列举已经上传的分片。支持分页。

        :param str key: 文件名
        :param str upload_id: 分片上传ID
        :param str marker: 分页符
        :param int max_parts: 一次最多罗列多少分片

        :return: :class:`ListPartsResult <oss2.models.ListPartsResult>`
        """
        resp = await self.__do_object('GET', key,
                                      params={'uploadId': upload_id,
                                              'part-number-marker': marker,
                                              'max-parts': str(max_parts)})
        return await self._parse_result(resp, xml_utils.parse_list_parts, models.ListPartsResult)

    async def put_symlink(self, target_key, symlink_key, headers=None):
        """创建Symlink。

        :param str target_key: 目标文件，目标文件不能为符号连接
        :param str symlink_key: 符号连接类文件，其实质是一个特殊的文件，数据指向目标文件

        :return: :class:`RequestResult <oss2.models.RequestResult>`
        """
        headers = headers or {}
        headers['x-oss-symlink-target'] = urlquote(target_key, '')
        resp = await self.__do_object('PUT', symlink_key, headers=headers, params={Bucket.SYMLINK: ''})
        return models.RequestResult(resp)

    async def get_symlink(self, symlink_key):
        """获取符号连接文件的目标文件。

        :param str symlink_key: 符号连接类文件

        :return: :class:`GetSymlinkResult <oss2.models.GetSymlinkResult>`

        :raises: 如果文件的符号链接不存在，则抛出 :class:`NoSuchKey <oss2.exceptions.NoSuchKey>` ；还可能抛出其他异常
        """
        resp = await self.__do_object('GET', symlink_key, params={Bucket.SYMLINK: ''})
        return models.GetSymlinkResult(resp)

    async def create_bucket(self, permission=None, input=None):
        """创建新的Bucket。

        :param str permission: 指定Bucket的ACL。可以是oss2.BUCKET_ACL_PRIVATE（推荐、缺省）、oss2.BUCKET_ACL_PUBLIC_READ或是
            oss2.BUCKET_ACL_PUBLIC_READ_WRITE。

        :param input: :class:`BucketCreateConfig <oss2.models.BucketCreateConfig>` object
        """
        if permission:
            headers = {'x-oss-acl': permission}
        else:
            headers = None

        data = self.__convert_data(models.BucketCreateConfig, xml_utils.to_put_bucket_config, input)
        resp = await self.__do_bucket('PUT', headers=headers, data=data)
        return models.RequestResult(resp)

    async def delete_bucket(self):
        """删除一个Bucket。只有没有任何文件，也没有任何未完成的分片上传的Bucket才能被删除。

        :return: :class:`RequestResult <oss2.models.RequestResult>`

        ":raises: 如果试图删除一个非空Bucket，则抛出 :class:`BucketNotEmpty <oss2.exceptions.BucketNotEmpty>`
        """
        resp = await self.__do_bucket('DELETE')
        return models.RequestResult(resp)

    async def put_bucket_acl(self, permission):
        """设置Bucket的ACL。

        :param str permission: 新的ACL，可以是oss2.BUCKET_ACL_PRIVATE、oss2.BUCKET_ACL_PUBLIC_READ或
            oss2.BUCKET_ACL_PUBLIC_READ_WRITE
        """
        resp = await self.__do_bucket('PUT', headers={'x-oss-acl': permission}, params={Bucket.ACL: ''})
        return models.RequestResult(resp)

    async def get_bucket_acl(self):
        """获取Bucket的ACL。

        :return: :class:`GetBucketAclResult <oss2.models.GetBucketAclResult>`
        """
        resp = await self.__do_bucket('GET', params={Bucket.ACL: ''})
        return await self._parse_result(resp, xml_utils.parse_get_bucket_acl, models.GetBucketAclResult)

    async def put_bucket_cors(self, input):
        """设置Bucket的CORS。

        :param input: :class:`BucketCors <oss2.models.BucketCors>` 对象或其他
        """
        data = self.__convert_data(models.BucketCors, xml_utils.to_put_bucket_cors, input)
        resp = await self.__do_bucket('PUT', data=data, params={Bucket.CORS: ''})
        return models.RequestResult(resp)

    async def get_bucket_cors(self):
        """获取Bucket的CORS配置。

        :return: :class:`GetBucketCorsResult <oss2.models.GetBucketCorsResult>`
        """
        resp = await self.__do_bucket('GET', params={Bucket.CORS: ''})
        return await self._parse_result(resp, xml_utils.parse_get_bucket_cors, models.GetBucketCorsResult)

    async def delete_bucket_cors(self):
        """删除Bucket的CORS配置。"""
        resp = await self.__do_bucket('DELETE', params={Bucket.CORS: ''})
        return models.RequestResult(resp)

    async def put_bucket_lifecycle(self, input):
        """设置生命周期管理的配置。

        :param input: :class:`BucketLifecycle <oss2.models.BucketLifecycle>` 对象或其他
        """
        data = self.__convert_data(models.BucketLifecycle, xml_utils.to_put_bucket_lifecycle, input)
        resp = await self.__do_bucket('PUT', data=data, params={Bucket.LIFECYCLE: ''})
        return models.RequestResult(resp)

    async def get_bucket_lifecycle(self):
        """获取生命周期管理配置。

        :return: :class:`GetBucketLifecycleResult <oss2.models.GetBucketLifecycleResult>`

        :raises: 如果没有设置Lifecycle，则抛出 :class:`NoSuchLifecycle <oss2.exceptions.NoSuchLifecycle>`
        """
        resp = await self.__do_bucket('GET', params={Bucket.LIFECYCLE: ''})
        return await self._parse_result(resp, xml_utils.parse_get_bucket_lifecycle, models.GetBucketLifecycleResult)

    async def delete_bucket_lifecycle(self):
        """删除生命周期管理配置。如果Lifecycle没有设置，也返回成功。"""
        resp = await self.__do_bucket('DELETE', params={Bucket.LIFECYCLE: ''})
        return models.RequestResult(resp)

    async def get_bucket_location(self):
        """获取Bucket的数据中心。

        :return: :class:`GetBucketLocationResult <oss2.models.GetBucketLocationResult>`
        """
        resp = await self.__do_bucket('GET', params={Bucket.LOCATION: ''})
        return await self._parse_result(resp, xml_utils.parse_get_bucket_location, models.GetBucketLocationResult)

    async def put_bucket_logging(self, input):
        """设置Bucket的访问日志功能。

        :param input: :class:`BucketLogging <oss2.models.BucketLogging>` 对象或其他
        """
        data = self.__convert_data(models.BucketLogging, xml_utils.to_put_bucket_logging, input)
        resp = await self.__do_bucket('PUT', data=data, params={Bucket.LOGGING: ''})
        return models.RequestResult(resp)

    async def get_bucket_logging(self):
        """获取Bucket的访问日志功能配置。

        :return: :class:`GetBucketLoggingResult <oss2.models.GetBucketLoggingResult>`
        """
        resp = await self.__do_bucket('GET', params={Bucket.LOGGING: ''})
        return await self._parse_result(resp, xml_utils.parse_get_bucket_logging, models.GetBucketLoggingResult)

    async def delete_bucket_logging(self):
        """关闭Bucket的访问日志功能。"""
        resp = await self.__do_bucket('DELETE', params={Bucket.LOGGING: ''})
        return models.RequestResult(resp)

    async def put_bucket_referer(self, input):
        """为Bucket设置防盗链。

        :param input: :class:`BucketReferer <oss2.models.BucketReferer>` 对象或其他
        """
        data = self.__convert_data(models.BucketReferer, xml_utils.to_put_bucket_referer, input)
        resp = await self.__do_bucket('PUT', data=data, params={Bucket.REFERER: ''})
        return models.RequestResult(resp)

    async def get_bucket_referer(self):
        """获取Bucket的防盗链配置。

        :return: :class:`GetBucketRefererResult <oss2.models.GetBucketRefererResult>`
        """
        resp = await self.__do_bucket('GET', params={Bucket.REFERER: ''})
        return await self._parse_result(resp, xml_utils.parse_get_bucket_referer, models.GetBucketRefererResult)

    async def get_bucket_stat(self):
        """查看Bucket的状态，目前包括bucket大小，bucket的object数量，bucket正在上传的Multipart Upload事件个数等。

        :return: :class:`GetBucketStatResult <oss2.models.GetBucketStatResult>`
        """
        resp = await self.__do_bucket('GET', params={Bucket.STAT: ''})
        return await self._parse_result(resp, xml_utils.parse_get_bucket_stat, models.GetBucketStatResult)

    async def get_bucket_info(self):
        """获取bucket相关信息，如创建时间，访问Endpoint，Owner与ACL等。

        :return: :class:`GetBucketInfoResult <oss2.models.GetBucketInfoResult>`
        """
        resp = await self.__do_bucket('GET', params={Bucket.BUCKET_INFO: ''})
        return await self._parse_result(resp, xml_utils.parse_get_bucket_info, models.GetBucketInfoResult)

    async def put_bucket_website(self, input):
        """为Bucket配置静态网站托管功能。

        :param input: :class:`BucketWebsite <oss2.models.BucketWebsite>`
        """
        data = self.__convert_data(models.BucketWebsite, xml_utils.to_put_bucket_website, input)
        resp = await self.__do_bucket('PUT', data=data, params={Bucket.WEBSITE: ''})
        return models.RequestResult(resp)

    async def get_bucket_website(self):
        """获取Bucket的静态网站托管配置。

        :return: :class:`GetBucketWebsiteResult <oss2.models.GetBucketWebsiteResult>`

        :raises: 如果没有设置静态网站托管，那么就抛出 :class:`NoSuchWebsite <oss2.exceptions.NoSuchWebsite>`
        """
        resp = await self.__do_bucket('GET', params={Bucket.WEBSITE: ''})
        return await self._parse_result(resp, xml_utils.parse_get_bucket_websiste, models.GetBucketWebsiteResult)

    async def delete_bucket_website(self):
        """关闭Bucket的静态网站托管功能。"""
        resp = await self.__do_bucket('DELETE', params={Bucket.WEBSITE: ''})
        return models.RequestResult(resp)

    async def create_live_channel(self, channel_name, input):
        """创建推流直播频道

        :param str channel_name: 要创建的live channel的名称
        :param input: LiveChannelInfo类型，包含了live channel中的描述信息

        :return: :class:`CreateLiveChannelResult <oss2.models.CreateLiveChannelResult>`
        """
        data = self.__convert_data(models.LiveChannelInfo, xml_utils.to_create_live_channel, input)
        resp = await self.__do_object('PUT', channel_name, data=data, params={Bucket.LIVE: ''})
        return await self._parse_result(resp, xml_utils.parse_create_live_channel, models.CreateLiveChannelResult)

    async def delete_live_channel(self, channel_name):
        """删除推流直播频道

        :param str channel_name: 要删除的live channel的名称
        """
        resp = await self.__do_object('DELETE', channel_name, params={Bucket.LIVE: ''})
        return models.RequestResult(resp)

    async def get_live_channel(self, channel_name):
        """获取直播频道配置

        :param str channel_name: 要获取的live channel的名称

        :return: :class:`GetLiveChannelResult <oss2.models.GetLiveChannelResult>`
        """
        resp = await self.__do_object('GET', channel_name, params={Bucket.LIVE: ''})
        return await self._parse_result(resp, xml_utils.parse_get_live_channel, models.GetLiveChannelResult)

    async def list_live_channel(self, prefix='', marker='', max_keys=100):
        """列举出Bucket下所有符合条件的live channel

        param: str prefix: list时channel_id的公共前缀
        param: str marker: list时指定的起始标记
        param: int max_keys: 本次list返回live channel的最大个数

        return: :class:`ListLiveChannelResult <oss2.models.ListLiveChannelResult>`
        """
        resp = await self.__do_bucket('GET', params={Bucket.LIVE: '',
                                                     'prefix': prefix,
                                                     'marker': marker,
                                                     'max-keys': str(max_keys)})
        return await self._parse_result(resp, xml_utils.parse_list_live_channel, models.ListLiveChannelResult)

    async def get_live_channel_stat(self, channel_name):
        """获取live channel当前推流的状态

        param str channel_name: 要获取推流状态的live channel的名称

        return: :class:`GetLiveChannelStatResult <oss2.models.GetLiveChannelStatResult>`
        """
        resp = await self.__do_object('GET', channel_name, params={Bucket.LIVE: '', Bucket.COMP: 'stat'})
        return await self._parse_result(resp, xml_utils.parse_live_channel_stat, models.GetLiveChannelStatResult)

    async def put_live_channel_status(self, channel_name, status):
        """更改live channel的status，仅能在“enabled”和“disabled”两种状态中更改

        param str channel_name: 要更改status的live channel的名称
        param str status: live channel的目标status
        """
        resp = await self.__do_object('PUT', channel_name, params={Bucket.LIVE: '', Bucket.STATUS: status})
        return models.RequestResult(resp)

    async def get_live_channel_history(self, channel_name):
        """获取live channel中最近的最多十次的推流记录，记录中包含推流的起止时间和远端的地址

        param str channel_name: 要获取最近推流记录的live channel的名称

        return: :class:`GetLiveChannelHistoryResult <oss2.models.GetLiveChannelHistoryResult>`
        """
        resp = await self.__do_object('GET', channel_name, params={Bucket.LIVE: '', Bucket.COMP: 'history'})
        return await self._parse_result(resp, xml_utils.parse_live_channel_history, models.GetLiveChannelHistoryResult)

    async def post_vod_playlist(self, channel_name, playlist_name, start_time=0, end_time=0):
        """根据指定的playlist name以及startTime和endTime生成一个点播的播放列表

        param str channel_name: 要生成点播列表的live channel的名称
        param str playlist_name: 要生成点播列表m3u8文件的名称
        param int start_time: 点播的起始时间，Unix Time格式，可以使用int(time.time())获取
        param int end_time: 点播的结束时间，Unix Time格式，可以使用int(time.time())获取
        """
        key = channel_name + "/" + playlist_name
        resp = await self.__do_object('POST', key, params={Bucket.VOD: '',
                                                           'startTime': str(start_time),
                                                           'endTime': str(end_time)})
        return models.RequestResult(resp)

    async def _get_bucket_config(self, config):
        """获得Bucket某项配置，具体哪种配置由 `config` 指定。该接口直接返回 `RequestResult` 对象。
        通过read()接口可以获得XML字符串。不建议使用。

        :param str config: 可以是 `Bucket.ACL` 、 `Bucket.LOGGING` 等。

        :return: :class:`RequestResult <oss2.models.RequestResult>`
        """
        return await self.__do_bucket('GET', params={config: ''})

    async def __do_object(self, method, key, **kwargs):
        return await self._do(method, self.bucket_name, key, **kwargs)

    async def __do_bucket(self, method, **kwargs):
        return await self._do(method, self.bucket_name, '', **kwargs)

    def __convert_data(self, klass, converter, data):
        if isinstance(data, klass):
            return converter(data)
        else:
            return data


def _normalize_endpoint(endpoint):
    if not endpoint.startswith('http://') and not endpoint.startswith('https://'):
        return 'http://' + endpoint
    else:
        return endpoint


_ENDPOINT_TYPE_ALIYUN = 0
_ENDPOINT_TYPE_CNAME = 1
_ENDPOINT_TYPE_IP = 2


def _make_range_string(range):
    if range is None:
        return ''

    start = range[0]
    last = range[1]

    if start is None and last is None:
        return ''

    return 'bytes=' + _range(start, last)


def _range(start, last):
    def to_str(pos):
        if pos is None:
            return ''
        else:
            return str(pos)

    return to_str(start) + '-' + to_str(last)


def _determine_endpoint_type(netloc, is_cname, bucket_name):
    if utils.is_ip_or_localhost(netloc):
        return _ENDPOINT_TYPE_IP

    if is_cname:
        return _ENDPOINT_TYPE_CNAME

    if utils.is_valid_bucket_name(bucket_name):
        return _ENDPOINT_TYPE_ALIYUN
    else:
        return _ENDPOINT_TYPE_IP


class _UrlMaker(object):
    def __init__(self, endpoint, is_cname):
        p = urlparse(endpoint)

        self.scheme = p.scheme
        self.netloc = p.netloc
        self.is_cname = is_cname

    def __call__(self, bucket_name, key):
        self.type = _determine_endpoint_type(self.netloc, self.is_cname, bucket_name)
        key = urlquote(key, '')

        if self.type == _ENDPOINT_TYPE_CNAME:
            return '{0}://{1}/{2}'.format(self.scheme, self.netloc, key)

        if self.type == _ENDPOINT_TYPE_IP:
            if bucket_name:
                return '{0}://{1}/{2}/{3}'.format(self.scheme, self.netloc, bucket_name, key)
            else:
                return '{0}://{1}/{2}'.format(self.scheme, self.netloc, key)
        if not bucket_name:
            assert not key
            return '{0}://{1}'.format(self.scheme, self.netloc)

        return '{0}://{1}.{2}/{3}'.format(self.scheme, bucket_name, self.netloc, key)
