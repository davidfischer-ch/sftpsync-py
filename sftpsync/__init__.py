# -*- encoding: utf-8 -*-

import logging, os, re, socket
from datetime import datetime
from stat import S_ISDIR

import paramiko

MTIME_TOLERANCE = 3

logger = logging.getLogger(__name__)


class AuthenticationError(Exception):
    pass


class TimeoutError(Exception):
    pass


class SSHError(Exception):
    pass


class SFTP(object):

    def __init__(self, host, username, password=None, port=22, timeout=10, max_attempts=3, **kwargs):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.sftp = None
        for i in range(max_attempts):
            try:
                self.client.connect(host, port=port, username=username, password=password, timeout=timeout, **kwargs)
                self.sftp = self.client.open_sftp()
                break
            except (paramiko.BadHostKeyException, paramiko.AuthenticationException) as e:
                raise AuthenticationError(str(e))
            except socket.timeout as e:
                raise TimeoutError(str(e))
            except Exception as e:
                if i == max_attempts - 1:
                    raise SSHError(str(e))

    def sync(self, src, dst, download, includes=None, excludes=None, delete=False, dry=False):
        """
        Sync files and directories.
        :param src: source directory
        :param dst: destination directory
        :param download: True to sync from a remote source to a local destination,
            else sync from a local source to a remote destination
        :param includes: list of regular expressions the source files must match
        :param excludes: list of regular expressions the source files must not match
        :param delete: remove destination files and directories not present
            at source or filtered by the include/exclude patterns
        """
        includes = self._get_filters(includes)
        excludes = self._get_filters(excludes)

        if src.endswith('/') != dst.endswith('/'):
            dst = os.path.join(dst, os.path.basename(src.rstrip('/')))
        src = src.rstrip('/')
        re_base = re.compile(r'^%s/' % re.escape(src))
        if not src:
            src = '/'

        self._makedirs_dst(dst, remote=not download, dry=dry)

        started = datetime.utcnow()
        total_size = 0
        dst_list = {'file': [], 'dir': []}

        for src_type, src_file, src_stat in self._walk(src, remote=download):
            file_ = re_base.sub('', src_file)
            if not self._validate_src(file_, includes, excludes):
                logger.debug('skip %s', src_file)
                continue

            dst_file = os.path.join(dst, file_)
            dst_list[src_type].append(dst_file)

            if src_type == 'dir':
                self._makedirs_dst(dst_file, remote=not download, dry=dry)
            elif src_type == 'file':
                if not self._validate_dst(dst_file, src_stat, remote=not download):
                    if not dry:
                        self._save(src_file, dst_file, src_stat, remote=not download)
                    total_size += src_stat.st_size
                    logger.debug('copy %s to %s', src_file, dst_file)
            else:
                raise ValueError(src_type)

        if delete:
            self._delete_dst(dst, dst_list, remote=not download, dry=dry)

        logger.debug('transferred %s bytes in %s', total_size, datetime.utcnow() - started)

    def _delete_dst(self, path, files, remote, dry=False):
        if remote:
            callables = {'file': self.sftp.remove, 'dir': self.sftp.rmdir}
        else:
            callables = {'file': os.remove, 'dir': os.rmdir}

        for type, file, stat in self._walk(path, topdown=False, remote=remote):
            if file not in files[type]:
                logger.debug('remove %s', file)
                if not dry:
                    try:
                        callables[type](file)
                    except Exception as e:
                        logger.debug('failed to remove %s: %s', file, str(e))
                        continue

    def _get_filters(self, filters):
        return [re.compile(f) for f in filters] if filters else []

    def _makedirs_dst(self, path, remote, dry=False):
        if remote:
            paths = []
            while path not in ('/', ''):
                paths.insert(0, path)
                path = os.path.dirname(path)
            for path in paths:
                try:
                    self.sftp.lstat(path)
                except Exception:
                    logger.debug('create destination directory %s', path)
                    if not dry:
                        self.sftp.mkdir(path)
        else:
            if not os.path.exists(path):
                logger.debug('create destination directory %s', path)
                if not dry:
                    os.makedirs(path)

    def _save(self, src, dst, src_stat, remote):
        if remote:
            logger.info('copy %s to %s@%s:%s', src, self.username, self.host, dst)
            self.sftp.put(src, dst)
            self.sftp.utime(dst, (int(src_stat.st_atime), int(src_stat.st_mtime)))
        else:
            logger.info('copy %s@%s:%s to %s', self.username, self.host, src, dst)
            self.sftp.get(src, dst)
            os.utime(dst, (int(src_stat.st_atime), int(src_stat.st_mtime)))

    def _validate_dst(self, file, src_stat, remote):
        if remote:
            try:
                dst_stat = self.sftp.lstat(file)
            except Exception:
                return False
        else:
            if not os.path.exists(file):
                return False
            dst_stat = os.stat(file)
        if abs(dst_stat.st_mtime - src_stat.st_mtime) > MTIME_TOLERANCE:
            logger.debug('%s modification time mismatch (source: %s, destination: %s)', file,
                         datetime.utcfromtimestamp(src_stat.st_mtime), datetime.utcfromtimestamp(dst_stat.st_mtime))
            return False
        if dst_stat.st_size != src_stat.st_size:
            return False
        return True

    def _validate_src(self, file, includes, excludes):
        return not any(e.search(file) for e in excludes) and (not includes or any(i.search(file) for i in includes))

    def _walk(self, path, topdown=True, remote=False):
        method = self._walk_remote if remote else self._walk_local
        return method(path=path, topdown=topdown)

    def _walk_local(self, path, topdown=True):
        for root, dirs, files in os.walk(path, topdown=topdown):
            for file in files:
                file = os.path.join(root, file)
                yield 'file', file, os.stat(file)
            for dir in dirs:
                dir = os.path.join(root, dir)
                yield 'dir', dir, os.stat(dir)

    def _walk_remote(self, path, topdown=True):
        try:
            attributes = self.sftp.listdir_attr(path)
        except IOError:
            attributes = []
        for stat in attributes:
            file = os.path.join(path, stat.filename)
            if not S_ISDIR(stat.st_mode):
                yield 'file', file, stat
            else:
                if topdown:
                    yield 'dir', file, stat
                    yield from self._walk_remote(file, topdown=topdown)
                else:
                    yield from self._walk_remote(file, topdown=topdown)
                    yield 'dir', file, None
