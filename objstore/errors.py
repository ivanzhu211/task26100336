"""异常类型定义。"""


class StoreError(Exception):
    """所有存储模块异常的基类。"""


class QuotaExceeded(StoreError):
    """已用加预占超过账户额度。"""


class ObjectNotFound(StoreError):
    """对象不存在。"""


class SessionNotFound(StoreError):
    """分段会话不存在。"""


class SessionNotOpen(StoreError):
    """会话已提交或已被清理放弃，不能再传段或提交。"""


class SizeMismatch(StoreError):
    """实际数据大小与声明不符。"""


class ObjectTooLarge(StoreError):
    """声明大小超过单对象上限。"""


class InvalidPart(StoreError):
    """段编号非法。"""


class UploadIncomplete(StoreError):
    """提交时段不齐或总大小不符。"""
