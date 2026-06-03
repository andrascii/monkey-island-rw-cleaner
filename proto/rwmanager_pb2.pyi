import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class UserStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACTIVE: _ClassVar[UserStatus]
    DISABLED: _ClassVar[UserStatus]
    LIMITED: _ClassVar[UserStatus]
    EXPIRED: _ClassVar[UserStatus]

class TrafficLimitStrategy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NO_RESET: _ClassVar[TrafficLimitStrategy]
    DAY: _ClassVar[TrafficLimitStrategy]
    WEEK: _ClassVar[TrafficLimitStrategy]
    MONTH: _ClassVar[TrafficLimitStrategy]
ACTIVE: UserStatus
DISABLED: UserStatus
LIMITED: UserStatus
EXPIRED: UserStatus
NO_RESET: TrafficLimitStrategy
DAY: TrafficLimitStrategy
WEEK: TrafficLimitStrategy
MONTH: TrafficLimitStrategy

class UserLastConnectedNode(_message.Message):
    __slots__ = ("connected_at", "node_name")
    CONNECTED_AT_FIELD_NUMBER: _ClassVar[int]
    NODE_NAME_FIELD_NUMBER: _ClassVar[int]
    connected_at: _timestamp_pb2.Timestamp
    node_name: str
    def __init__(self, connected_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., node_name: _Optional[str] = ...) -> None: ...

class ActiveInternalSquad(_message.Message):
    __slots__ = ("uuid", "name")
    UUID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    uuid: str
    name: str
    def __init__(self, uuid: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class HappCrypto(_message.Message):
    __slots__ = ("crypto_link",)
    CRYPTO_LINK_FIELD_NUMBER: _ClassVar[int]
    crypto_link: str
    def __init__(self, crypto_link: _Optional[str] = ...) -> None: ...

class UserActiveInbound(_message.Message):
    __slots__ = ("uuid", "tag", "type", "network", "security")
    UUID_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    NETWORK_FIELD_NUMBER: _ClassVar[int]
    SECURITY_FIELD_NUMBER: _ClassVar[int]
    uuid: str
    tag: str
    type: str
    network: str
    security: str
    def __init__(self, uuid: _Optional[str] = ..., tag: _Optional[str] = ..., type: _Optional[str] = ..., network: _Optional[str] = ..., security: _Optional[str] = ...) -> None: ...

class ErrorInfo(_message.Message):
    __slots__ = ("error_code", "status_code", "description")
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    STATUS_CODE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    error_code: str
    status_code: int
    description: str
    def __init__(self, error_code: _Optional[str] = ..., status_code: _Optional[int] = ..., description: _Optional[str] = ...) -> None: ...

class UserResponse(_message.Message):
    __slots__ = ("uuid", "subscription_uuid", "short_uuid", "username", "status", "used_traffic_bytes", "lifetime_used_traffic_bytes", "traffic_limit_bytes", "traffic_limit_strategy", "sub_last_user_agent", "sub_last_opened_at", "expire_at", "online_at", "sub_revoked_at", "last_traffic_reset_at", "trojan_password", "vless_uuid", "ss_password", "description", "telegram_id", "email", "hwid_device_limit", "subscription_url", "first_connected", "last_trigger_threshold", "happ", "active_internal_squads", "created_at", "updated_at")
    UUID_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIPTION_UUID_FIELD_NUMBER: _ClassVar[int]
    SHORT_UUID_FIELD_NUMBER: _ClassVar[int]
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    USED_TRAFFIC_BYTES_FIELD_NUMBER: _ClassVar[int]
    LIFETIME_USED_TRAFFIC_BYTES_FIELD_NUMBER: _ClassVar[int]
    TRAFFIC_LIMIT_BYTES_FIELD_NUMBER: _ClassVar[int]
    TRAFFIC_LIMIT_STRATEGY_FIELD_NUMBER: _ClassVar[int]
    SUB_LAST_USER_AGENT_FIELD_NUMBER: _ClassVar[int]
    SUB_LAST_OPENED_AT_FIELD_NUMBER: _ClassVar[int]
    EXPIRE_AT_FIELD_NUMBER: _ClassVar[int]
    ONLINE_AT_FIELD_NUMBER: _ClassVar[int]
    SUB_REVOKED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_TRAFFIC_RESET_AT_FIELD_NUMBER: _ClassVar[int]
    TROJAN_PASSWORD_FIELD_NUMBER: _ClassVar[int]
    VLESS_UUID_FIELD_NUMBER: _ClassVar[int]
    SS_PASSWORD_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TELEGRAM_ID_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    HWID_DEVICE_LIMIT_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIPTION_URL_FIELD_NUMBER: _ClassVar[int]
    FIRST_CONNECTED_FIELD_NUMBER: _ClassVar[int]
    LAST_TRIGGER_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    HAPP_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_INTERNAL_SQUADS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    uuid: str
    subscription_uuid: str
    short_uuid: str
    username: str
    status: UserStatus
    used_traffic_bytes: float
    lifetime_used_traffic_bytes: float
    traffic_limit_bytes: int
    traffic_limit_strategy: TrafficLimitStrategy
    sub_last_user_agent: str
    sub_last_opened_at: _timestamp_pb2.Timestamp
    expire_at: _timestamp_pb2.Timestamp
    online_at: _timestamp_pb2.Timestamp
    sub_revoked_at: _timestamp_pb2.Timestamp
    last_traffic_reset_at: _timestamp_pb2.Timestamp
    trojan_password: str
    vless_uuid: str
    ss_password: str
    description: str
    telegram_id: int
    email: str
    hwid_device_limit: int
    subscription_url: str
    first_connected: _timestamp_pb2.Timestamp
    last_trigger_threshold: int
    happ: HappCrypto
    active_internal_squads: _containers.RepeatedCompositeFieldContainer[ActiveInternalSquad]
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, uuid: _Optional[str] = ..., subscription_uuid: _Optional[str] = ..., short_uuid: _Optional[str] = ..., username: _Optional[str] = ..., status: _Optional[_Union[UserStatus, str]] = ..., used_traffic_bytes: _Optional[float] = ..., lifetime_used_traffic_bytes: _Optional[float] = ..., traffic_limit_bytes: _Optional[int] = ..., traffic_limit_strategy: _Optional[_Union[TrafficLimitStrategy, str]] = ..., sub_last_user_agent: _Optional[str] = ..., sub_last_opened_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., expire_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., online_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., sub_revoked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_traffic_reset_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., trojan_password: _Optional[str] = ..., vless_uuid: _Optional[str] = ..., ss_password: _Optional[str] = ..., description: _Optional[str] = ..., telegram_id: _Optional[int] = ..., email: _Optional[str] = ..., hwid_device_limit: _Optional[int] = ..., subscription_url: _Optional[str] = ..., first_connected: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_trigger_threshold: _Optional[int] = ..., happ: _Optional[_Union[HappCrypto, _Mapping]] = ..., active_internal_squads: _Optional[_Iterable[_Union[ActiveInternalSquad, _Mapping]]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class GetUserByUuidRequest(_message.Message):
    __slots__ = ("uuid",)
    UUID_FIELD_NUMBER: _ClassVar[int]
    uuid: str
    def __init__(self, uuid: _Optional[str] = ...) -> None: ...

class GetUserByUsernameRequest(_message.Message):
    __slots__ = ("username",)
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    username: str
    def __init__(self, username: _Optional[str] = ...) -> None: ...

class AddUserRequest(_message.Message):
    __slots__ = ("username", "email", "telegram_id", "expire_at", "created_at", "last_traffic_reset_at", "active_internal_squads", "status", "traffic_limit_strategy", "description", "tag", "hwid_device_limit")
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    TELEGRAM_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRE_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_TRAFFIC_RESET_AT_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_INTERNAL_SQUADS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TRAFFIC_LIMIT_STRATEGY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    HWID_DEVICE_LIMIT_FIELD_NUMBER: _ClassVar[int]
    username: str
    email: str
    telegram_id: int
    expire_at: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    last_traffic_reset_at: _timestamp_pb2.Timestamp
    active_internal_squads: _containers.RepeatedScalarFieldContainer[str]
    status: UserStatus
    traffic_limit_strategy: TrafficLimitStrategy
    description: str
    tag: str
    hwid_device_limit: int
    def __init__(self, username: _Optional[str] = ..., email: _Optional[str] = ..., telegram_id: _Optional[int] = ..., expire_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_traffic_reset_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., active_internal_squads: _Optional[_Iterable[str]] = ..., status: _Optional[_Union[UserStatus, str]] = ..., traffic_limit_strategy: _Optional[_Union[TrafficLimitStrategy, str]] = ..., description: _Optional[str] = ..., tag: _Optional[str] = ..., hwid_device_limit: _Optional[int] = ...) -> None: ...

class UpdateUserRequest(_message.Message):
    __slots__ = ("uuid", "status", "traffic_limit_bytes", "traffic_limit_strategy", "expire_at", "last_traffic_reset_at", "description", "tag", "telegram_id", "email", "hwid_device_limit", "active_internal_squads")
    UUID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TRAFFIC_LIMIT_BYTES_FIELD_NUMBER: _ClassVar[int]
    TRAFFIC_LIMIT_STRATEGY_FIELD_NUMBER: _ClassVar[int]
    EXPIRE_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_TRAFFIC_RESET_AT_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    TELEGRAM_ID_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    HWID_DEVICE_LIMIT_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_INTERNAL_SQUADS_FIELD_NUMBER: _ClassVar[int]
    uuid: str
    status: UserStatus
    traffic_limit_bytes: int
    traffic_limit_strategy: TrafficLimitStrategy
    expire_at: _timestamp_pb2.Timestamp
    last_traffic_reset_at: _timestamp_pb2.Timestamp
    description: str
    tag: str
    telegram_id: int
    email: str
    hwid_device_limit: int
    active_internal_squads: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, uuid: _Optional[str] = ..., status: _Optional[_Union[UserStatus, str]] = ..., traffic_limit_bytes: _Optional[int] = ..., traffic_limit_strategy: _Optional[_Union[TrafficLimitStrategy, str]] = ..., expire_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_traffic_reset_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., description: _Optional[str] = ..., tag: _Optional[str] = ..., telegram_id: _Optional[int] = ..., email: _Optional[str] = ..., hwid_device_limit: _Optional[int] = ..., active_internal_squads: _Optional[_Iterable[str]] = ...) -> None: ...

class GetAllUsersRequest(_message.Message):
    __slots__ = ("offset", "count")
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    offset: int
    count: int
    def __init__(self, offset: _Optional[int] = ..., count: _Optional[int] = ...) -> None: ...

class GetAllUsersReply(_message.Message):
    __slots__ = ("users", "total")
    USERS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    users: _containers.RepeatedCompositeFieldContainer[UserResponse]
    total: float
    def __init__(self, users: _Optional[_Iterable[_Union[UserResponse, _Mapping]]] = ..., total: _Optional[float] = ...) -> None: ...

class DeleteUserRequest(_message.Message):
    __slots__ = ("uuid",)
    UUID_FIELD_NUMBER: _ClassVar[int]
    uuid: str
    def __init__(self, uuid: _Optional[str] = ...) -> None: ...

class DeleteUserResponse(_message.Message):
    __slots__ = ("is_deleted",)
    IS_DELETED_FIELD_NUMBER: _ClassVar[int]
    is_deleted: bool
    def __init__(self, is_deleted: _Optional[bool] = ...) -> None: ...

class Inbound(_message.Message):
    __slots__ = ("uuid", "tag", "type", "port", "network", "security")
    UUID_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    NETWORK_FIELD_NUMBER: _ClassVar[int]
    SECURITY_FIELD_NUMBER: _ClassVar[int]
    uuid: str
    tag: str
    type: str
    port: float
    network: str
    security: str
    def __init__(self, uuid: _Optional[str] = ..., tag: _Optional[str] = ..., type: _Optional[str] = ..., port: _Optional[float] = ..., network: _Optional[str] = ..., security: _Optional[str] = ...) -> None: ...

class GetInboundsResponse(_message.Message):
    __slots__ = ("inbounds",)
    INBOUNDS_FIELD_NUMBER: _ClassVar[int]
    inbounds: _containers.RepeatedCompositeFieldContainer[Inbound]
    def __init__(self, inbounds: _Optional[_Iterable[_Union[Inbound, _Mapping]]] = ...) -> None: ...

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
