from typing import Optional
import asyncio

_LOCK = asyncio.Lock()


class ReferenceCount:
    """
    >>> ReferenceCount()
    ReferenceCount(0)
    """

    def __init__(self, count: int = 0):
        """
        >>> ReferenceCount(5)
        ReferenceCount(5)
        """
        self._count: int = count

    @property
    def count(self) -> int:
        """
        >>> rc = ReferenceCount(5)
        >>> rc.count
        5
        """
        return self._count

    def acquire(self) -> bool:
        """
        >>> rc = ReferenceCount(5)
        >>> rc.acquire()
        True
        >>> rc
        ReferenceCount(6)
        >>> rc.acquire()
        True
        >>> rc
        ReferenceCount(7)
        """

        self._count += 1
        return True

    def release(self) -> bool:
        """
        >>> rc = ReferenceCount(2)
        >>> rc.release()
        False
        >>> rc
        ReferenceCount(1)
        >>> rc.release()
        True
        >>> rc
        ReferenceCount(0)
        >>> rc.release()
        False
        >>> rc
        ReferenceCount(0)
        """

        final_release = self._count == 1
        if self._count > 0:
            self._count -= 1
        return final_release

    def __repr__(self) -> str:
        """
        >>> repr(ReferenceCount(5))
        'ReferenceCount(5)'
        """
        return f"{self.__class__.__name__}({self.count})"

    def __str__(self) -> str:
        """
        >>> str(ReferenceCount(5))
        '5'
        """
        return str(self.count)


class ResourceTracker:
    """
    >>> ResourceTracker()
    ResourceTracker() <resources=0 rc=0>
    """

    def __init__(self):
        self._resources = {}
        self._rc = ReferenceCount()
        self._lock = None

    @property
    def num_resources(self):
        return len(self._resources)

    @property
    def ref_count(self):
        return self._rc.count

    async def acquire(self, class_type=None, *args):
        async with await self._get_lock():
            self._rc.acquire()
            if not class_type:
                return

            key = (class_type, args)
            instance = self._resources.get(key)
            if not instance:
                instance = self._resources[key] = class_type(*args)
            return instance

    async def release(self):
        async with await self._get_lock():
            releasing = self._rc.release()

        if releasing:
            await self._release()

    async def _release(self):
        resources = self._resources
        self._resources = {}

        for instance in resources.values():
            close = getattr(instance, "close")
            if not callable(close):
                continue

            try:
                await close()
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                pass

    async def _get_lock(self):
        async with _LOCK:
            self._lock = self._lock or asyncio.Lock()
            return self._lock

    def __repr__(self) -> str:
        """
        >>> repr(ResourceTracker())
        'ResourceTracker() <resources=0 rc=0>'
        """
        return f"{self.__class__.__name__}() <resources={self.num_resources} rc={self.ref_count}>"

    def __str__(self) -> str:
        """
        >>> str(ResourceTracker())
        '<ResourceTracker>'
        """
        return f"<{self.__class__.__name__}>"


class SessionResource:
    """
    >>> SessionResource(None)
    SessionResource(None)
    """

    def __init__(self, class_type, tracker: Optional[ResourceTracker] = None, *args):
        self._class = class_type
        self._tracker = tracker or ResourceTracker()
        self._args = args

    async def __aenter__(self):
        return await self._tracker.acquire(self._class, *self._args)

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._tracker.release()

    def __repr__(self) -> str:
        """
        >>> repr(SessionResource(None))
        'SessionResource(None)'
        """
        return "%s(%r)" % (self.__class__.__name__, self._class)

    def __str__(self) -> str:
        """
        >>> str(SessionResource(None))
        '<SessionResource>'
        """
        return f"<{self.__class__.__name__}>"


class Session:
    """
    >>> class MyResource:
    ...     def __init__(self):
    ...         print('init')
    ...     async def foo(self, n):
    ...         print('foo(%d)' % n)
    ...     async def close(self):
    ...         print('close')
    >>> async def example():
    ...     session = Session()
    ...     async with session.resource(MyResource) as res1:
    ...         await res1.foo(1)
    ...     async with session.resource(MyResource) as res2:
    ...         await res2.foo(2)
    ...     print('last')
    >>> asyncio.run(example())
    init
    foo(1)
    close
    init
    foo(2)
    close
    last
    """

    def __init__(self, *, session: Optional["Session"] = None):
        self._tracker: Optional[ResourceTracker] = session._tracker if session else None

    def resource(self, class_type, *args) -> SessionResource:
        return SessionResource(class_type, self._tracker, *args)

    async def __aenter__(self):
        """
        >>> asyncio.run(Session().__aenter__())
        Session()
        """
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        >>> session = asyncio.run(Session().__aenter__())
        >>> asyncio.run(session.__aexit__(None, None, None))
        >>> session
        Session()
        """
        pass

    def __repr__(self) -> str:
        """
        >>> repr(Session())
        'Session()'
        """
        t = self._tracker
        if t:
            tracker_msg = f" <resources={t.num_resources} rc={t.ref_count}>"
        else:
            tracker_msg = ""
        return f"{self.__class__.__name__}(){tracker_msg}"

    def __str__(self) -> str:
        """
        >>> str(Session())
        '<Session>'
        """
        return f"<{self.__class__.__name__}>"


class PersistentSession(Session):
    """
    >>> class MyResource:
    ...     def __init__(self):
    ...         print('init')
    ...     async def foo(self, n):
    ...         print('foo(%d)' % n)
    ...     async def close(self):
    ...         print('close')
    >>> async def example():
    ...     async with PersistentSession() as session:
    ...         async with session.resource(MyResource) as res1:
    ...             await res1.foo(1)
    ...         async with session.resource(MyResource) as res2:
    ...             await res2.foo(2)
    ...         print('last')
    >>> asyncio.run(example())
    init
    foo(1)
    foo(2)
    last
    close
    """

    def __init__(self):
        """
        >>> PersistentSession()
        PersistentSession()
        """
        self._tracker = None

    async def __aenter__(self):
        """
        >>> asyncio.run(PersistentSession().__aenter__())
        Session() <resources=0 rc=1>
        """
        self._tracker = self._tracker or ResourceTracker()
        await self._tracker.acquire()
        return Session(session=self)

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        >>> psession = PersistentSession()
        >>> session = asyncio.run(psession.__aenter__())
        >>> asyncio.run(psession.__aexit__(None, None, None))
        >>> session
        Session() <resources=0 rc=0>
        >>> psession
        PersistentSession() <resources=0 rc=0>
        """
        await self._tracker.release()
